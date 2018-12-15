/*
Copyright (c) 2017 James Ahlborn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.healthmarketscience.jackcess.expr;

import java.text.DateFormatSymbols;
import java.util.Locale;

/**
 * A TemporalConfig encapsulates date/time formatting options for expression
 * evaluation.  The default {@link #US_TEMPORAL_CONFIG} instance provides US
 * specific locale configuration.  Databases which have been built for other
 * locales can utilize custom implementations of TemporalConfig in order to
 * evaluate expressions correctly.
 *
 * @author James Ahlborn
 */
public class TemporalConfig
{
  public static final String US_DATE_FORMAT = "M/d[/uuuu]";
  public static final String US_TIME_FORMAT_12_FORMAT = "h:mm:ss a";
  public static final String US_TIME_FORMAT_24_FORMAT = "H:mm:ss";
  public static final String US_LONG_DATE_FORMAT = "EEEE, MMMM dd, uuuu";

  public static final String MEDIUM_DATE_FORMAT = "dd-MMM-uu";
  public static final String MEDIUM_TIME_FORMAT = "hh:mm a";
  public static final String SHORT_TIME_FORMAT = "HH:mm";

  /** default implementation which is configured for the US locale */
  public static final TemporalConfig US_TEMPORAL_CONFIG = new TemporalConfig(
      US_DATE_FORMAT, US_LONG_DATE_FORMAT,
      US_TIME_FORMAT_12_FORMAT, US_TIME_FORMAT_24_FORMAT, '/', ':', Locale.US);

  public enum Type {
    DATE, TIME, DATE_TIME, TIME_12, TIME_24, DATE_TIME_12, DATE_TIME_24,
    GENERAL_DATE, LONG_DATE, MEDIUM_DATE, SHORT_DATE,
    LONG_TIME, MEDIUM_TIME, SHORT_TIME;

    public Type getDefaultType() {
      switch(this) {
      case DATE:
      case LONG_DATE:
      case MEDIUM_DATE:
      case SHORT_DATE:
        return DATE;
      case TIME:
      case TIME_12:
      case TIME_24:
      case LONG_TIME:
      case MEDIUM_TIME:
      case SHORT_TIME:
        return TIME;
      case DATE_TIME:
      case DATE_TIME_12:
      case DATE_TIME_24:
      case GENERAL_DATE:
        return DATE_TIME;
      default:
        throw new RuntimeException("invalid type " + this);
      }
    }

    public Value.Type getValueType() {
      switch(this) {
      case DATE:
      case LONG_DATE:
      case MEDIUM_DATE:
      case SHORT_DATE:
        return Value.Type.DATE;
      case TIME:
      case TIME_12:
      case TIME_24:
      case LONG_TIME:
      case MEDIUM_TIME:
      case SHORT_TIME:
        return Value.Type.TIME;
      case DATE_TIME:
      case DATE_TIME_12:
      case DATE_TIME_24:
      case GENERAL_DATE:
        return Value.Type.DATE_TIME;
      default:
        throw new RuntimeException("invalid type " + this);
      }
    }

    public boolean includesTime() {
      return !isDateOnly();
    }

    public boolean includesDate() {
      return !isTimeOnly();
    }

    public boolean isDateOnly() {
      switch(this) {
      case DATE:
      case LONG_DATE:
      case MEDIUM_DATE:
      case SHORT_DATE:
        return true;
      default:
        return false;
      }
    }

    public boolean isTimeOnly() {
      switch(this) {
      case TIME:
      case TIME_12:
      case TIME_24:
      case LONG_TIME:
      case MEDIUM_TIME:
      case SHORT_TIME:
        return true;
      default:
        return false;
      }
    }
  }

  private final Locale _locale;
  private final String _dateFormat;
  private final String _longDateFormat;
  private final String _timeFormat12;
  private final String _timeFormat24;
  private final char _dateSeparator;
  private final char _timeSeparator;
  private final String _dateTimeFormat12;
  private final String _dateTimeFormat24;
  private final String[] _amPmStrings;

  /**
   * Instantiates a new TemporalConfig with the given configuration.  Note
   * that the date/time format variants will be created by concatenating the
   * relevant date and time formats, separated by a single space, e.g. "&lt;date&gt;
   * &lt;time&gt;".
   *
   * @param dateFormat the date (no time) format
   * @param timeFormat12 the 12 hour time format
   * @param timeFormat24 the 24 hour time format
   * @param dateSeparator the primary separator used to separate elements in
   *                      the date format.  this is used to identify the
   *                      components of date/time string.
   * @param timeSeparator the primary separator used to separate elements in
   *                      the time format (both 12 hour and 24 hour).  this is
   *                      used to identify the components of a date/time
   *                      string.  This value should differ from the
   *                      dateSeparator.
   */
  public TemporalConfig(String dateFormat, String longDateFormat,
                        String timeFormat12, String timeFormat24,
                        char dateSeparator, char timeSeparator, Locale locale)
  {
    _locale = locale;
    _dateFormat = dateFormat;
    _longDateFormat = longDateFormat;
    _timeFormat12 = timeFormat12;
    _timeFormat24 = timeFormat24;
    _dateSeparator = dateSeparator;
    _timeSeparator = timeSeparator;
    _dateTimeFormat12 = toDateTimeFormat(_dateFormat, _timeFormat12);
    _dateTimeFormat24 = toDateTimeFormat(_dateFormat, _timeFormat24);
    // there doesn't seem to be a good/easy way to get this in new jave.time
    // api, so just use old api
    _amPmStrings = DateFormatSymbols.getInstance(locale).getAmPmStrings();
  }

  public Locale getLocale() {
    return _locale;
  }

  public String getDateFormat() {
    return _dateFormat;
  }

  public String getTimeFormat12() {
    return _timeFormat12;
  }

  public String getTimeFormat24() {
    return _timeFormat24;
  }

  public String getDateTimeFormat12() {
    return _dateTimeFormat12;
  }

  public String getDateTimeFormat24() {
    return _dateTimeFormat24;
  }

  public String getDefaultDateFormat() {
    return getDateFormat();
  }

  public String getDefaultTimeFormat() {
    return getTimeFormat12();
  }

  public String getDefaultDateTimeFormat() {
    return getDateTimeFormat12();
  }

  public char getDateSeparator() {
    return _dateSeparator;
  }

  public char getTimeSeparator() {
    return _timeSeparator;
  }

  public String getDateTimeFormat(Type type) {
    switch(type) {
    case DATE:
    case SHORT_DATE:
      return getDefaultDateFormat();
    case TIME:
      return getDefaultTimeFormat();
    case DATE_TIME:
    case GENERAL_DATE:
      return getDefaultDateTimeFormat();
    case TIME_12:
    case LONG_TIME:
      return getTimeFormat12();
    case TIME_24:
      return getTimeFormat24();
    case DATE_TIME_12:
      return getDateTimeFormat12();
    case DATE_TIME_24:
      return getDateTimeFormat24();
    case LONG_DATE:
      return getLongDateFormat();
    case MEDIUM_DATE:
      return getMediumDateFormat();
    case MEDIUM_TIME:
      return getMediumTimeFormat();
    case SHORT_TIME:
      return getShortTimeFormat();
    default:
      throw new IllegalArgumentException("unknown date/time type " + type);
    }
  }

  public String[] getAmPmStrings() {
    return _amPmStrings;
  }

  private static String toDateTimeFormat(String dateFormat, String timeFormat) {
    return dateFormat + " " + timeFormat;
  }

  protected String getLongDateFormat() {
    return _longDateFormat;
  }

  protected String getMediumDateFormat() {
    return MEDIUM_DATE_FORMAT;
  }

  protected String getMediumTimeFormat() {
    return MEDIUM_TIME_FORMAT;
  }

  protected String getShortTimeFormat() {
    return SHORT_TIME_FORMAT;
  }
}
