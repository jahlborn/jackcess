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
  public static final String US_DATE_FORMAT = "M/d/yyyy";
  public static final String US_TIME_FORMAT_12 = "h:mm:ss a";
  public static final String US_TIME_FORMAT_24 = "H:mm:ss";

  /** default implementation which is configured for the US locale */
  public static final TemporalConfig US_TEMPORAL_CONFIG = new TemporalConfig(
      US_DATE_FORMAT, US_TIME_FORMAT_12, US_TIME_FORMAT_24, '/', ':', Locale.US);

  public enum Type {
    DATE, TIME, DATE_TIME, TIME_12, TIME_24, DATE_TIME_12, DATE_TIME_24;

    public Type getDefaultType() {
      switch(this) {
      case DATE:
        return DATE;
      case TIME:
      case TIME_12:
      case TIME_24:
        return TIME;
      case DATE_TIME:
      case DATE_TIME_12:
      case DATE_TIME_24:
        return DATE_TIME;
      default:
        throw new RuntimeException("invalid type " + this);
      }
    }

    public Value.Type getValueType() {
      switch(this) {
      case DATE:
        return Value.Type.DATE;
      case TIME:
      case TIME_12:
      case TIME_24:
        return Value.Type.TIME;
      case DATE_TIME:
      case DATE_TIME_12:
      case DATE_TIME_24:
        return Value.Type.DATE_TIME;
      default:
        throw new RuntimeException("invalid type " + this);
      }
    }
  }

  private final String _dateFormat;
  private final String _timeFormat12;
  private final String _timeFormat24;
  private final char _dateSeparator;
  private final char _timeSeparator;
  private final String _dateTimeFormat12;
  private final String _dateTimeFormat24;
  private final DateFormatSymbols _symbols;

  /**
   * Instantiates a new TemporalConfig with the given configuration.  Note
   * that the date/time format variants will be created by concatenating the
   * relevant date and time formats, separated by a single space, e.g. "<date>
   * <time>".
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
  public TemporalConfig(String dateFormat, String timeFormat12,
                        String timeFormat24, char dateSeparator,
                        char timeSeparator, Locale locale)
  {
    _dateFormat = dateFormat;
    _timeFormat12 = timeFormat12;
    _timeFormat24 = timeFormat24;
    _dateSeparator = dateSeparator;
    _timeSeparator = timeSeparator;
    _dateTimeFormat12 = _dateFormat + " " + _timeFormat12;
    _dateTimeFormat24 = _dateFormat + " " + _timeFormat24;
    _symbols = DateFormatSymbols.getInstance(locale);
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
      return getDefaultDateFormat();
    case TIME:
      return getDefaultTimeFormat();
    case DATE_TIME:
      return getDefaultDateTimeFormat();
    case TIME_12:
      return getTimeFormat12();
    case TIME_24:
      return getTimeFormat24();
    case DATE_TIME_12:
      return getDateTimeFormat12();
    case DATE_TIME_24:
      return getDateTimeFormat24();
    default:
      throw new IllegalArgumentException("unknown date/time type " + type);
    }
  }

  public DateFormatSymbols getDateFormatSymbols() {
    return _symbols;
  }
}
