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

/**
 *
 * @author James Ahlborn
 */
public class TemporalConfig 
{
  public static final String US_DATE_FORMAT = "M/d/yyyy";
  public static final String US_TIME_FORMAT_12 = "hh:mm:ss a";
  public static final String US_TIME_FORMAT_24 = "HH:mm:ss";

  public static final TemporalConfig US_TEMPORAL_CONFIG = new TemporalConfig(
      US_DATE_FORMAT, US_TIME_FORMAT_12, US_TIME_FORMAT_24, '/', ':');

  private final String _dateFormat;
  private final String _timeFormat12;
  private final String _timeFormat24;
  private final char _dateSeparator;
  private final char _timeSeparator;
  private final String _dateTimeFormat12;
  private final String _dateTimeFormat24;

  public TemporalConfig(String dateFormat, String timeFormat12, 
                        String timeFormat24, char dateSeparator, 
                        char timeSeparator)
  {
    _dateFormat = dateFormat;
    _timeFormat12 = timeFormat12;
    _timeFormat24 = timeFormat24;
    _dateSeparator = dateSeparator;
    _timeSeparator = timeSeparator;
    _dateTimeFormat12 = _dateFormat + " " + _timeFormat12;
    _dateTimeFormat24 = _dateFormat + " " + _timeFormat24;
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
}
