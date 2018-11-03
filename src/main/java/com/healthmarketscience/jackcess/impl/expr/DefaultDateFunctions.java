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

package com.healthmarketscience.jackcess.impl.expr;


import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.util.Calendar;
import java.util.Date;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import static com.healthmarketscience.jackcess.impl.expr.DefaultFunctions.*;
import static com.healthmarketscience.jackcess.impl.expr.FunctionSupport.*;

/**
 *
 * @author James Ahlborn
 */
public class DefaultDateFunctions
{
  // min, valid, recognizable date: January 1, 100 A.D. 00:00:00
  private static final double MIN_DATE = -657434.0d;
  // max, valid, recognizable date: December 31, 9999 A.D. 23:59:59
  private static final double MAX_DATE = 2958465.999988426d;

  private static final long SECONDS_PER_DAY = 24L * 60L * 60L;
  private static final double DSECONDS_PER_DAY = SECONDS_PER_DAY;

  private static final long SECONDS_PER_HOUR = 60L * 60L;
  private static final long SECONDS_PER_MINUTE = 60L;

  private static final String INTV_YEAR = "yyyy";
  private static final String INTV_QUARTER = "q";
  private static final String INTV_MONTH = "m";
  private static final String INTV_DAY_OF_YEAR = "y";
  private static final String INTV_DAY = "d";
  private static final String INTV_WEEKDAY = "w";
  private static final String INTV_WEEK = "ww";
  private static final String INTV_HOUR = "h";
  private static final String INTV_MINUTE = "n";
  private static final String INTV_SECOND = "s";


  private DefaultDateFunctions() {}

  static void init() {
    // dummy method to ensure this class is loaded
  }

  public static final Function DATE = registerFunc(new Func0("Date") {
    @Override
    protected Value eval0(EvalContext ctx) {
      double dd = dateOnly(currentTimeDouble(ctx));
      return ValueSupport.toDateValue(ctx, Value.Type.DATE, dd);
    }
  });

  public static final Function DATEVALUE = registerFunc(new Func1NullIsNull("DateValue") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Value dv = param1.getAsDateTimeValue(ctx);
      if(dv.getType() == Value.Type.DATE) {
        return dv;
      }
      double dd = dateOnly(dv.getAsDouble(ctx));
      return ValueSupport.toDateValue(ctx, Value.Type.DATE, dd);
    }
  });

  public static final Function DATESERIAL = registerFunc(new Func3("DateSerial") {
    @Override
    protected Value eval3(EvalContext ctx, Value param1, Value param2, Value param3) {
      int year = param1.getAsLongInt(ctx);
      int month = param2.getAsLongInt(ctx);
      int day = param3.getAsLongInt(ctx);

      // "default" two digit year handling
      if(year < 100) {
        year += ((year <= 29) ? 2000 : 1900);
      }

      Calendar cal = ctx.getCalendar();
      cal.clear();

      cal.set(Calendar.YEAR, year);
      // convert to 0 based value
      cal.set(Calendar.MONTH, month - 1);
      cal.set(Calendar.DAY_OF_MONTH, day);

      return ValueSupport.toValue(Value.Type.DATE, cal.getTime());
    }
  });

  public static final Function DATEPART = registerFunc(new FuncVar("DatePart", 2, 4) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param2 = params[1];
      if(param2.isNull()) {
        return ValueSupport.NULL_VAL;
      }

      int firstDay = getFirstDayParam(ctx, params, 2);
      int firstWeekType = getFirstWeekTypeParam(ctx, params, 3);

      String intv = params[0].getAsString(ctx).trim();
      int result = -1;
      if(intv.equalsIgnoreCase(INTV_YEAR)) {
        result = nonNullToCalendarField(ctx, param2, Calendar.YEAR);
      } else if(intv.equalsIgnoreCase(INTV_QUARTER)) {
        // month in is 0 based
        int month = nonNullToCalendarField(ctx, param2, Calendar.MONTH);
        result = (month / 3) + 1;
      } else if(intv.equalsIgnoreCase(INTV_MONTH)) {
        // convert from 0 based to 1 based value
        result = nonNullToCalendarField(ctx, param2, Calendar.MONTH) + 1;
      } else if(intv.equalsIgnoreCase(INTV_DAY_OF_YEAR)) {
        result = nonNullToCalendarField(ctx, param2, Calendar.DAY_OF_YEAR);
      } else if(intv.equalsIgnoreCase(INTV_DAY)) {
        result = nonNullToCalendarField(ctx, param2, Calendar.DAY_OF_MONTH);
      } else if(intv.equalsIgnoreCase(INTV_WEEKDAY)) {
        int dayOfWeek = nonNullToCalendarField(ctx, param2, Calendar.DAY_OF_WEEK);
        result = dayOfWeekToWeekDay(dayOfWeek, firstDay);
      } else if(intv.equalsIgnoreCase(INTV_WEEK)) {
        result = weekOfYear(ctx, param2, firstDay, firstWeekType);
      } else if(intv.equalsIgnoreCase(INTV_HOUR)) {
        result = nonNullToCalendarField(ctx, param2, Calendar.HOUR_OF_DAY);
      } else if(intv.equalsIgnoreCase(INTV_MINUTE)) {
        result = nonNullToCalendarField(ctx, param2, Calendar.MINUTE);
      } else if(intv.equalsIgnoreCase(INTV_SECOND)) {
        result = nonNullToCalendarField(ctx, param2, Calendar.SECOND);
      } else {
        throw new EvalException("Invalid interval " + intv);
      }

      return ValueSupport.toValue(result);
    }
  });

  public static final Function DATEADD = registerFunc(new Func3("DateAdd") {
    @Override
    protected Value eval3(EvalContext ctx,
                          Value param1, Value param2, Value param3) {
      if(param3.isNull()) {
        return ValueSupport.NULL_VAL;
      }

      String intv = param1.getAsString(ctx).trim();
      int val = param2.getAsLongInt(ctx);

      Calendar cal = nonNullToCalendar(ctx, param3);

      if(intv.equalsIgnoreCase(INTV_YEAR)) {
        cal.add(Calendar.YEAR, val);
      } else if(intv.equalsIgnoreCase(INTV_QUARTER)) {
        cal.add(Calendar.MONTH, val * 3);
      } else if(intv.equalsIgnoreCase(INTV_MONTH)) {
        cal.add(Calendar.MONTH, val);
      } else if(intv.equalsIgnoreCase(INTV_DAY_OF_YEAR)) {
        cal.add(Calendar.DAY_OF_YEAR, val);
      } else if(intv.equalsIgnoreCase(INTV_DAY)) {
        cal.add(Calendar.DAY_OF_YEAR, val);
      } else if(intv.equalsIgnoreCase(INTV_WEEKDAY)) {
        cal.add(Calendar.DAY_OF_WEEK, val);
      } else if(intv.equalsIgnoreCase(INTV_WEEK)) {
        cal.add(Calendar.WEEK_OF_YEAR, val);
      } else if(intv.equalsIgnoreCase(INTV_HOUR)) {
        cal.add(Calendar.HOUR, val);
      } else if(intv.equalsIgnoreCase(INTV_MINUTE)) {
        cal.add(Calendar.MINUTE, val);
      } else if(intv.equalsIgnoreCase(INTV_SECOND)) {
        cal.add(Calendar.SECOND, val);
      } else {
        throw new EvalException("Invalid interval " + intv);
      }

      return ValueSupport.toValue(cal);
    }
  });

  public static final Function NOW = registerFunc(new Func0("Now") {
    @Override
    protected Value eval0(EvalContext ctx) {
      return ValueSupport.toValue(Value.Type.DATE_TIME, new Date());
    }
  });

  public static final Function TIME = registerFunc(new Func0("Time") {
    @Override
    protected Value eval0(EvalContext ctx) {
      double dd = timeOnly(currentTimeDouble(ctx));
      return ValueSupport.toDateValue(ctx, Value.Type.TIME, dd);
    }
  });

  public static final Function TIMEVALUE = registerFunc(new Func1NullIsNull("TimeValue") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Value dv = param1.getAsDateTimeValue(ctx);
      if(dv.getType() == Value.Type.TIME) {
        return dv;
      }
      double dd = timeOnly(dv.getAsDouble(ctx));
      return ValueSupport.toDateValue(ctx, Value.Type.TIME, dd);
    }
  });

  public static final Function TIMER = registerFunc(new Func0("Timer") {
    @Override
    protected Value eval0(EvalContext ctx) {
      double dd = timeOnly(currentTimeDouble(ctx)) * DSECONDS_PER_DAY;
      return ValueSupport.toValue(dd);
    }
  });

  public static final Function TIMESERIAL = registerFunc(new Func3("TimeSerial") {
    @Override
    protected Value eval3(EvalContext ctx, Value param1, Value param2, Value param3) {
      int hours = param1.getAsLongInt(ctx);
      int minutes = param2.getAsLongInt(ctx);
      int seconds = param3.getAsLongInt(ctx);

      long totalSeconds = (hours * SECONDS_PER_HOUR) +
        (minutes * SECONDS_PER_MINUTE) + seconds;
      if(totalSeconds < 0L) {
        do {
          totalSeconds += SECONDS_PER_DAY;
        } while(totalSeconds < 0L);
      } else if(totalSeconds > SECONDS_PER_DAY) {
        totalSeconds %= SECONDS_PER_DAY;
      }

      double dd = totalSeconds / DSECONDS_PER_DAY;
      return ValueSupport.toDateValue(ctx, Value.Type.TIME, dd);
    }
  });

  public static final Function HOUR = registerFunc(new Func1NullIsNull("Hour") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.HOUR_OF_DAY));
    }
  });

  public static final Function MINUTE = registerFunc(new Func1NullIsNull("Minute") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.MINUTE));
    }
  });

  public static final Function SECOND = registerFunc(new Func1NullIsNull("Second") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.SECOND));
    }
  });

  public static final Function YEAR = registerFunc(new Func1NullIsNull("Year") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.YEAR));
    }
  });

  public static final Function MONTH = registerFunc(new Func1NullIsNull("Month") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      // convert from 0 based to 1 based value
      return ValueSupport.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.MONTH) + 1);
    }
  });

  public static final Function MONTHNAME = registerFunc(new FuncVar("MonthName", 1, 2) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(param1.isNull()) {
        return ValueSupport.NULL_VAL;
      }
      // convert from 1 based to 0 based value
      int month = param1.getAsLongInt(ctx) - 1;

      boolean abbreviate = getOptionalBooleanParam(ctx, params, 1);

      DateFormatSymbols syms = ctx.getTemporalConfig().getDateFormatSymbols();
      String[] monthNames = (abbreviate ?
                             syms.getShortMonths() : syms.getMonths());
      // note, the array is 1 based
      return ValueSupport.toValue(monthNames[month]);
    }
  });

  public static final Function DAY = registerFunc(new Func1NullIsNull("Day") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.DAY_OF_MONTH));
    }
  });

  public static final Function WEEKDAY = registerFunc(new FuncVar("Weekday", 1, 2) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(param1.isNull()) {
        return ValueSupport.NULL_VAL;
      }
      int dayOfWeek = nonNullToCalendarField(ctx, param1, Calendar.DAY_OF_WEEK);

      int firstDay = getFirstDayParam(ctx, params, 1);

      return ValueSupport.toValue(dayOfWeekToWeekDay(dayOfWeek, firstDay));
    }
  });

  public static final Function WEEKDAYNAME = registerFunc(new FuncVar("WeekdayName", 1, 3) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(param1.isNull()) {
        return ValueSupport.NULL_VAL;
      }
      int weekday = param1.getAsLongInt(ctx);

      boolean abbreviate = getOptionalBooleanParam(ctx, params, 1);

      int firstDay = getFirstDayParam(ctx, params, 2);

      int dayOfWeek = weekDayToDayOfWeek(weekday, firstDay);

      DateFormatSymbols syms = ctx.getTemporalConfig().getDateFormatSymbols();
      String[] weekdayNames = (abbreviate ?
                               syms.getShortWeekdays() : syms.getWeekdays());
      // note, the array is 1 based
      return ValueSupport.toValue(weekdayNames[dayOfWeek]);
    }
  });

  private static int nonNullToCalendarField(EvalContext ctx, Value param,
                                            int field) {
    return nonNullToCalendar(ctx, param).get(field);
  }

  private static Calendar nonNullToCalendar(EvalContext ctx, Value param) {
    Calendar cal = ctx.getCalendar();
    cal.setTime(param.getAsDateTime(ctx));
    return cal;
  }

  static Value stringToDateValue(LocaleContext ctx, String valStr) {
    // see if we can coerce to date/time
    TemporalConfig.Type valTempType = ExpressionTokenizer.determineDateType(
        valStr, ctx);

    if(valTempType != null) {

      DateFormat parseDf = ExpressionTokenizer.createParseDateTimeFormat(
          valTempType, ctx);

      try {
        Date dateVal = ExpressionTokenizer.parseComplete(parseDf, valStr);
        return ValueSupport.toValue(valTempType.getValueType(), dateVal);
      } catch(java.text.ParseException pe) {

        if(valTempType.includesDate()) {
          // the date may not include a year value, in which case it means
          // to use the "current" year.  see if this is an implicit year date
          parseDf = ExpressionTokenizer.createParseImplicitYearDateTimeFormat(
              valTempType, ctx);
          try {
            Date dateVal = ExpressionTokenizer.parseComplete(parseDf, valStr);
            return ValueSupport.toValue(valTempType.getValueType(), dateVal);
          } catch(java.text.ParseException pe2) {
            // guess not, continue on to failure
          }
        }
      }
    }

    // not a valid date string, not a date/time
    return null;
  }

  static Value numberToDateValue(LocaleContext ctx, double dd) {
    if((dd < MIN_DATE) || (dd > MAX_DATE)) {
      // outside valid date range
      return null;
    }

    boolean hasDate = (dateOnly(dd) != 0.0d);
    boolean hasTime = (timeOnly(dd) != 0.0d);

    Value.Type type = (hasDate ? (hasTime ? Value.Type.DATE_TIME :
                                  Value.Type.DATE) :
                       Value.Type.TIME);
    return ValueSupport.toDateValue(ctx, type, dd);
  }

  private static double dateOnly(double dd) {
    // the integral part of the date/time double is the date value.  discard
    // the fractional portion
    return (long)dd;
  }

  private static double timeOnly(double dd) {
    // the fractional part of the date/time double is the time value.  discard
    // the integral portion
    return new BigDecimal(dd).remainder(BigDecimal.ONE).doubleValue();
  }

  private static double currentTimeDouble(LocaleContext ctx) {
    return ColumnImpl.toDateDouble(System.currentTimeMillis(), ctx.getCalendar());
  }

  private static int dayOfWeekToWeekDay(int day, int firstDay) {
    // shift all the values to 0 based to calculate the correct value, then
    // back to 1 based to return the result
    return (((day - 1) - (firstDay - 1) + 7) % 7) + 1;
  }

  private static int weekDayToDayOfWeek(int weekday, int firstDay) {
    // shift all the values to 0 based to calculate the correct value, then
    // back to 1 based to return the result
    return (((firstDay - 1) + (weekday - 1)) % 7) + 1;
  }

  private static int getFirstDayParam(
      LocaleContext ctx, Value[] params, int idx) {
    // vbSunday (default) 1
    // vbUseSystem 0
    return getOptionalIntParam(ctx, params, idx, 1, 0);
  }

  private static int getFirstWeekTypeParam(
      LocaleContext ctx, Value[] params, int idx) {
    // vbFirstJan1 (default) 1
    // vbUseSystem 0
    return getOptionalIntParam(ctx, params, idx, 1, 0);
  }

  private static int weekOfYear(EvalContext ctx, Value param, int firstDay,
                                int firstWeekType) {
    Calendar cal = nonNullToCalendar(ctx, param);

    // need to mess with some calendar settings, but they need to be restored
    // when done because the Calendar instance may be shared
    int origFirstDay = cal.getFirstDayOfWeek();
    int origMinDays = cal.getMinimalDaysInFirstWeek();
    try {

      int minDays = 1;
      switch(firstWeekType) {
      case 1:
        // vbUseSystem 0
        // vbFirstJan1 1 (default)
        break;
      case 2:
        // vbFirstFourDays 2
        minDays = 4;
        break;
      case 3:
        // vbFirstFullWeek 3
        minDays = 7;
        break;
      default:
        throw new EvalException("Invalid first week of year type " +
                                firstWeekType);
      }

      cal.setFirstDayOfWeek(firstDay);
      cal.setMinimalDaysInFirstWeek(minDays);

      return cal.get(Calendar.WEEK_OF_YEAR);

    } finally {
      cal.setFirstDayOfWeek(origFirstDay);
      cal.setMinimalDaysInFirstWeek(origMinDays);
    }
  }
}
