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

  private DefaultDateFunctions() {}

  static void init() {
    // dummy method to ensure this class is loaded
  }

  public static final Function DATE = registerFunc(new Func0("Date") {
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat fmt = ValueSupport.getDateFormatForType(ctx, Value.Type.DATE);
      double dd = dateOnly(currentTimeDouble(fmt));
      return ValueSupport.toValue(Value.Type.DATE, dd, fmt);
    }
  });

  public static final Function DATEVALUE = registerFunc(new Func1NullIsNull("DateValue") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Value dv = nonNullToDateValue(ctx, param1);
      if(dv.getType() == Value.Type.DATE) {
        return dv;
      }
      double dd = dateOnly(dv.getAsDouble());
      DateFormat fmt = ValueSupport.getDateFormatForType(ctx, Value.Type.DATE);
      return ValueSupport.toValue(Value.Type.DATE, dd, fmt);
    }
  });

  public static final Function DATESERIAL = registerFunc(new Func3("DateSerial") {
    @Override
    protected Value eval3(EvalContext ctx, Value param1, Value param2, Value param3) {
      int year = param1.getAsLongInt();
      int month = param2.getAsLongInt();
      int day = param3.getAsLongInt();

      // "default" two digit year handling
      if(year < 100) {
        year += ((year <= 29) ? 2000 : 1900);
      }

      DateFormat fmt = ValueSupport.getDateFormatForType(ctx, Value.Type.DATE);
      Calendar cal = fmt.getCalendar();
      cal.clear();

      cal.set(Calendar.YEAR, year);
      // convert to 0 based value
      cal.set(Calendar.MONTH, month - 1);
      cal.set(Calendar.DAY_OF_MONTH, day);

      return ValueSupport.toValue(Value.Type.DATE, cal.getTime(), fmt);
    }
  });

  public static final Function NOW = registerFunc(new Func0("Now") {
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat fmt = ValueSupport.getDateFormatForType(ctx, Value.Type.DATE_TIME);
      return ValueSupport.toValue(Value.Type.DATE_TIME, new Date(), fmt);
    }
  });

  public static final Function TIME = registerFunc(new Func0("Time") {
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat fmt = ValueSupport.getDateFormatForType(ctx, Value.Type.TIME);
      double dd = timeOnly(currentTimeDouble(fmt));
      return ValueSupport.toValue(Value.Type.TIME, dd, fmt);
    }
  });

  public static final Function TIMEVALUE = registerFunc(new Func1NullIsNull("TimeValue") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Value dv = nonNullToDateValue(ctx, param1);
      if(dv.getType() == Value.Type.TIME) {
        return dv;
      }
      double dd = timeOnly(dv.getAsDouble());
      DateFormat fmt = ValueSupport.getDateFormatForType(ctx, Value.Type.TIME);
      return ValueSupport.toValue(Value.Type.TIME, dd, fmt);
    }
  });

  public static final Function TIMER = registerFunc(new Func0("Timer") {
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat fmt = ValueSupport.getDateFormatForType(ctx, Value.Type.TIME);
      double dd = timeOnly(currentTimeDouble(fmt)) * DSECONDS_PER_DAY;
      return ValueSupport.toValue(dd);
    }
  });

  public static final Function TIMESERIAL = registerFunc(new Func3("TimeSerial") {
    @Override
    protected Value eval3(EvalContext ctx, Value param1, Value param2, Value param3) {
      int hours = param1.getAsLongInt();
      int minutes = param2.getAsLongInt();
      int seconds = param3.getAsLongInt();

      long totalSeconds = (hours * SECONDS_PER_HOUR) +
        (minutes * SECONDS_PER_MINUTE) + seconds;
      if(totalSeconds < 0L) {
        do {
          totalSeconds += SECONDS_PER_DAY;
        } while(totalSeconds < 0L);
      } else if(totalSeconds > SECONDS_PER_DAY) {
        totalSeconds %= SECONDS_PER_DAY;
      }

      DateFormat fmt = ValueSupport.getDateFormatForType(ctx, Value.Type.TIME);
      double dd = totalSeconds / DSECONDS_PER_DAY;
      return ValueSupport.toValue(Value.Type.TIME, dd, fmt);
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
      if(param1 == null) {
        return null;
      }
      // convert from 1 based to 0 based value
      int month = param1.getAsLongInt() - 1;

      boolean abbreviate = getOptionalBooleanParam(params, 1);

      DateFormatSymbols syms = ctx.createDateFormat(
          ctx.getTemporalConfig().getDateFormat()).getDateFormatSymbols();
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
      if(param1 == null) {
        return null;
      }
      int dayOfWeek = nonNullToCalendarField(ctx, param1, Calendar.DAY_OF_WEEK);

      int firstDay = getFirstDayParam(params, 1);

      return ValueSupport.toValue(dayOfWeekToWeekDay(dayOfWeek, firstDay));
    }
  });

  public static final Function WEEKDAYNAME = registerFunc(new FuncVar("WeekdayName", 1, 3) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(param1 == null) {
        return null;
      }
      int weekday = param1.getAsLongInt();

      boolean abbreviate = getOptionalBooleanParam(params, 1);

      int firstDay = getFirstDayParam(params, 2);

      int dayOfWeek = weekDayToDayOfWeek(weekday, firstDay);

      DateFormatSymbols syms = ctx.createDateFormat(
          ctx.getTemporalConfig().getDateFormat()).getDateFormatSymbols();
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
    Value origParam = param;
    param = nonNullToDateValue(ctx, param);
    if(param == null) {
      // not a date/time
      throw new EvalException("Invalid date/time expression '" +
                              origParam + "'");
    }

    Calendar cal = getDateValueFormat(ctx, param).getCalendar();
    cal.setTime(param.getAsDateTime(ctx));
    return cal;
  }

  static Value nonNullToDateValue(EvalContext ctx, Value param) {
    Value.Type type = param.getType();
    if(type.isTemporal()) {
      return param;
    }

    if(type == Value.Type.STRING) {

      // see if we can coerce to date/time or double
      String valStr = param.getAsString();
      TemporalConfig.Type valTempType = ExpressionTokenizer.determineDateType(
          valStr, ctx);

      if(valTempType != null) {

        try {
          DateFormat parseDf = ExpressionTokenizer.createParseDateFormat(
              valTempType, ctx);
          Date dateVal = ExpressionTokenizer.parseComplete(parseDf, valStr);
          return ValueSupport.toValue(ctx, valTempType.getValueType(),
                                      dateVal);
        } catch(java.text.ParseException pe) {
          // not a valid date string, not a date/time
          return null;
        }
      }

      // see if string can be coerced to number
      try {
        return numberToDateValue(ctx, param.getAsDouble());
      } catch(NumberFormatException ignored) {
        // not a number, not a date/time
        return null;
      }
    }

    // must be a number
    return numberToDateValue(ctx, param.getAsDouble());
  }

  private static Value numberToDateValue(EvalContext ctx, double dd) {
    if((dd < MIN_DATE) || (dd > MAX_DATE)) {
      // outside valid date range
      return null;
    }

    boolean hasDate = (dateOnly(dd) != 0.0d);
    boolean hasTime = (timeOnly(dd) != 0.0d);

    Value.Type type = (hasDate ? (hasTime ? Value.Type.DATE_TIME :
                                  Value.Type.DATE) :
                       Value.Type.TIME);
    DateFormat fmt = ValueSupport.getDateFormatForType(ctx, type);
    return ValueSupport.toValue(type, dd, fmt);
  }

  private static DateFormat getDateValueFormat(EvalContext ctx, Value param) {
    return ((param instanceof BaseDateValue) ?
            ((BaseDateValue)param).getFormat() :
       ValueSupport.getDateFormatForType(ctx, param.getType()));
  }

  private static double dateOnly(double dd) {
    // the integral part of the date/time double is the date value.  discard
    // the fractional portion
    return (long)dd;
  }

  private static double timeOnly(double dd) {
    // the fractional part of the date/time double is the time value.  discard
    // the integral portion and convert to seconds
    return new BigDecimal(dd).remainder(BigDecimal.ONE).doubleValue();
  }

  private static double currentTimeDouble(DateFormat fmt) {
    return ColumnImpl.toDateDouble(System.currentTimeMillis(), fmt.getCalendar());
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

  private static int getFirstDayParam(Value[] params, int idx) {
    // vbSunday (default)
    int firstDay = 1;
    if(params.length > idx) {
      firstDay = params[idx].getAsLongInt();
      if(firstDay == 0) {
        // 0 == vbUseSystem, so we will use the default "sunday"
        firstDay = 1;
      }
    }
    return firstDay;
  }

  private static boolean getOptionalBooleanParam(Value[] params, int idx) {
    if(params.length > idx) {
      return params[idx].getAsBoolean();
    }
    return false;
  }
}
