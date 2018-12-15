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


import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;

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

  private static final WeekFields SUNDAY_FIRST =
    WeekFields.of(DayOfWeek.SUNDAY, 1);

  private DefaultDateFunctions() {}

  static void init() {
    // dummy method to ensure this class is loaded
  }

  public static final Function DATE = registerFunc(new Func0("Date") {
    @Override
    protected Value eval0(EvalContext ctx) {
      return ValueSupport.toValue(LocalDate.now());
    }
  });

  public static final Function DATEVALUE = registerFunc(new Func1NullIsNull("DateValue") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Value dv = param1.getAsDateTimeValue(ctx);
      if(dv.getType() == Value.Type.DATE) {
        return dv;
      }
      return ValueSupport.toValue(dv.getAsLocalDateTime(ctx).toLocalDate());
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

      // we have to construct incrementatlly to handle out of range values
      LocalDate ld = LocalDate.of(year,1,1).plusMonths(month - 1)
        .plusDays(day - 1);

      return ValueSupport.toValue(ld);
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
        result = param2.getAsLocalDateTime(ctx).getYear();
      } else if(intv.equalsIgnoreCase(INTV_QUARTER)) {
        result = getQuarter(param2.getAsLocalDateTime(ctx));
      } else if(intv.equalsIgnoreCase(INTV_MONTH)) {
        result = param2.getAsLocalDateTime(ctx).getMonthValue();
      } else if(intv.equalsIgnoreCase(INTV_DAY_OF_YEAR)) {
        result = param2.getAsLocalDateTime(ctx).getDayOfYear();
      } else if(intv.equalsIgnoreCase(INTV_DAY)) {
        result = param2.getAsLocalDateTime(ctx).getDayOfMonth();
      } else if(intv.equalsIgnoreCase(INTV_WEEKDAY)) {
        int dayOfWeek = param2.getAsLocalDateTime(ctx)
          .get(SUNDAY_FIRST.dayOfWeek());
        result = dayOfWeekToWeekDay(dayOfWeek, firstDay);
      } else if(intv.equalsIgnoreCase(INTV_WEEK)) {
        result = weekOfYear(ctx, param2, firstDay, firstWeekType);
      } else if(intv.equalsIgnoreCase(INTV_HOUR)) {
        result = param2.getAsLocalDateTime(ctx).getHour();
      } else if(intv.equalsIgnoreCase(INTV_MINUTE)) {
        result = param2.getAsLocalDateTime(ctx).getMinute();
      } else if(intv.equalsIgnoreCase(INTV_SECOND)) {
        result = param2.getAsLocalDateTime(ctx).getSecond();
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

      LocalDateTime ldt = param3.getAsLocalDateTime(ctx);

      if(intv.equalsIgnoreCase(INTV_YEAR)) {
        ldt = ldt.plus(val, ChronoUnit.YEARS);
      } else if(intv.equalsIgnoreCase(INTV_QUARTER)) {
        ldt = ldt.plus(val * 3, ChronoUnit.MONTHS);
      } else if(intv.equalsIgnoreCase(INTV_MONTH)) {
        ldt = ldt.plus(val, ChronoUnit.MONTHS);
      } else if(intv.equalsIgnoreCase(INTV_DAY_OF_YEAR) ||
                intv.equalsIgnoreCase(INTV_DAY) ||
                intv.equalsIgnoreCase(INTV_WEEKDAY)) {
        ldt = ldt.plus(val, ChronoUnit.DAYS);
      } else if(intv.equalsIgnoreCase(INTV_WEEK)) {
        ldt = ldt.plus(val, ChronoUnit.WEEKS);
      } else if(intv.equalsIgnoreCase(INTV_HOUR)) {
        ldt = ldt.plus(val, ChronoUnit.HOURS);
      } else if(intv.equalsIgnoreCase(INTV_MINUTE)) {
        ldt = ldt.plus(val, ChronoUnit.MINUTES);
      } else if(intv.equalsIgnoreCase(INTV_SECOND)) {
        ldt = ldt.plus(val, ChronoUnit.SECONDS);
      } else {
        throw new EvalException("Invalid interval " + intv);
      }

      return ValueSupport.toValue(ldt);
    }
  });

  public static final Function DATEDIFF = registerFunc(new FuncVar("DateDiff", 3, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {

      Value param2 = params[1];
      Value param3 = params[2];
      if(param2.isNull() || param3.isNull()) {
        return ValueSupport.NULL_VAL;
      }

      int firstDay = getFirstDayParam(ctx, params, 3);
      int firstWeekType = getFirstWeekTypeParam(ctx, params, 4);

      String intv = params[0].getAsString(ctx).trim();

      LocalDateTime ldt1 = param2.getAsLocalDateTime(ctx);
      LocalDateTime ldt2 = param3.getAsLocalDateTime(ctx);

      int sign = 1;
      if(ldt1.isAfter(ldt2)) {
        LocalDateTime tmp = ldt1;
        ldt1 = ldt2;
        ldt2 = tmp;
        sign = -1;
      }

      // NOTE: DateDiff understands leap years, but not daylight savings time
      // (i.e. it doesn't really take into account timezones).  so all time
      // based calculations assume 24 hour days.

      int result = -1;
      if(intv.equalsIgnoreCase(INTV_YEAR)) {
        result = ldt2.getYear() - ldt1.getYear();
      } else if(intv.equalsIgnoreCase(INTV_QUARTER)) {
        int y1 = ldt1.getYear();
        int q1 = getQuarter(ldt1);
        int y2 = ldt2.getYear();
        int q2 = getQuarter(ldt2);
        while(y2 > y1) {
          q2 += 4;
          --y2;
        }
        result = q2 - q1;
      } else if(intv.equalsIgnoreCase(INTV_MONTH)) {
        int y1 = ldt1.getYear();
        int m1 = ldt1.getMonthValue();
        int y2 = ldt2.getYear();
        int m2 = ldt2.getMonthValue();
        while(y2 > y1) {
          m2 += 12;
          --y2;
        }
        result = m2 - m1;
      } else if(intv.equalsIgnoreCase(INTV_DAY_OF_YEAR) ||
                intv.equalsIgnoreCase(INTV_DAY)) {
        result = getDayDiff(ldt1, ldt2);
      } else if(intv.equalsIgnoreCase(INTV_WEEKDAY)) {
        // this calulates number of 7 day periods between two dates
        result = getDayDiff(ldt1, ldt2) / 7;
      } else if(intv.equalsIgnoreCase(INTV_WEEK)) {
        // this counts number of "week of year" intervals between two dates
        WeekFields weekFields = weekFields(firstDay, firstWeekType);
        int w1 = ldt1.get(weekFields.weekOfWeekBasedYear());
        int y1 = ldt1.get(weekFields.weekBasedYear());
        int w2 = ldt2.get(weekFields.weekOfWeekBasedYear());
        int y2 = ldt2.get(weekFields.weekBasedYear());
        while(y2 > y1) {
          --y2;
          w2 += weeksInYear(y2, weekFields);
        }
        result = w2 - w1;
      } else if(intv.equalsIgnoreCase(INTV_HOUR)) {
        result = getHourDiff(ldt1, ldt2);
      } else if(intv.equalsIgnoreCase(INTV_MINUTE)) {
        result = getMinuteDiff(ldt1, ldt2);
      } else if(intv.equalsIgnoreCase(INTV_SECOND)) {
        int s1 = ldt1.getSecond();
        int s2 = ldt2.getSecond();
        int minuteDiff = getMinuteDiff(ldt1, ldt2);
        result = (s2 + (60 * minuteDiff)) - s1;
      } else {
        throw new EvalException("Invalid interval " + intv);
      }

      return ValueSupport.toValue(result * sign);
    }
  });

  public static final Function NOW = registerFunc(new Func0("Now") {
    @Override
    protected Value eval0(EvalContext ctx) {
      return ValueSupport.toValue(Value.Type.DATE_TIME,
                                  LocalDateTime.now(ctx.getZoneId()));
    }
  });

  public static final Function TIME = registerFunc(new Func0("Time") {
    @Override
    protected Value eval0(EvalContext ctx) {
      return ValueSupport.toValue(LocalTime.now(ctx.getZoneId()));
    }
  });

  public static final Function TIMEVALUE = registerFunc(new Func1NullIsNull("TimeValue") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Value dv = param1.getAsDateTimeValue(ctx);
      if(dv.getType() == Value.Type.TIME) {
        return dv;
      }
      return ValueSupport.toValue(dv.getAsLocalDateTime(ctx).toLocalTime());
    }
  });

  public static final Function TIMER = registerFunc(new Func0("Timer") {
    @Override
    protected Value eval0(EvalContext ctx) {
      double dd = LocalTime.now(ctx.getZoneId())
        .get(ChronoField.MILLI_OF_DAY) / 1000d;
      return ValueSupport.toValue(dd);
    }
  });

  public static final Function TIMESERIAL = registerFunc(new Func3("TimeSerial") {
    @Override
    protected Value eval3(EvalContext ctx, Value param1, Value param2, Value param3) {
      int hours = param1.getAsLongInt(ctx);
      int minutes = param2.getAsLongInt(ctx);
      int seconds = param3.getAsLongInt(ctx);

      // we have to construct incrementatlly to handle out of range values
      LocalTime lt = ColumnImpl.BASE_LT.plusHours(hours).plusMinutes(minutes)
        .plusSeconds(seconds);

      return ValueSupport.toValue(lt);
    }
  });

  public static final Function HOUR = registerFunc(new Func1NullIsNull("Hour") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(param1.getAsLocalDateTime(ctx).getHour());
    }
  });

  public static final Function MINUTE = registerFunc(new Func1NullIsNull("Minute") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(param1.getAsLocalDateTime(ctx).getMinute());
    }
  });

  public static final Function SECOND = registerFunc(new Func1NullIsNull("Second") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(param1.getAsLocalDateTime(ctx).getSecond());
    }
  });

  public static final Function YEAR = registerFunc(new Func1NullIsNull("Year") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(param1.getAsLocalDateTime(ctx).getYear());
    }
  });

  public static final Function MONTH = registerFunc(new Func1NullIsNull("Month") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(param1.getAsLocalDateTime(ctx).getMonthValue());
    }
  });

  public static final Function MONTHNAME = registerFunc(new FuncVar("MonthName", 1, 2) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(param1.isNull()) {
        return ValueSupport.NULL_VAL;
      }
      Month month = Month.of(param1.getAsLongInt(ctx));

      TextStyle textStyle = getTextStyle(ctx, params, 1);
      String monthName = month.getDisplayName(
          textStyle, ctx.getTemporalConfig().getLocale());
      return ValueSupport.toValue(monthName);
    }
  });

  public static final Function DAY = registerFunc(new Func1NullIsNull("Day") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return ValueSupport.toValue(
          param1.getAsLocalDateTime(ctx).getDayOfMonth());
    }
  });

  public static final Function WEEKDAY = registerFunc(new FuncVar("Weekday", 1, 2) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(param1.isNull()) {
        return ValueSupport.NULL_VAL;
      }
      int dayOfWeek = param1.getAsLocalDateTime(ctx)
        .get(SUNDAY_FIRST.dayOfWeek());

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

      TextStyle textStyle = getTextStyle(ctx, params, 1);

      int firstDay = getFirstDayParam(ctx, params, 2);

      int dayOfWeek = weekDayToDayOfWeek(weekday, firstDay);
      String weekdayName = dayOfWeek(dayOfWeek).getDisplayName(
          textStyle, ctx.getTemporalConfig().getLocale());
      return ValueSupport.toValue(weekdayName);
    }
  });

  static Value stringToDateValue(LocaleContext ctx, String valStr) {
    // see if we can coerce to date/time
    TemporalConfig.Type valTempType = ExpressionTokenizer.determineDateType(
        valStr, ctx);

    if(valTempType != null) {

      DateTimeFormatter parseDf = ctx.createDateFormatter(
          ctx.getTemporalConfig().getDateTimeFormat(valTempType));

      try {
        TemporalAccessor parsedInfo = parseDf.parse(valStr);

        LocalDate ld = ColumnImpl.BASE_LD;
        if(valTempType.includesDate()) {
          // the year may not be explicitly specified
          if(parsedInfo.isSupported(ChronoField.YEAR)) {
            ld = LocalDate.from(parsedInfo);
          } else {
            ld = MonthDay.from(parsedInfo).atYear(
                Year.now(ctx.getZoneId()).getValue());
          }
        }

        LocalTime lt = ColumnImpl.BASE_LT;
        if(valTempType.includesTime()) {
          lt = LocalTime.from(parsedInfo);
        }

        return ValueSupport.toValue(LocalDateTime.of(ld, lt));
      } catch(DateTimeException de) {
        // note a valid date/time
      }
    }

    // not a valid date string, not a date/time
    return null;
  }

  static boolean isValidDateDouble(double dd) {
    return ((dd >= MIN_DATE) && (dd <= MAX_DATE));
  }

  static Value numberToDateValue(double dd) {
    if(!isValidDateDouble(dd)) {
      // outside valid date range
      return null;
    }

    LocalDateTime ldt = ColumnImpl.ldtFromLocalDateDouble(dd);
    return ValueSupport.toValue(ldt);
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

  static int getFirstDayParam(
      LocaleContext ctx, Value[] params, int idx) {
    // vbSunday (default) 1
    // vbUseSystem 0
    return getOptionalIntParam(ctx, params, idx, 1, 0);
  }

  static int getFirstWeekTypeParam(
      LocaleContext ctx, Value[] params, int idx) {
    // vbFirstJan1 (default) 1
    // vbUseSystem 0
    return getOptionalIntParam(ctx, params, idx, 1, 0);
  }

  private static WeekFields weekFields(int firstDay, int firstWeekType) {

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

    return WeekFields.of(dayOfWeek(firstDay), minDays);
  }

  private static DayOfWeek dayOfWeek(int dayOfWeek) {
    return DayOfWeek.SUNDAY.plus(dayOfWeek - 1);
  }

  private static TextStyle getTextStyle(EvalContext ctx, Value[] params,
                                        int idx) {
    boolean abbreviate = getOptionalBooleanParam(ctx, params, 1);
    return (abbreviate ? TextStyle.SHORT : TextStyle.FULL);
  }

  private static int weekOfYear(EvalContext ctx, Value param, int firstDay,
                                int firstWeekType) {
    return weekOfYear(param.getAsLocalDateTime(ctx), firstDay, firstWeekType);
  }

  private static int weekOfYear(LocalDateTime ldt, int firstDay,
                                int firstWeekType) {
    WeekFields weekFields = weekFields(firstDay, firstWeekType);
    return ldt.get(weekFields.weekOfWeekBasedYear());
  }

  private static int weeksInYear(int year, WeekFields weekFields) {
    return (int)LocalDate.of(year,2,1).range(weekFields.weekOfWeekBasedYear())
      .getMaximum();
  }

  private static int getQuarter(LocalDateTime ldt) {
    int month = ldt.getMonthValue() - 1;
    return (month / 3) + 1;
  }

  private static int getDayDiff(LocalDateTime ldt1, LocalDateTime ldt2) {
    int y1 = ldt1.getYear();
    int d1 = ldt1.getDayOfYear();
    int y2 = ldt2.getYear();
    int d2 = ldt2.getDayOfYear();
    while(y2  > y1) {
      ldt2 = ldt2.minusYears(1);
      d2 += ldt2.range(ChronoField.DAY_OF_YEAR).getMaximum();
      y2 = ldt2.getYear();
    }
    return d2 - d1;
  }

  private static int getHourDiff(LocalDateTime ldt1, LocalDateTime ldt2) {
    int h1 = ldt1.getHour();
    int h2 = ldt2.getHour();
    int dayDiff = getDayDiff(ldt1, ldt2);
    return (h2 + (24 * dayDiff)) - h1;
  }

  private static int getMinuteDiff(LocalDateTime ldt1, LocalDateTime ldt2) {
    int m1 = ldt1.getMinute();
    int m2 = ldt2.getMinute();
    int hourDiff = getHourDiff(ldt1, ldt2);
    return (m2 + (60 * hourDiff)) - m1;
  }
}
