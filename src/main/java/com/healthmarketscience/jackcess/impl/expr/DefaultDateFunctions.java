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
import java.util.Calendar;
import java.util.Date;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import static com.healthmarketscience.jackcess.impl.expr.DefaultFunctions.*;

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
      DateFormat fmt = BuiltinOperators.getDateFormatForType(ctx, Value.Type.DATE);
      double dd = dateOnly(currentTimeDouble(fmt));
      return BuiltinOperators.toValue(Value.Type.DATE, dd, fmt);
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
      DateFormat fmt = BuiltinOperators.getDateFormatForType(ctx, Value.Type.DATE);
      return BuiltinOperators.toValue(Value.Type.DATE, dd, fmt);
    }
  });

  public static final Function NOW = registerFunc(new Func0("Now") {
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat fmt = BuiltinOperators.getDateFormatForType(ctx, Value.Type.DATE_TIME);
      return BuiltinOperators.toValue(Value.Type.DATE_TIME, new Date(), fmt);
    }
  });

  public static final Function TIME = registerFunc(new Func0("Time") {
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat fmt = BuiltinOperators.getDateFormatForType(ctx, Value.Type.TIME);
      double dd = timeOnly(currentTimeDouble(fmt));
      return BuiltinOperators.toValue(Value.Type.TIME, dd, fmt);
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
      DateFormat fmt = BuiltinOperators.getDateFormatForType(ctx, Value.Type.TIME);
      return BuiltinOperators.toValue(Value.Type.TIME, dd, fmt);
    }
  });

  public static final Function TIMER = registerFunc(new Func0("Timer") {
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat fmt = BuiltinOperators.getDateFormatForType(ctx, Value.Type.TIME);
      double dd = timeOnly(currentTimeDouble(fmt)) * DSECONDS_PER_DAY;
      return BuiltinOperators.toValue(dd);
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
      DateFormat fmt = BuiltinOperators.getDateFormatForType(ctx, Value.Type.TIME);
      double dd = totalSeconds / DSECONDS_PER_DAY;
      return BuiltinOperators.toValue(Value.Type.TIME, dd, fmt);
    }
  });

  public static final Function HOUR = registerFunc(new Func1NullIsNull("Hour") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return BuiltinOperators.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.HOUR_OF_DAY));
    }
  });

  public static final Function MINUTE = registerFunc(new Func1NullIsNull("Minute") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return BuiltinOperators.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.MINUTE));
    }
  });

  public static final Function SECOND = registerFunc(new Func1NullIsNull("Second") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return BuiltinOperators.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.SECOND));
    }
  });

  public static final Function YEAR = registerFunc(new Func1NullIsNull("Year") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return BuiltinOperators.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.YEAR));
    }
  });

  public static final Function MONTH = registerFunc(new Func1NullIsNull("Month") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      // convert from 0 based to 1 based value
      return BuiltinOperators.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.MONTH) + 1);
    }
  });

  public static final Function DAY = registerFunc(new Func1NullIsNull("Day") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return BuiltinOperators.toValue(
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
      int day = nonNullToCalendarField(ctx, param1, Calendar.DAY_OF_WEEK);

      // vbSunday (default)
      int firstDay = 1;
      if(params.length > 1) {
        firstDay = params[1].getAsLongInt();
        if(firstDay == 0) {
          // 0 == vbUseSystem, so we will use the default "sunday"
          firstDay = 1;
        }
      }

      // shift all the values to 0 based to calculate the correct value, then
      // back to 1 based to return the result
      day = (((day - 1) - (firstDay - 1) + 7) % 7) + 1;

      return BuiltinOperators.toValue(day);
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
      // see if we can coerce to date/time

      // FIXME use ExpressionatorTokenizer to detect explicit date/time format

      try {
        return numberToDateValue(ctx, param.getAsDouble());
      } catch(NumberFormatException ignored) {
        // not a number
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

    Value.Type type = (hasDate ? (hasTime ? Value.Type.DATE_TIME : Value.Type.DATE) :
                       Value.Type.TIME);
    DateFormat fmt = BuiltinOperators.getDateFormatForType(ctx, type);
    return BuiltinOperators.toValue(type, dd, fmt);
  }

  private static DateFormat getDateValueFormat(EvalContext ctx, Value param) {
    return ((param instanceof BaseDateValue) ?
            ((BaseDateValue)param).getFormat() :
       BuiltinOperators.getDateFormatForType(ctx, param.getType()));
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
}
