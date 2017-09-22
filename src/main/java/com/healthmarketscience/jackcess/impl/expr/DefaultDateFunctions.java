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
  
  private DefaultDateFunctions() {}

  static void init() {
    // dummy method to ensure this class is loaded
  }

  public static final Function DATE = registerFunc(new Func0("Date") {
    @Override
    public boolean isPure() {
      return false;
    }
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat df = BuiltinOperators.getDateFormatForType(ctx, Value.Type.DATE);
      double dd = ColumnImpl.toDateDouble(System.currentTimeMillis(), df.getCalendar());
      // the integral part of the date/time double is the date value.  discard
      // the fractional portion
      dd = ((long)dd);
      return BuiltinOperators.toValue(Value.Type.DATE, new Date(), df);
    }
  });

  public static final Function NOW = registerFunc(new Func0("Now") {
    @Override
    public boolean isPure() {
      return false;
    }
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat df = BuiltinOperators.getDateFormatForType(ctx, Value.Type.DATE_TIME);
      return BuiltinOperators.toValue(Value.Type.DATE_TIME, new Date(), df);
    }
  });

  public static final Function TIME = registerFunc(new Func0("Time") {
    @Override
    public boolean isPure() {
      return false;
    }
    @Override
    protected Value eval0(EvalContext ctx) {
      DateFormat df = BuiltinOperators.getDateFormatForType(ctx, Value.Type.TIME);
      double dd = ColumnImpl.toDateDouble(System.currentTimeMillis(), df.getCalendar());
      // the fractional part of the date/time double is the time value.  discard
      // the integral portion
      dd = Math.IEEEremainder(dd, 1.0d);
      return BuiltinOperators.toValue(Value.Type.TIME, new Date(), df);
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
      // convert from 0 based to 1 based value
      return BuiltinOperators.toValue(
          nonNullToCalendarField(ctx, param1, Calendar.YEAR) + 1);
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
      // FIXME handle first day of week
      // if(params.length > 1) {
      //   int firstDay = params[1].getAsLong();
      // }
      return BuiltinOperators.toValue(day);
    }
  });

  
  private static int nonNullToCalendarField(EvalContext ctx, Value param,
                                            int field) {
    return nonNullToCalendar(ctx, param).get(field);
  }
  
  private static Calendar nonNullToCalendar(EvalContext ctx, Value param) {
    param = nonNullToDateValue(ctx, param);
    if(param == null) {
      // not a date/time
      throw new IllegalStateException("Invalid date/time expression '" + param + "'");
    }

    Calendar cal = 
      ((param instanceof BaseDateValue) ?
       ((BaseDateValue)param).getFormat().getCalendar() :
       BuiltinOperators.getDateFormatForType(ctx, param.getType()).getCalendar());

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

    boolean hasDate = (((long)dd) != 0L);
    boolean hasTime = (Math.IEEEremainder(dd, 1.0d) != 0.0d);

    Value.Type type = (hasDate ? (hasTime ? Value.Type.DATE_TIME : Value.Type.DATE) :
                       Value.Type.TIME);
    DateFormat df = BuiltinOperators.getDateFormatForType(ctx, type);
    Date d = new Date(ColumnImpl.fromDateDouble(dd, df.getCalendar()));
    return BuiltinOperators.toValue(type, d, df);
  }
}
