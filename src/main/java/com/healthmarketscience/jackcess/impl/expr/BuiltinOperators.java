/*
Copyright (c) 2016 James Ahlborn

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
import java.math.BigInteger;
import java.text.DateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.ColumnImpl;


/**
 *
 * @author James Ahlborn
 */
public class BuiltinOperators 
{
  private static final String DIV_BY_ZERO = "/ by zero";

  public static final Value NULL_VAL = new BaseValue() {
    @Override public boolean isNull() {
      return true;
    }
    public Type getType() {
      return Type.NULL;
    }
    public Object get() {
      return null;
    }
  };
  // access seems to like -1 for true and 0 for false (boolean values are
  // basically an illusion)
  public static final Value TRUE_VAL = new LongValue(-1L);
  public static final Value FALSE_VAL = new LongValue(0L);
  public static final Value EMPTY_STR_VAL = new StringValue("");

  private enum CoercionType {
    SIMPLE(true, true), GENERAL(false, true), COMPARE(false, false);

    final boolean _preferTemporal;
    final boolean _allowCoerceStringToNum;

    private CoercionType(boolean preferTemporal,
                         boolean allowCoerceStringToNum) {
      _preferTemporal = preferTemporal;
      _allowCoerceStringToNum = allowCoerceStringToNum;
    }
  }

  private BuiltinOperators() {}

  // null propagation rules:
  // http://www.utteraccess.com/wiki/index.php/Nulls_And_Their_Behavior
  // https://theaccessbuddy.wordpress.com/2012/10/24/6-logical-operators-in-ms-access-that-you-must-know-operator-types-3-of-5/
  // - number ops
  // - comparison ops
  // - logical ops (some "special")
  //   - And - can be false if one arg is false
  //   - Or - can be true if one arg is true
  // - between, not, like, in
  // - *NOT* concal op '&'

  public static Value negate(EvalContext ctx, Value param1) {
    if(param1.isNull()) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = param1.getType();

    switch(mathType) {
    // case STRING: break; unsupported
    case DATE:
    case TIME:
    case DATE_TIME:
      // dates/times get converted to date doubles for arithmetic
      double result = -param1.getAsDouble();
      return toDateValue(ctx, mathType, result, param1, null);
    case LONG:
      return toValue(-param1.getAsLong());
    case DOUBLE:
      return toValue(-param1.getAsDouble());
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal().negate());
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  public static Value add(EvalContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(param1, param2, 
                                                CoercionType.SIMPLE);

    switch(mathType) {
    case STRING: 
      // string '+' is a null-propagation (handled above) concat
      return nonNullConcat(param1, param2);
    case DATE:
    case TIME:
    case DATE_TIME:
      // dates/times get converted to date doubles for arithmetic
      double result = param1.getAsDouble() + param2.getAsDouble();
      return toDateValue(ctx, mathType, result, param1, param2);
    case LONG:
      return toValue(param1.getAsLong() + param2.getAsLong());
    case DOUBLE:
      return toValue(param1.getAsDouble() + param2.getAsDouble());
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal().add(param2.getAsBigDecimal()));
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  public static Value subtract(EvalContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(param1, param2, 
                                                CoercionType.SIMPLE);

    switch(mathType) {
    // case STRING: break; unsupported
    case DATE:
    case TIME:
    case DATE_TIME:
      // dates/times get converted to date doubles for arithmetic
      double result = param1.getAsDouble() - param2.getAsDouble();
      return toDateValue(ctx, mathType, result, param1, param2);
    case LONG:
      return toValue(param1.getAsLong() - param2.getAsLong());
    case DOUBLE:
      return toValue(param1.getAsDouble() - param2.getAsDouble());
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal().subtract(param2.getAsBigDecimal()));
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  public static Value multiply(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(param1, param2, 
                                                CoercionType.GENERAL);

    switch(mathType) {
    // case STRING: break; unsupported
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      return toValue(param1.getAsLong() * param2.getAsLong());
    case DOUBLE:
      return toValue(param1.getAsDouble() * param2.getAsDouble());
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal().multiply(param2.getAsBigDecimal()));
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  public static Value divide(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(param1, param2, 
                                                CoercionType.GENERAL);

    switch(mathType) {
    // case STRING: break; unsupported
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      long lp1 = param1.getAsLong();
      long lp2 = param2.getAsLong();
      if((lp1 % lp2) == 0) {
        return toValue(lp1 / lp2);
      }
      return toValue((double)lp1 / (double)lp2);
    case DOUBLE:
      double d2 = param2.getAsDouble();
      if(d2 == 0.0d) {
        throw new ArithmeticException(DIV_BY_ZERO);
      }
      return toValue(param1.getAsDouble() / d2);
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal().divide(param2.getAsBigDecimal()));
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  @SuppressWarnings("fallthrough")
  public static Value intDivide(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(param1, param2, 
                                                CoercionType.GENERAL);

    boolean wasDouble = false;
    switch(mathType) {
    // case STRING: break; unsupported
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      return toValue(param1.getAsLong() / param2.getAsLong());
    case DOUBLE:
      wasDouble = true;
      // fallthrough
    case BIG_DEC:
      BigInteger result = getAsBigInteger(param1).divide(
          getAsBigInteger(param2));
      return (wasDouble ? toValue(result.longValue()) : toValue(result));
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  public static Value exp(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(param1, param2, 
                                                CoercionType.GENERAL);

    // jdk only supports general pow() as doubles, let's go with that
    double result = Math.pow(param1.getAsDouble(), param2.getAsDouble());

    // attempt to convert integral types back to integrals if possible
    if((mathType == Value.Type.LONG) && isIntegral(result)) {
      return toValue((long)result);
    }

    return toValue(result);
  }

  @SuppressWarnings("fallthrough")
  public static Value mod(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(param1, param2, 
                                                CoercionType.GENERAL);

    boolean wasDouble = false;
    switch(mathType) {
    // case STRING: break; unsupported
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      return toValue(param1.getAsLong() % param2.getAsLong());
    case DOUBLE:
      wasDouble = true;
      // fallthrough
    case BIG_DEC:
      BigInteger bi1 = getAsBigInteger(param1);
      BigInteger bi2 = getAsBigInteger(param2).abs();
      if(bi2.signum() == 0) {
        throw new ArithmeticException(DIV_BY_ZERO);
      }
      BigInteger result = bi1.mod(bi2);
      // BigInteger.mod differs from % when using negative values, need to
      // make them consistent
      if((bi1.signum() == -1) && (result.signum() == 1)) {
        result = result.subtract(bi2);
      }
      return (wasDouble ? toValue(result.longValue()) : toValue(result));
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  public static Value concat(Value param1, Value param2) {

    // note, this op converts null to empty string
    if(param1.isNull()) {
      param1 = EMPTY_STR_VAL;
    }

    if(param2.isNull()) {
      param2 = EMPTY_STR_VAL;
    }

    return nonNullConcat(param1, param2);
  }

  private static Value nonNullConcat(Value param1, Value param2) {
    return toValue(param1.getAsString().concat(param2.getAsString()));
  }

  public static Value not(Value param1) {
    if(param1.isNull()) {
      // null propagation
      return NULL_VAL;
    }
    
    return toValue(!param1.getAsBoolean());
  }

  public static Value lessThan(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(param1, param2) < 0);
  }

  public static Value greaterThan(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(param1, param2) > 0);
  }

  public static Value lessThanEq(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(param1, param2) <= 0);
  }

  public static Value greaterThanEq(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(param1, param2) >= 0);
  }

  public static Value equals(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(param1, param2) == 0);
  }

  public static Value notEquals(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(param1, param2) != 0);
  }

  public static Value and(Value param1, Value param2) {

    // "and" uses short-circuit logic

    if(param1.isNull()) {
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean();
    if(!b1) {
      return FALSE_VAL;
    }

    if(param2.isNull()) {
      return NULL_VAL;
    }

    return toValue(param2.getAsBoolean());
  }

  public static Value or(Value param1, Value param2) {

    // "or" uses short-circuit logic

    if(param1.isNull()) {
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean();
    if(b1) {
      return TRUE_VAL;
    }

    if(param2.isNull()) {
      return NULL_VAL;
    }

    return toValue(param2.getAsBoolean());
  }

  public static Value eqv(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean();
    boolean b2 = param2.getAsBoolean();

    return toValue(b1 == b2);
  }

  public static Value xor(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean();
    boolean b2 = param2.getAsBoolean();

    return toValue(b1 ^ b2);
  }

  public static Value imp(Value param1, Value param2) {

    // "imp" uses short-circuit logic

    if(param1.isNull()) {
      if(param2.isNull() || !param2.getAsBoolean()) {
        // null propagation
        return NULL_VAL;
      }

      return TRUE_VAL;
    }

    boolean b1 = param1.getAsBoolean();
    if(!b1) {
      return TRUE_VAL;
    }

    if(param2.isNull()) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(param2.getAsBoolean());
  }

  public static Value isNull(Value param1) {
    return toValue(param1.isNull());
  }

  public static Value isNotNull(Value param1) {
    return toValue(!param1.isNull());
  }

  public static Value like(Value param1, Pattern pattern) {
    if(param1.isNull()) {
      // null propagation
      return NULL_VAL;
    }
    
    return toValue(pattern.matcher(param1.getAsString()).matches());
  }

  public static Value between(Value param1, Value param2, Value param3) {
    // null propagate any param.  uses short circuit eval of params
    if(anyParamIsNull(param1, param2, param3)) {
      // null propagation
      return NULL_VAL;
    }

    // the between values can be in either order!?!
    Value min = param2;
    Value max = param3;
    Value gt = greaterThan(min, max);
    if(gt.getAsBoolean()) {
      min = param3;
      max = param2;
    }

    return and(greaterThanEq(param1, min), lessThanEq(param1, max));
  }

  public static Value notBetween(Value param1, Value param2, Value param3) {
    return not(between(param1, param2, param3));
  }

  public static Value in(Value param1, Value[] params) {

    // null propagate any param.  uses short circuit eval of params
    if(param1.isNull()) {
      // null propagation
      return NULL_VAL;
    }

    for(Value val : params) {
      if(val.isNull()) {
        continue;
      }

      Value eq = equals(param1, val);
      if(eq.getAsBoolean()) {
        return TRUE_VAL;
      }
    }

    return FALSE_VAL;
  }

  public static Value notIn(Value param1, Value[] params) {
    return not(in(param1, params));
  }

  
  private static boolean anyParamIsNull(Value param1, Value param2) {
    return (param1.isNull() || param2.isNull());
  }

  private static boolean anyParamIsNull(Value param1, Value param2,
                                        Value param3) {
    return (param1.isNull() || param2.isNull() || param3.isNull());
  }

  protected static int nonNullCompareTo(
      Value param1, Value param2)
  {
    // note that comparison does not do string to num coercion
    Value.Type compareType = getMathTypePrecedence(param1, param2, 
                                                   CoercionType.COMPARE);

    switch(compareType) {
    case STRING:
      // string comparison is only valid if _both_ params are strings
      if(param1.getType() != param2.getType()) {
        throw new RuntimeException("Unexpected type " + compareType);
      }
      return param1.getAsString().compareToIgnoreCase(param2.getAsString());
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      return param1.getAsLong().compareTo(param2.getAsLong());
    case DOUBLE:
      return param1.getAsDouble().compareTo(param2.getAsDouble());
    case BIG_DEC:
      return param1.getAsBigDecimal().compareTo(param2.getAsBigDecimal());
    default:
      throw new RuntimeException("Unexpected type " + compareType);
    }
  }

  public static Value toValue(boolean b) {
    return (b ? TRUE_VAL : FALSE_VAL);
  }

  public static Value toValue(String s) {
    return new StringValue(s);
  }

  public static Value toValue(Long s) {
    return new LongValue(s);
  }

  public static Value toValue(Double s) {
    return new DoubleValue(s);
  }

  public static Value toValue(BigInteger s) {
    return toValue(new BigDecimal(s));
  }

  public static Value toValue(BigDecimal s) {
    return new BigDecimalValue(s);
  }

  private static Value toDateValue(EvalContext ctx, Value.Type type, double v, 
                                   Value param1, Value param2)
  {
    DateFormat fmt = null;
    if((param1 instanceof BaseDateValue) && (param1.getType() == type)) {
      fmt = ((BaseDateValue)param1).getFormat();
    } else if((param2 instanceof BaseDateValue) && (param2.getType() == type)) {
      fmt = ((BaseDateValue)param2).getFormat();
    } else {
      String fmtStr = null;
      switch(type) {
      case DATE:
        fmtStr = ctx.getTemporalConfig().getDefaultDateFormat();
        break;
      case TIME:
        fmtStr = ctx.getTemporalConfig().getDefaultTimeFormat();
        break;
      case DATE_TIME:
        fmtStr = ctx.getTemporalConfig().getDefaultDateTimeFormat();
        break;
      default:
        throw new RuntimeException("Unexpected type " + type);
      }
      fmt = ctx.createDateFormat(fmtStr);
    }

    Date d = new Date(ColumnImpl.fromDateDouble(v, fmt.getCalendar()));

    switch(type) {
    case DATE:
      return new DateValue(d, fmt);
    case TIME:
      return new TimeValue(d, fmt);
    case DATE_TIME:
      return new DateTimeValue(d, fmt);
    default:
      throw new RuntimeException("Unexpected type " + type);
    }
  }

  private static Value.Type getMathTypePrecedence(
      Value param1, Value param2, CoercionType cType)
  {
    Value.Type t1 = param1.getType();
    Value.Type t2 = param2.getType();

    // note: for general math, date/time become double

    if(t1 == t2) {

      if(!cType._preferTemporal && t1.isTemporal()) {
        return t1.getPreferredNumericType();
      }

      return t1;
    }

    if((t1 == Value.Type.STRING) || (t2 == Value.Type.STRING)) {

      if(cType._allowCoerceStringToNum) {
        // see if this is mixed string/numeric and the string can be coerced
        // to a number
        Value.Type numericType = coerceStringToNumeric(param1, param2, cType);
        if(numericType != null) {
          // string can be coerced to number
          return numericType;
        }
      }

      // string always wins
      return Value.Type.STRING;
    }

    // for "simple" math, keep as date/times
    if(cType._preferTemporal &&
       (t1.isTemporal() || t2.isTemporal())) {
      return (t1.isTemporal() ?
              (t2.isTemporal() ? 
               // for mixed temporal types, always go to date/time
               Value.Type.DATE_TIME : t1) :
              t2);
    }

    t1 = t1.getPreferredNumericType();
    t2 = t2.getPreferredNumericType();

    // if both types are integral, choose "largest"
    if(t1.isIntegral() && t2.isIntegral()) {
      return max(t1, t2);
    }

    // choose largest relevant floating-point type
    return max(t1.getPreferredFPType(), t2.getPreferredFPType());
  }

  private static Value.Type coerceStringToNumeric(
      Value param1, Value param2, CoercionType cType) {
    Value.Type t1 = param1.getType();
    Value.Type t2 = param2.getType();

    Value.Type prefType = null;
    Value strParam = null;
    if(t1.isNumeric()) {
      prefType = t1;
      strParam = param2;
    } else if(t2.isNumeric()) {
      prefType = t2;
      strParam = param1;
    } else if(t1.isTemporal()) {
      prefType = (cType._preferTemporal ? t1 : t1.getPreferredNumericType());
      strParam = param2;
    } else if(t2.isTemporal()) {
      prefType = (cType._preferTemporal ? t2 : t2.getPreferredNumericType());
      strParam = param1;
    } else {
      // no numeric type involved
      return null;
    }

    try {
      // see if string can be coerced to a number
      strParam.getAsBigDecimal();
      return prefType;
    } catch(NumberFormatException ignored) {
      // not a number
    }

    return null;
  }

  private static Value.Type max(Value.Type t1, Value.Type t2) {
    return ((t1.compareTo(t2) > 0) ? t1 : t2);
  }

  static boolean isIntegral(double d) {
    return ((d == Math.rint(d)) && !Double.isInfinite(d) && !Double.isNaN(d));
  }

  private static BigInteger getAsBigInteger(Value v) {
    return v.getAsBigDecimal().toBigInteger();
  }
}
