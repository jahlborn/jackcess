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
import java.text.Format;
import java.util.Date;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.ColumnImpl;


/**
 *
 * @author James Ahlborn
 */
public class BuiltinOperators 
{

  public static final Value NULL_VAL = new BaseValue() {
    public Type getType() {
      return Type.NULL;
    }
    public Object get() {
      return null;
    }
  };
  public static final Value TRUE_VAL = new BooleanValue(Boolean.TRUE);
  public static final Value FALSE_VAL = new BooleanValue(Boolean.FALSE);
  public static final Value EMPTY_STR_VAL = new StringValue("");

  private BuiltinOperators() {}

  // FIXME, null propagation:
  // http://www.utteraccess.com/wiki/index.php/Nulls_And_Their_Behavior
  // https://theaccessbuddy.wordpress.com/2012/10/24/6-logical-operators-in-ms-access-that-you-must-know-operator-types-3-of-5/
  // - number ops
  // - comparison ops
  // - logical ops (some "special")
  //   - And - can be false if one arg is false
  //   - Or - can be true if one arg is true
  // - between, not, like, in
  // - *NOT* concal op '&'
  // FIXME, Imp operator?

  public static Value negate(Value param1) {
    if(paramIsNull(param1)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = param1.getType();

    switch(mathType) {
    case BOOLEAN:
      return toValue(-getAsNumericBoolean(param1));
    // case STRING: break; unsupported
    case DATE:
    case TIME:
    case DATE_TIME:
      // dates/times get converted to date doubles for arithmetic
      double result = -param1.getAsDouble();
      return toDateValue(mathType, result, param1, null);
    case LONG:
      return toValue(-param1.getAsLong());
    case DOUBLE:
      return toValue(-param1.getAsDouble());
    case BIG_INT:
      return toValue(param1.getAsBigInteger().negate());
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal().negate());
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  public static Value add(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getSimpleMathTypePrecedence(param1, param2);

    switch(mathType) {
    case BOOLEAN:
      return toValue(getAsNumericBoolean(param1) + getAsNumericBoolean(param2));
    case STRING: 
      // string '+' is a null-propagation (handled above) concat
      return nonNullConcat(param1, param2);
    case DATE:
    case TIME:
    case DATE_TIME:
      // dates/times get converted to date doubles for arithmetic
      double result = param1.getAsDouble() + param2.getAsDouble();
      return toDateValue(mathType, result, param1, param2);
    case LONG:
      return toValue(param1.getAsLong() + param2.getAsLong());
    case DOUBLE:
      return toValue(param1.getAsDouble() + param2.getAsDouble());
    case BIG_INT:
      return toValue(param1.getAsBigInteger().add(param2.getAsBigInteger()));
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal().add(param2.getAsBigDecimal()));
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  public static Value subtract(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getSimpleMathTypePrecedence(param1, param2);

    switch(mathType) {
    case BOOLEAN:
      return toValue(getAsNumericBoolean(param1) - getAsNumericBoolean(param2));
    // case STRING: break; unsupported
    case DATE:
    case TIME:
    case DATE_TIME:
      // dates/times get converted to date doubles for arithmetic
      double result = param1.getAsDouble() - param2.getAsDouble();
      return toDateValue(mathType, result, param1, param2);
    case LONG:
      return toValue(param1.getAsLong() - param2.getAsLong());
    case DOUBLE:
      return toValue(param1.getAsDouble() - param2.getAsDouble());
    case BIG_INT:
      return toValue(param1.getAsBigInteger().subtract(param2.getAsBigInteger()));
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

    Value.Type mathType = getGeneralMathTypePrecedence(param1, param2);

    switch(mathType) {
    case BOOLEAN:
      return toValue(getAsNumericBoolean(param1) * getAsNumericBoolean(param2));
    // case STRING: break; unsupported
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      return toValue(param1.getAsLong() * param2.getAsLong());
    case DOUBLE:
      return toValue(param1.getAsDouble() * param2.getAsDouble());
    case BIG_INT:
      return toValue(param1.getAsBigInteger().multiply(param2.getAsBigInteger()));
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

    Value.Type mathType = getGeneralMathTypePrecedence(param1, param2);

    switch(mathType) {
    case BOOLEAN:
      return toValue(getAsNumericBoolean(param1) / getAsNumericBoolean(param2));
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
      return toValue(param1.getAsDouble() / param2.getAsDouble());
    case BIG_INT:
      BigInteger bip1 = param1.getAsBigInteger();
      BigInteger bip2 = param2.getAsBigInteger();
      BigInteger[] res = bip1.divideAndRemainder(bip2);
      if(res[1].compareTo(BigInteger.ZERO) == 0) {
        return toValue(res[0]);
      }
      return toValue(new BigDecimal(bip1).divide(new BigDecimal(bip2)));
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

    Value.Type mathType = getGeneralMathTypePrecedence(param1, param2);

    boolean wasDouble = false;
    switch(mathType) {
    case BOOLEAN:
      return toValue(getAsNumericBoolean(param1) / getAsNumericBoolean(param2));
    // case STRING: break; unsupported
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      return toValue(param1.getAsLong() / param2.getAsLong());
    case DOUBLE:
      wasDouble = true;
      // fallthrough
    case BIG_INT:
    case BIG_DEC:
      BigInteger result = param1.getAsBigInteger().divide(
          param2.getAsBigInteger());
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

    Value.Type mathType = getGeneralMathTypePrecedence(param1, param2);

    // jdk only supports general pow() as doubles, let's go with that
    double result = Math.pow(param1.getAsDouble(), param2.getAsDouble());

    // attempt to convert integral types back to integrals if possible
    switch(mathType) {
    case BOOLEAN:
    case LONG:
      if(isIntegral(result)) {
        return toValue((long)result);
      }
      break;
    case BIG_INT:
      if(isIntegral(result)) {
        return toValue(BigDecimal.valueOf(result).toBigInteger());
      }
      break;
    }

    return toValue(result);
  }

  @SuppressWarnings("fallthrough")
  public static Value mod(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getGeneralMathTypePrecedence(param1, param2);

    boolean wasDouble = false;
    switch(mathType) {
    case BOOLEAN:
      return toValue(getAsNumericBoolean(param1) % getAsNumericBoolean(param2));
    // case STRING: break; unsupported
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      return toValue(param1.getAsDouble() % param2.getAsDouble());
    case DOUBLE:
      wasDouble = true;
      // fallthrough
    case BIG_INT:
    case BIG_DEC:
      BigInteger result = param1.getAsBigInteger().mod(param2.getAsBigInteger());
      return (wasDouble ? toValue(result.longValue()) : toValue(result));
    default:
      throw new RuntimeException("Unexpected type " + mathType);
    }
  }

  public static Value concat(Value param1, Value param2) {

    // note, this op converts null to empty string
    if(paramIsNull(param1)) {
      param1 = EMPTY_STR_VAL;
    }

    if(paramIsNull(param2)) {
      param2 = EMPTY_STR_VAL;
    }

    return nonNullConcat(param1, param2);
  }

  private static Value nonNullConcat(Value param1, Value param2) {
    return toValue(param1.getAsString().concat(param2.getAsString()));
  }

  public static Value not(Value param1) {
    if(paramIsNull(param1)) {
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

    if(paramIsNull(param1)) {
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean();
    if(!b1) {
      return FALSE_VAL;
    }

    if(paramIsNull(param2)) {
      return NULL_VAL;
    }

    return toValue(param2.getAsBoolean());
  }

  public static Value or(Value param1, Value param2) {

    // "or" uses short-circuit logic

    if(paramIsNull(param1)) {
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean();
    if(b1) {
      return TRUE_VAL;
    }

    if(paramIsNull(param2)) {
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

    if(paramIsNull(param1)) {
      if(paramIsNull(param2) || !param2.getAsBoolean()) {
        // null propagation
        return NULL_VAL;
      }

      return TRUE_VAL;
    }

    boolean b1 = param1.getAsBoolean();
    if(!b1) {
      return TRUE_VAL;
    }

    if(paramIsNull(param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(param2.getAsBoolean());
  }

  public static Value isNull(Value param1) {
    return toValue(param1.getType() == Value.Type.NULL);
  }

  public static Value isNotNull(Value param1) {
    return toValue(param1.getType() == Value.Type.NULL);
  }

  public static Value like(Value param1, Pattern pattern) {
    if(paramIsNull(param1)) {
      // null propagation
      return NULL_VAL;
    }
    
    return toValue(pattern.matcher(param1.getAsString()).matches());
  }

  public static Value between(Value param1, Value param2, Value param3) {
    // FIXME, use delay for and() or check here?
    // null propagate any param.  uses short circuit eval of params
    if(anyParamIsNull(param1, param2, param3)) {
      // null propagation
      return NULL_VAL;
    }

    return and(greaterThanEq(param1, param2), lessThanEq(param1, param3));
  }

  public static Value notBetween(Value param1, Value param2, Value param3) {
    return not(between(param1, param2, param3));
  }

  public static Value in(Value param1, Value[] params) {

    // null propagate any param.  uses short circuit eval of params
    if(paramIsNull(param1)) {
      // null propagation
      return NULL_VAL;
    }

    for(Value val : params) {
      if(paramIsNull(val)) {
        continue;
      }

      // FIXME test
    }

    // FIXME
    return null;
  }

  public static Value notIn(Value param1, Value[] params) {
    return not(in(param1, params));
  }

  
  private static boolean anyParamIsNull(Value param1, Value param2) {
    return (paramIsNull(param1) || paramIsNull(param2));
  }

  private static boolean anyParamIsNull(Value param1, Value param2,
                                        Value param3) {
    return (paramIsNull(param1) || paramIsNull(param2) || paramIsNull(param3));
  }

  private static boolean paramIsNull(Value param1) {
    return (param1.getType() == Value.Type.NULL);
  }

  protected static int nonNullCompareTo(
      Value param1, Value param2)
  {
    Value.Type compareType = getGeneralMathTypePrecedence(param1, param2);

    switch(compareType) {
    case BOOLEAN:
      return compare(getAsNumericBoolean(param1), getAsNumericBoolean(param2));
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
    case BIG_INT:
      return param1.getAsBigInteger().compareTo(param2.getAsBigInteger());
    case BIG_DEC:
      return param1.getAsBigDecimal().compareTo(param2.getAsBigDecimal());
    default:
      throw new RuntimeException("Unexpected type " + compareType);
    }
  }

  private static int compare(long l1, long l2) {
    return ((l1 < l2) ? -1 : ((l1 > l2) ? 1 : 0));
  }

  private static long getAsNumericBoolean(Value v) {
    return BooleanValue.numericBoolean(v.getAsBoolean());
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
    return new BigIntegerValue(s);
  }

  public static Value toValue(BigDecimal s) {
    return new BigDecimalValue(s);
  }

  private static Value toDateValue(Value.Type type, double v, 
                                   Value param1, Value param2)
  {
    // FIXME find format from first matching param
    DateFormat fmt = null;
    // if(param1.getType() == type) {
    //   fmt = (DateFormat)param1.getFormat();
    // } else if(param2 != null) {
    //   fmt = (DateFormat)param2.getFormat();
    // }

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

  private static Value.Type getSimpleMathTypePrecedence(
      Value param1, Value param2)
  {
    Value.Type t1 = param1.getType();
    Value.Type t2 = param2.getType();

    if(t1 == t2) {
      return t1;
    }

    if((t1 == Value.Type.STRING) || (t2 == Value.Type.STRING)) {
      // string always wins
      return Value.Type.STRING;
    }

    // for "simple" math, keep as date/times
    if(t1.isTemporal() || t2.isTemporal()) {
      return (t1.isTemporal() ?
              (t2.isTemporal() ? 
               // for mixed temporal types, always go to date/time
               Value.Type.DATE_TIME : t1) :
              t2);
    }

    // if both types are integral, choose "largest"
    if(t1.isIntegral() && t2.isIntegral()) {
      return max(t1, t2);
    }

    // choose largest relevant floating-point type
    return max(t1.getPreferredFPType(), t2.getPreferredFPType());
  }

  private static Value.Type getGeneralMathTypePrecedence(
      Value param1, Value param2)
  {
    Value.Type t1 = param1.getType();
    Value.Type t2 = param2.getType();

    // note: for general math, date/time become double

    if(t1 == t2) {

      if(t1.isTemporal()) {
        return Value.Type.DOUBLE;
      }

      return t1;
    }

    if((t1 == Value.Type.STRING) || (t2 == Value.Type.STRING)) {
      // string always wins
      return Value.Type.STRING;
    }

    // if both types are integral, choose "largest"
    if(t1.isIntegral() && t2.isIntegral()) {
      return max(t1, t2);
    }

    // choose largest relevant floating-point type
    return max(t1.getPreferredFPType(), t2.getPreferredFPType());
  }

  private static Value.Type max(Value.Type t1, Value.Type t2) {
    return ((t1.compareTo(t2) > 0) ? t1 : t2);
  }

  private static boolean isIntegral(double d) {
    return (d == Math.rint(d));
  }

}
