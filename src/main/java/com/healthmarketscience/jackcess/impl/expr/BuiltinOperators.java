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
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.NumberFormatter;
import static com.healthmarketscience.jackcess.impl.expr.ValueSupport.*;


/**
 *
 * @author James Ahlborn
 */
public class BuiltinOperators
{
  private static final String DIV_BY_ZERO = "/ by zero";

  private static final double MIN_INT = Integer.MIN_VALUE;
  private static final double MAX_INT = Integer.MAX_VALUE;

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

  public static Value negate(LocaleContext ctx, Value param1) {
    if(param1.isNull()) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = param1.getType();

    switch(mathType) {
    case DATE:
    case TIME:
    case DATE_TIME:
      // dates/times get converted to date doubles for arithmetic
      double result = -param1.getAsDouble(ctx);
      return toDateValue(ctx, mathType, result);
    case LONG:
      return toValue(-param1.getAsLongInt(ctx));
    case DOUBLE:
      return toValue(-param1.getAsDouble(ctx));
    case STRING:
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal(ctx).negate(
                         NumberFormatter.DEC_MATH_CONTEXT));
    default:
      throw new EvalException("Unexpected type " + mathType);
    }
  }

  public static Value add(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(ctx, param1, param2,
                                                CoercionType.SIMPLE);

    switch(mathType) {
    case STRING:
      // string '+' is a null-propagation (handled above) concat
      return nonNullConcat(ctx, param1, param2);
    case DATE:
    case TIME:
    case DATE_TIME:
      // dates/times get converted to date doubles for arithmetic
      double result = param1.getAsDouble(ctx) + param2.getAsDouble(ctx);
      return toDateValue(ctx, mathType, result);
    case LONG:
      return toValue(param1.getAsLongInt(ctx) + param2.getAsLongInt(ctx));
    case DOUBLE:
      return toValue(param1.getAsDouble(ctx) + param2.getAsDouble(ctx));
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal(ctx).add(
                         param2.getAsBigDecimal(ctx),
                         NumberFormatter.DEC_MATH_CONTEXT));
    default:
      throw new EvalException("Unexpected type " + mathType);
    }
  }

  public static Value subtract(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(ctx, param1, param2,
                                                CoercionType.SIMPLE);

    switch(mathType) {
    // case STRING: break; unsupported
    case DATE:
    case TIME:
    case DATE_TIME:
      // dates/times get converted to date doubles for arithmetic
      double result = param1.getAsDouble(ctx) - param2.getAsDouble(ctx);
      return toDateValue(ctx, mathType, result);
    case LONG:
      return toValue(param1.getAsLongInt(ctx) - param2.getAsLongInt(ctx));
    case DOUBLE:
      return toValue(param1.getAsDouble(ctx) - param2.getAsDouble(ctx));
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal(ctx).subtract(
                         param2.getAsBigDecimal(ctx),
                         NumberFormatter.DEC_MATH_CONTEXT));
    default:
      throw new EvalException("Unexpected type " + mathType);
    }
  }

  public static Value multiply(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(ctx, param1, param2,
                                                CoercionType.GENERAL);

    switch(mathType) {
    // case STRING: break; unsupported
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      return toValue(param1.getAsLongInt(ctx) * param2.getAsLongInt(ctx));
    case DOUBLE:
      return toValue(param1.getAsDouble(ctx) * param2.getAsDouble(ctx));
    case BIG_DEC:
      return toValue(param1.getAsBigDecimal(ctx).multiply(
                         param2.getAsBigDecimal(ctx),
                         NumberFormatter.DEC_MATH_CONTEXT));
    default:
      throw new EvalException("Unexpected type " + mathType);
    }
  }

  public static Value divide(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(ctx, param1, param2,
                                                CoercionType.GENERAL);

    switch(mathType) {
    // case STRING: break; unsupported
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      int lp1 = param1.getAsLongInt(ctx);
      int lp2 = param2.getAsLongInt(ctx);
      if((lp1 % lp2) == 0) {
        return toValue(lp1 / lp2);
      }
      return toValue((double)lp1 / (double)lp2);
    case DOUBLE:
      double d2 = param2.getAsDouble(ctx);
      if(d2 == 0.0d) {
        throw new ArithmeticException(DIV_BY_ZERO);
      }
      return toValue(param1.getAsDouble(ctx) / d2);
    case BIG_DEC:
      return toValue(divide(param1.getAsBigDecimal(ctx), param2.getAsBigDecimal(ctx)));
    default:
      throw new EvalException("Unexpected type " + mathType);
    }
  }

  public static Value intDivide(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(ctx, param1, param2,
                                                CoercionType.GENERAL);
    if(mathType == Value.Type.STRING) {
      throw new EvalException("Unexpected type " + mathType);
    }
    return toValue(param1.getAsLongInt(ctx) / param2.getAsLongInt(ctx));
  }

  public static Value exp(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(ctx, param1, param2,
                                                CoercionType.GENERAL);

    if(mathType == Value.Type.BIG_DEC) {
      // see if we can handle the limited options supported for BigDecimal
      // (must be a positive int exponent)
      try {
        BigDecimal result = param1.getAsBigDecimal(ctx).pow(
            param2.getAsBigDecimal(ctx).intValueExact(),
            NumberFormatter.DEC_MATH_CONTEXT);
        return toValue(result);
      } catch(ArithmeticException ae) {
        // fall back to general handling via doubles...
      }
    }

    // jdk only supports general pow() as doubles, let's go with that
    double result = Math.pow(param1.getAsDouble(ctx), param2.getAsDouble(ctx));

    // attempt to convert integral types back to integrals if possible
    if((mathType == Value.Type.LONG) && isIntegral(result)) {
      return toValue((int)result);
    }

    return toValue(result);
  }

  public static Value mod(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    Value.Type mathType = getMathTypePrecedence(ctx, param1, param2,
                                                CoercionType.GENERAL);

    if(mathType == Value.Type.STRING) {
      throw new EvalException("Unexpected type " + mathType);
    }
    return toValue(param1.getAsLongInt(ctx) % param2.getAsLongInt(ctx));
  }

  public static Value concat(LocaleContext ctx, Value param1, Value param2) {

    // note, this op converts null to empty string
    if(param1.isNull()) {
      param1 = EMPTY_STR_VAL;
    }

    if(param2.isNull()) {
      param2 = EMPTY_STR_VAL;
    }

    return nonNullConcat(ctx, param1, param2);
  }

  private static Value nonNullConcat(
      LocaleContext ctx, Value param1, Value param2) {
    return toValue(param1.getAsString(ctx).concat(param2.getAsString(ctx)));
  }

  public static Value not(LocaleContext ctx, Value param1) {
    if(param1.isNull()) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(!param1.getAsBoolean(ctx));
  }

  public static Value lessThan(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(ctx, param1, param2) < 0);
  }

  public static Value greaterThan(
      LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(ctx, param1, param2) > 0);
  }

  public static Value lessThanEq(
      LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(ctx, param1, param2) <= 0);
  }

  public static Value greaterThanEq(
      LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(ctx, param1, param2) >= 0);
  }

  public static Value equals(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(ctx, param1, param2) == 0);
  }

  public static Value notEquals(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullCompareTo(ctx, param1, param2) != 0);
  }

  public static Value and(LocaleContext ctx, Value param1, Value param2) {

    // "and" uses short-circuit logic

    if(param1.isNull()) {
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean(ctx);
    if(!b1) {
      return FALSE_VAL;
    }

    if(param2.isNull()) {
      return NULL_VAL;
    }

    return toValue(param2.getAsBoolean(ctx));
  }

  public static Value or(LocaleContext ctx, Value param1, Value param2) {

    // "or" uses short-circuit logic

    if(param1.isNull()) {
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean(ctx);
    if(b1) {
      return TRUE_VAL;
    }

    if(param2.isNull()) {
      return NULL_VAL;
    }

    return toValue(param2.getAsBoolean(ctx));
  }

  public static Value eqv(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean(ctx);
    boolean b2 = param2.getAsBoolean(ctx);

    return toValue(b1 == b2);
  }

  public static Value xor(LocaleContext ctx, Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    boolean b1 = param1.getAsBoolean(ctx);
    boolean b2 = param2.getAsBoolean(ctx);

    return toValue(b1 ^ b2);
  }

  public static Value imp(LocaleContext ctx, Value param1, Value param2) {

    // "imp" uses short-circuit logic

    if(param1.isNull()) {
      if(param2.isNull() || !param2.getAsBoolean(ctx)) {
        // null propagation
        return NULL_VAL;
      }

      return TRUE_VAL;
    }

    boolean b1 = param1.getAsBoolean(ctx);
    if(!b1) {
      return TRUE_VAL;
    }

    if(param2.isNull()) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(param2.getAsBoolean(ctx));
  }

  public static Value isNull(Value param1) {
    return toValue(param1.isNull());
  }

  public static Value isNotNull(Value param1) {
    return toValue(!param1.isNull());
  }

  public static Value like(LocaleContext ctx, Value param1, Pattern pattern) {
    if(param1.isNull()) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(pattern.matcher(param1.getAsString(ctx)).matches());
  }

  public static Value notLike(
      LocaleContext ctx, Value param1, Pattern pattern) {
    return not(ctx, like(ctx, param1, pattern));
  }

  public static Value between(
      LocaleContext ctx, Value param1, Value param2, Value param3) {
    // null propagate any param.  uses short circuit eval of params
    if(anyParamIsNull(param1, param2, param3)) {
      // null propagation
      return NULL_VAL;
    }

    // the between values can be in either order!?!
    Value min = param2;
    Value max = param3;
    Value gt = greaterThan(ctx, min, max);
    if(gt.getAsBoolean(ctx)) {
      min = param3;
      max = param2;
    }

    return and(ctx, greaterThanEq(ctx, param1, min), lessThanEq(ctx, param1, max));
  }

  public static Value notBetween(
      LocaleContext ctx, Value param1, Value param2, Value param3) {
    return not(ctx, between(ctx, param1, param2, param3));
  }

  public static Value in(LocaleContext ctx, Value param1, Value[] params) {

    // null propagate any param.  uses short circuit eval of params
    if(param1.isNull()) {
      // null propagation
      return NULL_VAL;
    }

    for(Value val : params) {
      if(val.isNull()) {
        continue;
      }

      Value eq = equals(ctx, param1, val);
      if(eq.getAsBoolean(ctx)) {
        return TRUE_VAL;
      }
    }

    return FALSE_VAL;
  }

  public static Value notIn(LocaleContext ctx, Value param1, Value[] params) {
    return not(ctx, in(ctx, param1, params));
  }


  private static boolean anyParamIsNull(Value param1, Value param2) {
    return (param1.isNull() || param2.isNull());
  }

  private static boolean anyParamIsNull(Value param1, Value param2,
                                        Value param3) {
    return (param1.isNull() || param2.isNull() || param3.isNull());
  }

  protected static int nonNullCompareTo(
      LocaleContext ctx, Value param1, Value param2)
  {
    // note that comparison does not do string to num coercion
    Value.Type compareType = getMathTypePrecedence(ctx, param1, param2,
                                                   CoercionType.COMPARE);

    switch(compareType) {
    case STRING:
      // string comparison is only valid if _both_ params are strings
      if(param1.getType() != param2.getType()) {
        throw new EvalException("Unexpected type " + compareType);
      }
      return param1.getAsString(ctx).compareToIgnoreCase(param2.getAsString(ctx));
    // case DATE: break; promoted to double
    // case TIME: break; promoted to double
    // case DATE_TIME: break; promoted to double
    case LONG:
      return param1.getAsLongInt(ctx).compareTo(param2.getAsLongInt(ctx));
    case DOUBLE:
      return param1.getAsDouble(ctx).compareTo(param2.getAsDouble(ctx));
    case BIG_DEC:
      return param1.getAsBigDecimal(ctx).compareTo(param2.getAsBigDecimal(ctx));
    default:
      throw new EvalException("Unexpected type " + compareType);
    }
  }

  private static Value.Type getMathTypePrecedence(
      LocaleContext ctx, Value param1, Value param2, CoercionType cType)
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
        Value.Type numericType = coerceStringToNumeric(
            ctx, param1, param2, cType);
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

    return getPreferredNumericType(t1.getPreferredNumericType(),
                                   t2.getPreferredNumericType());
  }

  private static Value.Type getPreferredNumericType(Value.Type t1, Value.Type t2)
  {
    // if both types are integral, choose "largest"
    if(t1.isIntegral() && t2.isIntegral()) {
      return max(t1, t2);
    }

    // choose largest relevant floating-point type
    return max(t1.getPreferredFPType(), t2.getPreferredFPType());
  }

  private static Value.Type coerceStringToNumeric(
      LocaleContext ctx, Value param1, Value param2, CoercionType cType) {
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
      strParam.getAsBigDecimal(ctx);
      if(prefType.isNumeric()) {
        // seems like when strings are coerced to numbers, they are usually
        // doubles, unless the current context is decimal
        prefType = ((prefType == Value.Type.BIG_DEC) ?
                    Value.Type.BIG_DEC : Value.Type.DOUBLE);
      }
      return prefType;
    } catch(EvalException ignored) {
      // not a number
    }

    return null;
  }

  private static Value.Type max(Value.Type t1, Value.Type t2) {
    return ((t1.compareTo(t2) > 0) ? t1 : t2);
  }

  static BigDecimal divide(BigDecimal num, BigDecimal denom) {
    return num.divide(denom, NumberFormatter.DEC_MATH_CONTEXT);
  }

  static boolean isIntegral(double d) {
    double id = Math.rint(d);
    return ((d == id) && (d >= MIN_INT) && (d <= MAX_INT) &&
            !Double.isInfinite(d) && !Double.isNaN(d));
  }
}
