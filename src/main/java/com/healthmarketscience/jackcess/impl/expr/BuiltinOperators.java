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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.RuntimeIOException;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.expr.Expression;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.RowContext;


/**
 *
 * @author James Ahlborn
 */
public class BuiltinOperators 
{

  public static final Value NULL_VAL = 
    new SimpleValue(Value.Type.NULL, null);
  public static final Value TRUE_VAL = 
    new SimpleValue(Value.Type.BOOLEAN, Boolean.TRUE);
  public static final Value FALSE_VAL = 
    new SimpleValue(Value.Type.BOOLEAN, Boolean.FALSE);

  public static class SimpleValue implements Value
  {
    private final Value.Type _type;
    private final Object _val;

    public SimpleValue(Value.Type type, Object val) {
      _type = type;
      _val = val;
    }

    public Value.Type getType() {
      return _type;
    }

    public Object get() {
      return _val;
    }
  }

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
    // FIXME
    return null;
  }

  public static Value add(Value param1, Value param2) {
    // FIXME
    return null;
  }

  public static Value subtract(Value param1, Value param2) {
    // FIXME
    return null;
  }

  public static Value multiply(Value param1, Value param2) {
    // FIXME
    return null;
  }

  public static Value divide(Value param1, Value param2) {
    // FIXME
    return null;
  }

  public static Value intDivide(Value param1, Value param2) {
    // FIXME
    return null;
  }

  public static Value exp(Value param1, Value param2) {
    // FIXME
    return null;
  }

  public static Value concat(Value param1, Value param2) {
    // note, this op converts null to empty string
    

    // FIXME
    return null;
  }

  public static Value mod(Value param1, Value param2) {
    // FIXME
    return null;
  }

  public static Value not(Value param1) {
    if(paramIsNull(param1)) {
      // null propagation
      return NULL_VAL;
    }
    
    return toValue(!nonNullValueToBoolean(param1));
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

    boolean b1 = nonNullValueToBoolean(param1);
    if(!b1) {
      return FALSE_VAL;
    }

    if(paramIsNull(param2)) {
      return NULL_VAL;
    }

    return toValue(nonNullValueToBoolean(param2));
  }

  public static Value or(Value param1, Value param2) {

    // "or" uses short-circuit logic

    if(paramIsNull(param1)) {
      return NULL_VAL;
    }

    boolean b1 = nonNullValueToBoolean(param1);
    if(b1) {
      return TRUE_VAL;
    }

    if(paramIsNull(param2)) {
      return NULL_VAL;
    }

    return toValue(nonNullValueToBoolean(param2));
  }

  public static Value eqv(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    boolean b1 = nonNullValueToBoolean(param1);
    boolean b2 = nonNullValueToBoolean(param2);

    return toValue(b1 == b2);
  }

  public static Value xor(Value param1, Value param2) {
    if(anyParamIsNull(param1, param2)) {
      // null propagation
      return NULL_VAL;
    }

    boolean b1 = nonNullValueToBoolean(param1);
    boolean b2 = nonNullValueToBoolean(param2);

    return toValue(b1 ^ b2);
  }

  public static Value imp(Value param1, Value param2) {

    // "imp" uses short-circuit logic

    if(paramIsNull(param1)) {
      if(paramIsNull(param2) || !nonNullValueToBoolean(param2)) {
        // null propagation
        return NULL_VAL;
      }

      return TRUE_VAL;
    }

    boolean b1 = nonNullValueToBoolean(param1);
    if(!b1) {
      return TRUE_VAL;
    }

    if(paramIsNull(param2)) {
      // null propagation
      return NULL_VAL;
    }

    return toValue(nonNullValueToBoolean(param2));
  }

  public static Value isNull(Value param1) {
    return toValue(param1.getType() == Value.Type.NULL);
  }

  public static Value isNotNull(Value param1) {
    return toValue(param1.getType() == Value.Type.NULL);
  }

  public static Value like(Value param1, Pattern pattern) {
    // FIXME
    return null;
  }

  public static Value between(Value param1, Value param2, Value param3) {

    // null propagate any field left to right.  uses short circuit eval
    if(anyParamIsNull(param1, param2, param3)) {
      // null propagation
      return NULL_VAL;
    }

    // FIXME
    return null;
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

  protected static CharSequence paramToString(Object param) {
    try {
      return ColumnImpl.toCharSequence(param);
    } catch(IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  protected static boolean paramToBoolean(Object param) {
    // FIXME, null is false...?
    return ColumnImpl.toBooleanValue(param);
  }

  protected static Number paramToNumber(Object param) {
    // FIXME
    return null;
  }

  protected static boolean nonNullValueToBoolean(Value val) {
    switch(val.getType()) {
    case BOOLEAN:
      return (Boolean)val.get();
    case STRING:
    case DATE:
    case TIME:
    case DATE_TIME:
      // strings and dates are always true
      return true;
    case LONG:
      return (((Number)val.get()).longValue() != 0L);
    case DOUBLE:
      return (((Number)val.get()).doubleValue() != 0.0d);
    case BIG_INT:
      return (((BigInteger)val.get()).compareTo(BigInteger.ZERO) != 0L);
    case BIG_DEC:
      return (((BigDecimal)val.get()).compareTo(BigDecimal.ZERO) != 0L);
    default:
      throw new RuntimeException("Unexpected type " + val.getType());
    }
  }

  protected static int nonNullCompareTo(
      Value param1, Value param2)
  {
    // FIXME
    return 0;
  }

  public static Value toValue(boolean b) {
    return (b ? TRUE_VAL : FALSE_VAL);
  }

  public static Value toValue(Object obj) {
    if(obj == null) {
      return NULL_VAL;
    }

    if(obj instanceof Value) {
      return (Value)obj;
    }

    if(obj instanceof Boolean) {
      return (((Boolean)obj) ? TRUE_VAL : FALSE_VAL);
    }

    if(obj instanceof Date) {
      // any way to figure out whether it's a date/time/dateTime?
      return new SimpleValue(Value.Type.DATE_TIME, obj);
    }

    if(obj instanceof Number) {
      if((obj instanceof Double) || (obj instanceof Float)) {
        return new SimpleValue(Value.Type.DOUBLE, obj);
      }
      if(obj instanceof BigDecimal) {
        return new SimpleValue(Value.Type.BIG_DEC, obj);
      }
      if(obj instanceof BigInteger) {
        return new SimpleValue(Value.Type.BIG_INT, obj);
      }
      return new SimpleValue(Value.Type.LONG, obj);
    }

    try {
      return new SimpleValue(Value.Type.STRING, 
                             ColumnImpl.toCharSequence(obj).toString());
    } catch(IOException e) {
      throw new RuntimeIOException(e);
    }
  }

}
