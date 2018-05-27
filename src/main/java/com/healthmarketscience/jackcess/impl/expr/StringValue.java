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

/**
 *
 * @author James Ahlborn
 */
public class StringValue extends BaseValue
{
  private static final Object NOT_A_NUMBER = new Object();

  private final String _val;
  private Object _num;

  public StringValue(String val)
  {
    _val = val;
  }

  public Type getType() {
    return Type.STRING;
  }

  public Object get() {
    return _val;
  }

  @Override
  public boolean getAsBoolean() {
    // ms access seems to treat strings as "true"
    return true;
  }

  @Override
  public String getAsString() {
    return _val;
  }

  @Override
  public Integer getAsLongInt() {
    return roundToLongInt();
  }

  @Override
  public Double getAsDouble() {
    return getNumber().doubleValue();
  }

  @Override
  public BigDecimal getAsBigDecimal() {
    return getNumber();
  }

  protected BigDecimal getNumber() {
    if(_num instanceof BigDecimal) {
      return (BigDecimal)_num;
    }
    if(_num == null) {
      // see if it is parseable as a number
      try {
        _num = BuiltinOperators.normalize(new BigDecimal(_val));
        return (BigDecimal)_num;
      } catch(NumberFormatException nfe) {
        _num = NOT_A_NUMBER;
        // fall through to throw...
      }
    }
    throw new NumberFormatException("Invalid number '" + _val + "'");
  }
}
