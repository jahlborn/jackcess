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

import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public class BooleanValue extends BaseValue
{
  private final Boolean _val;

  public BooleanValue(Boolean val) 
  {
    _val = val;
  }

  public Type getType() {
    return Type.BOOLEAN;
  }

  public Object get() {
    return _val;
  }

  @Override
  public Boolean getAsBoolean() {
    return _val;
  }

  @Override
  public String getAsString() {
    // access seems to like -1 for true and 0 for false
    return (_val ? "-1" : "0");
  }

  @Override
  public Long getAsLong() {
    // access seems to like -1 for true and 0 for false
    return numericBoolean(_val);
  }

  @Override
  public Double getAsDouble() {
    // access seems to like -1 for true and 0 for false
    return (_val ? -1d : 0d);
  }

  @Override
  public BigInteger getAsBigInteger() {
    // access seems to like -1 for true and 0 for false
    return (_val ? BigInteger.valueOf(-1) : BigInteger.ZERO);
  }

  @Override
  public BigDecimal getAsBigDecimal() {
    // access seems to like -1 for true and 0 for false
    return (_val ? BigDecimal.valueOf(-1) : BigDecimal.ZERO);
  }

  @Override
  public Value toNumericValue() {
    return new LongValue(getAsLong());
  }

  protected static long numericBoolean(Boolean b) {
    // access seems to like -1 for true and 0 for false
    return (b ? -1L : 0L);
  }
}
