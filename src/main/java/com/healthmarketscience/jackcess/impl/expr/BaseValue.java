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
import java.util.Date;

import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public abstract class BaseValue implements Value
{
  public Boolean getAsBoolean() {
    throw invalidConversion(Value.Type.BOOLEAN);
  }

  public String getAsString() {
    throw invalidConversion(Value.Type.STRING);
  }

  public Date getAsDateTime() {
    throw invalidConversion(Value.Type.DATE_TIME);
  }

  public Long getAsLong() {
    throw invalidConversion(Value.Type.LONG);
  }

  public Double getAsDouble() {
    throw invalidConversion(Value.Type.DOUBLE);
  }

  public BigInteger getAsBigInteger() {
    throw invalidConversion(Value.Type.BIG_INT);
  }

  public BigDecimal getAsBigDecimal() {
    throw invalidConversion(Value.Type.BIG_DEC);
  }

  public Value toNumericValue() {
    throw invalidConversion(Value.Type.LONG);
  }

  private UnsupportedOperationException invalidConversion(Value.Type newType) {
    return new UnsupportedOperationException(
        getType() + " value cannot be converted to " + newType);
  }

  @Override
  public String toString() {
    return "Value[" + getType() + "] '" + get() + "'";
  }
}
