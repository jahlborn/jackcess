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
import java.util.Date;

import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.EvalException;

/**
 *
 * @author James Ahlborn
 */
public abstract class BaseValue implements Value
{
  public boolean isNull() {
    return(getType() == Type.NULL);
  }

  public boolean getAsBoolean() {
    throw invalidConversion(Value.Type.LONG);
  }

  public String getAsString() {
    throw invalidConversion(Value.Type.STRING);
  }

  public Date getAsDateTime(EvalContext ctx) {
    throw invalidConversion(Value.Type.DATE_TIME);
  }

  public Integer getAsLongInt() {
    throw invalidConversion(Value.Type.LONG);
  }

  public Double getAsDouble() {
    throw invalidConversion(Value.Type.DOUBLE);
  }

  public BigDecimal getAsBigDecimal() {
    throw invalidConversion(Value.Type.BIG_DEC);
  }

  private EvalException invalidConversion(Value.Type newType) {
    return new EvalException(
        getType() + " value cannot be converted to " + newType);
  }

  protected Integer roundToLongInt() {
    return getAsBigDecimal().setScale(0, BuiltinOperators.ROUND_MODE)
      .intValueExact();
  }
  
  @Override
  public String toString() {
    return "Value[" + getType() + "] '" + get() + "'";
  }
}
