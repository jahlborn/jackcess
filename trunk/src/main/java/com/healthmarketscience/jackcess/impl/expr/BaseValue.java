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
import java.time.LocalDateTime;

import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public abstract class BaseValue implements Value
{
  @Override
  public boolean isNull() {
    return(getType() == Type.NULL);
  }

  @Override
  public boolean getAsBoolean(LocaleContext ctx) {
    throw invalidConversion(Type.LONG);
  }

  @Override
  public String getAsString(LocaleContext ctx) {
    throw invalidConversion(Type.STRING);
  }

  @Override
  public LocalDateTime getAsLocalDateTime(LocaleContext ctx) {
    return (LocalDateTime)getAsDateTimeValue(ctx).get();
  }

  @Override
  public Value getAsDateTimeValue(LocaleContext ctx) {
    throw invalidConversion(Type.DATE_TIME);
  }

  @Override
  public Integer getAsLongInt(LocaleContext ctx) {
    throw invalidConversion(Type.LONG);
  }

  @Override
  public Double getAsDouble(LocaleContext ctx) {
    throw invalidConversion(Type.DOUBLE);
  }

  @Override
  public BigDecimal getAsBigDecimal(LocaleContext ctx) {
    throw invalidConversion(Type.BIG_DEC);
  }

  protected EvalException invalidConversion(Type newType) {
    return new EvalException(
        this + " cannot be converted to " + newType);
  }

  protected Integer roundToLongInt(LocaleContext ctx) {
    return getAsBigDecimal(ctx).setScale(0, NumberFormatter.ROUND_MODE)
      .intValueExact();
  }

  @Override
  public String toString() {
    return "Value[" + getType() + "] '" + get() + "'";
  }
}
