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


import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public abstract class BaseNumericValue extends BaseValue
{

  protected BaseNumericValue()
  {
  }

  @Override
  public Integer getAsLongInt(LocaleContext ctx) {
    return roundToLongInt(ctx);
  }

  @Override
  public Double getAsDouble(LocaleContext ctx) {
    return getNumber().doubleValue();
  }

  @Override
  public Value getAsDateTimeValue(LocaleContext ctx) {
    Value dateValue = DefaultDateFunctions.numberToDateValue(
        ctx, getNumber().doubleValue());
    if(dateValue == null) {
      throw invalidConversion(Value.Type.DATE_TIME);
    }
    return dateValue;
  }

  protected abstract Number getNumber();
}
