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

import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public class DateTimeValue extends BaseValue
{
  private final Type _type;
  private final Date _val;

  public DateTimeValue(Type type, Date val) {
    if(!type.isTemporal()) {
      throw new IllegalArgumentException("invalid date/time type");
    }
    _type = type;
    _val = val;
  }

  public Type getType() {
    return _type;
  }

  public Object get() {
    return _val;
  }

  protected Double getNumber(LocaleContext ctx) {
    return ColumnImpl.toDateDouble(_val, ctx.getCalendar());
  }

  @Override
  public boolean getAsBoolean(LocaleContext ctx) {
    // ms access seems to treat dates/times as "true"
    return true;
  }

  @Override
  public String getAsString(LocaleContext ctx) {
    return ValueSupport.getDateFormatForType(ctx, getType()).format(_val);
  }

  @Override
  public Date getAsDateTime(LocaleContext ctx) {
    return _val;
  }

  @Override
  public Value getAsDateTimeValue(LocaleContext ctx) {
    return this;
  }

  @Override
  public Integer getAsLongInt(LocaleContext ctx) {
    return roundToLongInt(ctx);
  }

  @Override
  public Double getAsDouble(LocaleContext ctx) {
    return getNumber(ctx);
  }

  @Override
  public BigDecimal getAsBigDecimal(LocaleContext ctx) {
    return BigDecimal.valueOf(getNumber(ctx));
  }
}
