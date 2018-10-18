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

import java.text.SimpleDateFormat;
import java.util.Date;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

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
  public Integer getAsLongInt() {
    return roundToLongInt();
  }

  @Override
  public Double getAsDouble() {
    return getNumber().doubleValue();
  }

  @Override
  public Date getAsDateTime(EvalContext ctx) {
    double d = getNumber().doubleValue();

    SimpleDateFormat sdf = ctx.createDateFormat(
        ctx.getTemporalConfig().getDefaultDateTimeFormat());
    return new Date(ColumnImpl.fromDateDouble(d, sdf.getCalendar()));
  }

  protected abstract Number getNumber();
}
