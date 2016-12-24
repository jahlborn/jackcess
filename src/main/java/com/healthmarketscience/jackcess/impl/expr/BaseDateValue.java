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
import java.text.DateFormat;
import java.util.Date;

import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

/**
 *
 * @author James Ahlborn
 */
public abstract class BaseDateValue extends BaseValue
{
  private final Date _val;
  private final DateFormat _fmt;

  public BaseDateValue(Date val, DateFormat fmt) 
  {
    _val = val;
    _fmt = fmt;
  }

  public Object get() {
    return _val;
  }

  protected Double getNumber() {
    return ColumnImpl.toDateDouble(_val, _fmt.getCalendar());
  }

  @Override
  public boolean getAsBoolean() {
    // ms access seems to treat dates/times as "true"
    return true;
  }

  @Override
  public String getAsString() {
    return _fmt.format(_val);
  }

  @Override
  public Long getAsLong() {
    return getNumber().longValue();
  }

  @Override
  public Double getAsDouble() {
    return getNumber();
  }

  @Override
  public BigInteger getAsBigInteger() {
    return getAsBigDecimal().toBigInteger();
  }

  @Override
  public BigDecimal getAsBigDecimal() {
    return BigDecimal.valueOf(getNumber());
  }
}
