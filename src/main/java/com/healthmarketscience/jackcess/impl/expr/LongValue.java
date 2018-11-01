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

import com.healthmarketscience.jackcess.expr.LocaleContext;

/**
 *
 * @author James Ahlborn
 */
public class LongValue extends BaseNumericValue
{
  private final Integer _val;

  public LongValue(Integer val)
  {
    _val = val;
  }

  public Type getType() {
    return Type.LONG;
  }

  public Object get() {
    return _val;
  }

  @Override
  protected Number getNumber() {
    return _val;
  }

  @Override
  public boolean getAsBoolean(LocaleContext ctx) {
    return (_val.longValue() != 0L);
  }

  @Override
  public Integer getAsLongInt(LocaleContext ctx) {
    return _val;
  }

  @Override
  public BigDecimal getAsBigDecimal(LocaleContext ctx) {
    return BigDecimal.valueOf(_val);
  }

  @Override
  public String getAsString(LocaleContext ctx) {
    return _val.toString();
  }
}
