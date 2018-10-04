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

import java.math.BigDecimal;
import java.util.Date;

import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public abstract class BaseDelayedValue implements Value
{
  private Value _val;

  protected BaseDelayedValue() {
  }

  private Value getDelegate() {
    if(_val == null) {
      _val = eval();
    }
    return _val;
  }

  public boolean isNull() {
    return(getType() == Type.NULL);
  }

  public Value.Type getType() {
    return getDelegate().getType();
  }

  public Object get() {
    return getDelegate().get();
  }

  public boolean getAsBoolean(LocaleContext ctx) {
    return getDelegate().getAsBoolean(ctx);
  }

  public String getAsString(LocaleContext ctx) {
    return getDelegate().getAsString(ctx);
  }

  public Date getAsDateTime(LocaleContext ctx) {
    return getDelegate().getAsDateTime(ctx);
  }

  public Integer getAsLongInt(LocaleContext ctx) {
    return getDelegate().getAsLongInt(ctx);
  }

  public Double getAsDouble(LocaleContext ctx) {
    return getDelegate().getAsDouble(ctx);
  }

  public BigDecimal getAsBigDecimal(LocaleContext ctx) {
    return getDelegate().getAsBigDecimal(ctx);
  }

  protected abstract Value eval();
}
