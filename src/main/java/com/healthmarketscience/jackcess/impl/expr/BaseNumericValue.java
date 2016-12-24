// Copyright (c) 2016 Dell Boomi, Inc.

package com.healthmarketscience.jackcess.impl.expr;

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
  public String getAsString() {
    return getNumber().toString();
  }

  @Override
  public Long getAsLong() {
    return getNumber().longValue();
  }

  @Override
  public Double getAsDouble() {
    return getNumber().doubleValue();
  }

  protected abstract Number getNumber();
}
