// Copyright (c) 2016 Dell Boomi, Inc.

package com.healthmarketscience.jackcess.impl.expr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

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

  public boolean getAsBoolean() {
    return getDelegate().getAsBoolean();
  }

  public String getAsString() {
    return getDelegate().getAsString();
  }

  public Date getAsDateTime() {
    return getDelegate().getAsDateTime();
  }

  public Long getAsLong() {
    return getDelegate().getAsLong();
  }

  public Double getAsDouble() {
    return getDelegate().getAsDouble();
  }

  public BigInteger getAsBigInteger() {
    return getDelegate().getAsBigInteger();
  }

  public BigDecimal getAsBigDecimal() {
    return getDelegate().getAsBigDecimal();
  }

  protected abstract Value eval();
}
