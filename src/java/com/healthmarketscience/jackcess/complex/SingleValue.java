// Copyright (c) 2011 Boomi, Inc.

package com.healthmarketscience.jackcess.complex;

/**
 *
 * @author James Ahlborn
 */
public interface SingleValue extends ComplexValue 
{
  public Object get();

  public void set(Object value);
}
