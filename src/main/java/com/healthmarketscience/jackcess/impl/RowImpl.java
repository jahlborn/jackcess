/*
Copyright (c) 2013 James Ahlborn

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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Date;
import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.util.OleBlob;


/**
 * A row of data as column-&gt;value pairs.
 * <p>
 * Note that the {@link #equals} and {@link #hashCode} methods work on the row
 * contents <i>only</i> (i.e. they ignore the id).
 *
 * @author James Ahlborn
 */
public class RowImpl extends LinkedHashMap<String,Object> implements Row
{
  private static final long serialVersionUID = 20130314L;

  private final RowIdImpl _id;

  public RowImpl(RowIdImpl id) {
    _id = id;
  }

  public RowImpl(RowIdImpl id, int expectedSize) {
    super(expectedSize);
    _id = id;
  }

  public RowImpl(Row row) {
    super(row);
    _id = (RowIdImpl)row.getId();
  }

  public RowIdImpl getId() {
    return _id;
  }

  public String getString(String name) {
    return (String)get(name);
  }

  public Boolean getBoolean(String name) {
    return (Boolean)get(name);
  }

  public Byte getByte(String name) {
    return (Byte)get(name);
  }

  public Short getShort(String name) {
    return (Short)get(name);
  }

  public Integer getInt(String name) {
    return (Integer)get(name);
  }

  public BigDecimal getBigDecimal(String name) {
    return (BigDecimal)get(name);
  }

  public Float getFloat(String name) {
    return (Float)get(name);
  }

  public Double getDouble(String name) {
    return (Double)get(name);
  }

  @SuppressWarnings("deprecation")
  public Date getDate(String name) {
    return (Date)get(name);
  }

  public LocalDateTime getLocalDateTime(String name) {
    return (LocalDateTime)get(name);
  }

  public byte[] getBytes(String name) {
    return (byte[])get(name);
  }

  public ComplexValueForeignKey getForeignKey(String name) {
    return (ComplexValueForeignKey)get(name);
  }

  public OleBlob getBlob(String name) throws IOException {
    byte[] bytes = getBytes(name);
    return ((bytes != null) ? OleBlob.Builder.fromInternalData(bytes) : null);
  }

  @Override
  public String toString() {
    return CustomToStringStyle.valueBuilder("Row[" + _id + "]")
      .append(null, this)
      .toString();
  }
}
