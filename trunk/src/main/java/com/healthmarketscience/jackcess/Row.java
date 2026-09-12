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

package com.healthmarketscience.jackcess;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.util.OleBlob;


/**
 * A row of data as column name-&gt;value pairs.  Values are strongly typed, and
 * column names are case sensitive.
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public interface Row extends Map<String,Object>
{
  /**
   * @return the id of this row
   */
  public RowId getId();

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a String (DataTypes TEXT, MEMO, GUID).
   */
  public String getString(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a Boolean (DataType BOOLEAN).
   */
  public Boolean getBoolean(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a Byte (DataType BYTE).
   */
  public Byte getByte(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a Short (DataType INT).
   */
  public Short getShort(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a Integer (DataType LONG).
   */
  public Integer getInt(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a BigDecimal (DataTypes MONEY, NUMERIC).
   */
  public BigDecimal getBigDecimal(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a Float (DataType FLOAT).
   */
  public Float getFloat(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a Double (DataType DOUBLE).
   */
  public Double getDouble(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a Date (DataType SHORT_DATE_TIME).
   * @deprecated this is only valid for Database instances configured for the
   *             legacy {@link DateTimeType#DATE}.  Prefer using
   *             {@link DateTimeType#LOCAL_DATE_TIME} and the corresponding
   *             {@link #getLocalDateTime} method. Using Date is being phased
   *             out and will eventually be removed.
   */
  @Deprecated
  public Date getDate(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a LocalDateTime (DataType SHORT_DATE_TIME or
   * EXT_DATE_TIME).  This method will only work for Database instances
   * configured for {@link DateTimeType#LOCAL_DATE_TIME}.
   */
  public LocalDateTime getLocalDateTime(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a byte[] (DataTypes BINARY, OLE).
   */
  public byte[] getBytes(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * casting it to a {@link ComplexValueForeignKey} (DataType COMPLEX_TYPE).
   */
  public ComplexValueForeignKey getForeignKey(String name);

  /**
   * Convenience method which gets the value for the row with the given name,
   * converting it to an {@link OleBlob} (DataTypes OLE).
   * <p>
   * Note, <i>the OleBlob should be closed after use</i>.
   */
  public OleBlob getBlob(String name);
}
