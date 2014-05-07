/*
Copyright (c) 2013 James Ahlborn

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA
*/

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.math.BigDecimal;

import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.util.OleBlob;


/**
 * A row of data as column name->value pairs.  Values are strongly typed, and
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
   */
  public Date getDate(String name);

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
   * </p>
   * Note, <i>the OleBlob should be closed after use</i>.
   */
  public OleBlob getBlob(String name) throws IOException;
}
