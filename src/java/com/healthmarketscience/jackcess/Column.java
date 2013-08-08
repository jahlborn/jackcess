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
import java.sql.SQLException;
import java.util.Map;

import com.healthmarketscience.jackcess.complex.ComplexColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexValue;

/**
 * Access database column definition.  A {@link Table} has a list of Column
 * instances describing the table schema.
 * <p>
 * A Column instance is not thread-safe (see {@link Database} for more
 * thread-safety details).
 *
 * @author James Ahlborn
 */
public interface Column 
{
  /**
   * Meaningless placeholder object for inserting values in an autonumber
   * column.  it is not required that this value be used (any passed in value
   * is ignored), but using this placeholder may make code more obvious.
   * @usage _general_field_
   */
  public static final Object AUTO_NUMBER = "<AUTO_NUMBER>";
  
  /**
   * Meaningless placeholder object for updating rows which indicates that a
   * given column should keep its existing value.
   * @usage _general_field_
   */
  public static final Object KEEP_VALUE = "<KEEP_VALUE>";
  
  /**
   * @usage _general_method_
   */
  public Table getTable();

  /**
   * @usage _general_method_
   */
  public Database getDatabase();

  /**
   * @usage _general_method_
   */
  public String getName();

  /**
   * @usage _advanced_method_
   */
  public boolean isVariableLength();

  /**
   * @usage _general_method_
   */
  public boolean isAutoNumber();

  /**
   * @usage _advanced_method_
   */
  public int getColumnIndex();

  /**
   * @usage _general_method_
   */
  public DataType getType();

  /**
   * @usage _general_method_
   */
  public int getSQLType() throws SQLException;

  /**
   * @usage _general_method_
   */
  public boolean isCompressedUnicode();

  /**
   * @usage _general_method_
   */
  public byte getPrecision();

  /**
   * @usage _general_method_
   */
  public byte getScale();

  /**
   * @usage _general_method_
   */
  public short getLength();

  /**
   * @usage _general_method_
   */
  public short getLengthInUnits();

  /**
   * Whether or not this column is "append only" (its history is tracked by a
   * separate version history column).
   * @usage _general_method_
   */
  public boolean isAppendOnly();

  /**
   * Returns whether or not this is a hyperlink column (only possible for
   * columns of type MEMO).
   * @usage _general_method_
   */
  public boolean isHyperlink();

  /**
   * Returns extended functionality for "complex" columns.
   * @usage _general_method_
   */
  public ComplexColumnInfo<? extends ComplexValue> getComplexInfo();

  /**
   * @return the properties for this column
   * @usage _general_method_
   */
  public PropertyMap getProperties() throws IOException;
  
  /**
   * Returns the column which tracks the version history for an "append only"
   * column.
   * @usage _intermediate_method_
   */
  public Column getVersionHistoryColumn();

  public Object setRowValue(Object[] rowArray, Object value);
  
  public Object setRowValue(Map<String,Object> rowMap, Object value);
  
  public Object getRowValue(Object[] rowArray);
  
  public Object getRowValue(Map<String,?> rowMap);
}
