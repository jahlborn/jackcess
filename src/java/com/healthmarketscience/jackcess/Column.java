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
 *
 * @author James Ahlborn
 */
public abstract class Column 
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
  public abstract Table getTable();

  /**
   * @usage _general_method_
   */
  public abstract Database getDatabase();

  /**
   * @usage _general_method_
   */
  public abstract String getName();

  /**
   * @usage _advanced_method_
   */
  public abstract boolean isVariableLength();

  /**
   * @usage _general_method_
   */
  public abstract boolean isAutoNumber();

  /**
   * @usage _advanced_method_
   */
  public abstract int getColumnIndex();

  /**
   * @usage _general_method_
   */
  public abstract DataType getType();

  /**
   * @usage _general_method_
   */
  public abstract int getSQLType() throws SQLException;

  /**
   * @usage _general_method_
   */
  public abstract boolean isCompressedUnicode();

  /**
   * @usage _general_method_
   */
  public abstract byte getPrecision();

  /**
   * @usage _general_method_
   */
  public abstract byte getScale();

  /**
   * @usage _general_method_
   */
  public abstract short getLength();

  /**
   * @usage _general_method_
   */
  public abstract short getLengthInUnits();

  /**
   * Whether or not this column is "append only" (its history is tracked by a
   * separate version history column).
   * @usage _general_method_
   */
  public abstract boolean isAppendOnly();

  /**
   * Returns whether or not this is a hyperlink column (only possible for
   * columns of type MEMO).
   * @usage _general_method_
   */
  public abstract boolean isHyperlink();

  /**
   * Returns extended functionality for "complex" columns.
   * @usage _general_method_
   */
  public abstract ComplexColumnInfo<? extends ComplexValue> getComplexInfo();

  /**
   * @return the properties for this column
   * @usage _general_method_
   */
  public abstract PropertyMap getProperties() throws IOException;

  public abstract Object setRowValue(Object[] rowArray, Object value);
  
  public abstract Object setRowValue(Map<String,Object> rowMap, Object value);
  
  public abstract Object getRowValue(Object[] rowArray);
  
  public abstract Object getRowValue(Map<String,?> rowMap);
}
