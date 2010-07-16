/*
Copyright (c) 2008 Health Market Science, Inc.

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

You can contact Health Market Science at info@healthmarketscience.com
or at the following address:

Health Market Science
2700 Horizon Drive
Suite 200
King of Prussia, PA 19406
*/

package com.healthmarketscience.jackcess;

import java.sql.SQLException;

/**
 * Builder style class for constructing a Column.
 *
 * @author James Ahlborn
 */
public class ColumnBuilder {

  /** name of the new column */
  private String _name;
  /** the type of the new column */
  private DataType _type;
  /** optional length for the new column */
  private Integer _length;
  /** optional precision for the new column */
  private Integer _precision;
  /** optional scale for the new column */
  private Integer _scale;
  /** whether or not the column is auto-number */
  private boolean _autoNumber;
  /** whether or not the column allows compressed unicode */
  private Boolean _compressedUnicode;

  public ColumnBuilder(String name) {
    this(name, null);
  }
  
  public ColumnBuilder(String name, DataType type) {
    _name = name;
    _type = type;
  }

  /**
   * Sets the type for the new column.
   */
  public ColumnBuilder setType(DataType type) {
    _type = type;
    return this;
  }

  /**
   * Sets the type for the new column based on the given SQL type.
   */
  public ColumnBuilder setSQLType(int type) throws SQLException {
    return setSQLType(type, 0);
  }
  
  /**
   * Sets the type for the new column based on the given SQL type and target
   * data length (in type specific units).
   */
  public ColumnBuilder setSQLType(int type, int lengthInUnits)
    throws SQLException
  {
    return setType(DataType.fromSQLType(type, lengthInUnits));
  }

  /**
   * Sets the precision for the new column.
   */
  public ColumnBuilder setPrecision(int newPrecision) {
    _precision = newPrecision;
    return this;
  }

  /**
   * Sets the scale for the new column.
   */
  public ColumnBuilder setScale(int newScale) {
    _scale = newScale;
    return this;
  }

  /**
   * Sets the length (in bytes) for the new column.
   */
  public ColumnBuilder setLength(int length) {
    _length = length;
    return this;
  }

  /**
   * Sets the length (in type specific units) for the new column.
   */
  public ColumnBuilder setLengthInUnits(int unitLength) {
    return setLength(_type.getUnitSize() * unitLength);
  }
  
  /**
   * Sets the length for the new column to the max length for the type.
   */
  public ColumnBuilder setMaxLength() {
    return setLength(_type.getMaxSize());
  }
  
  /**
   * Sets whether of not the new column is an auto-number column.
   */
  public ColumnBuilder setAutoNumber(boolean autoNumber) {
    _autoNumber = autoNumber;
    return this;
  }

  /**
   * Sets whether of not the new column allows unicode compression.
   */
  public ColumnBuilder setCompressedUnicode(boolean compressedUnicode) {
    _compressedUnicode = compressedUnicode;
    return this;
  }

  /**
   * Sets all attributes except name from the given Column template.
   */
  public ColumnBuilder setFromColumn(Column template) {
    DataType type = template.getType();
    setType(type);
    setLength(template.getLength());
    setAutoNumber(template.isAutoNumber());
    if(type.getHasScalePrecision()) {
      setScale(template.getScale());
      setPrecision(template.getPrecision());
    }
    
    return this;
  }

  /**
   * Escapes the new column's name using {@link Database#escapeIdentifier}.
   */
  public ColumnBuilder escapeName()
  {
    _name = Database.escapeIdentifier(_name);
    return this;
  }

  /**
   * Creates a new Column with the currently configured attributes.
   */
  public Column toColumn() {
    Column col = new Column();
    col.setName(_name);
    col.setType(_type);
    if(_length != null) {
      col.setLength(_length.shortValue());
    }
    if(_precision != null) {
      col.setPrecision(_precision.byteValue());
    }
    if(_scale != null) {
      col.setScale(_scale.byteValue());
    }
    if(_autoNumber) {
      col.setAutoNumber(true);
    }
    if(_compressedUnicode != null) {
      col.setCompressedUnicode(_compressedUnicode);
    }
    return col;
  }
  
}
