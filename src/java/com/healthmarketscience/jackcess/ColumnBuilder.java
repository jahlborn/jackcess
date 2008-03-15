// Copyright (c) 2008 Health Market Science, Inc.

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
   * Sets whether of not the new column is an auto-number column.
   */
  public ColumnBuilder setAutoNumber(boolean autoNumber) {
    _autoNumber = autoNumber;
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
    return col;
  }
  
}
