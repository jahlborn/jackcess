/*
Copyright (c) 2008 Health Market Science, Inc.

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.JetFormat;
import com.healthmarketscience.jackcess.impl.PropertyMapImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.impl.TableUpdater;

/**
 * Builder style class for constructing a {@link Column}.  See {@link
 * TableBuilder} for example usage.  Additionally, a Column can be added to an
 * existing Table using the {@link #addToTable(Table)} method.
 *
 * @author James Ahlborn
 * @see TableBuilder
 * @usage _general_class_
 */
public class ColumnBuilder {

  /** name of the new column */
  private String _name;
  /** the type of the new column */
  private DataType _type;
  /** optional length for the new column */
  private Short _length;
  /** optional precision for the new column */
  private Byte _precision;
  /** optional scale for the new column */
  private Byte _scale;
  /** whether or not the column is auto-number */
  private boolean _autoNumber;
  /** whether or not the column allows compressed unicode */
  private boolean _compressedUnicode;
  /** whether or not the column is calculated */
  private boolean _calculated;
  /** whether or not the column is a hyperlink (memo only) */
  private boolean _hyperlink;
  /** 0-based column number */
  private short _columnNumber;
  /** the collating sort order for a text field */
  private ColumnImpl.SortOrder _sortOrder;
  /** table properties (if any) */
  private Map<String,PropertyMap.Property> _props;


  public ColumnBuilder(String name) {
    this(name, null);
  }

  public ColumnBuilder(String name, DataType type) {
    _name = name;
    _type = type;
  }

  public String getName() {
    return _name;
  }

  /**
   * Sets the type for the new column.
   */
  public ColumnBuilder setType(DataType type) {
    _type = type;
    return this;
  }

  public DataType getType() {
    return _type;
  }

  /**
   * Sets the type for the new column based on the given SQL type.
   */
  public ColumnBuilder setSQLType(int type) throws IOException {
    return setSQLType(type, 0, null);
  }

  /**
   * Sets the type for the new column based on the given SQL type and target
   * data length (in type specific units).
   */
  public ColumnBuilder setSQLType(int type, int lengthInUnits)
    throws IOException
  {
    return setSQLType(type, lengthInUnits, null);
  }

  /**
   * Sets the type for the new column based on the given SQL type, target
   * data length (in type specific units), and target FileFormat.
   */
  public ColumnBuilder setSQLType(int type, int lengthInUnits,
                                  Database.FileFormat fileFormat)
    throws IOException
  {
    return setType(DataType.fromSQLType(type, lengthInUnits, fileFormat));
  }

  /**
   * Sets the precision for the new column.
   */
  public ColumnBuilder setPrecision(int newPrecision) {
    _precision = (byte)newPrecision;
    return this;
  }

  public byte getPrecision() {
    return ((_precision != null) ? _precision : (byte)_type.getDefaultPrecision());
  }

  /**
   * Sets the precision for the new column to the max length for the type.
   * Does nothing for types which do not have a precision.
   */
  public ColumnBuilder setMaxPrecision() {
    if(_type.getHasScalePrecision()) {
      setPrecision(_type.getMaxPrecision());
    }
    return this;
  }

  /**
   * Sets the scale for the new column.
   */
  public ColumnBuilder setScale(int newScale) {
    _scale = (byte)newScale;
    return this;
  }

  public byte getScale() {
    return ((_scale != null) ? _scale : (byte)_type.getDefaultScale());
  }

  /**
   * Sets the scale for the new column to the max length for the type.  Does
   * nothing for types which do not have a scale.
   */
  public ColumnBuilder setMaxScale() {
    if(_type.getHasScalePrecision()) {
      setScale(_type.getMaxScale());
    }
    return this;
  }

  /**
   * Sets the length (in bytes) for the new column.
   */
  public ColumnBuilder setLength(int length) {
    _length = (short)length;
    return this;
  }

  public short getLength() {
    return ((_length != null) ? _length :
            (short)(!_type.isVariableLength() ? _type.getFixedSize() :
                    _type.getDefaultSize()));
  }

  /**
   * Sets the length (in type specific units) for the new column.
   */
  public ColumnBuilder setLengthInUnits(int unitLength) {
    return setLength(_type.fromUnitSize(unitLength));
  }

  /**
   * Sets the length for the new column to the max length for the type.  Does
   * nothing for types which are not variable length.
   */
  public ColumnBuilder setMaxLength() {
    // length setting only makes sense for variable length columns
    if(_type.isVariableLength()) {
      setLength(_type.getMaxSize());
    }
    return this;
  }

  /**
   * Sets whether of not the new column is an auto-number column.
   */
  public ColumnBuilder setAutoNumber(boolean autoNumber) {
    _autoNumber = autoNumber;
    return this;
  }

  public boolean isAutoNumber() {
    return _autoNumber;
  }

  /**
   * Sets whether of not the new column allows unicode compression.
   */
  public ColumnBuilder setCompressedUnicode(boolean compressedUnicode) {
    _compressedUnicode = compressedUnicode;
    return this;
  }

  public boolean isCompressedUnicode() {
    return _compressedUnicode;
  }

  /**
   * Sets whether of not the new column is a calculated column.
   */
  public ColumnBuilder setCalculated(boolean calculated) {
    _calculated = calculated;
    return this;
  }

  public boolean isCalculated() {
    return _calculated;
  }

  /**
   * Convenience method to set the various info for a calculated type (flag,
   * result type property and expression)
   */
  public ColumnBuilder setCalculatedInfo(String expression) {
    setCalculated(true);
    putProperty(PropertyMap.EXPRESSION_PROP, expression);
    return putProperty(PropertyMap.RESULT_TYPE_PROP, getType().getValue());
  }

  public boolean isVariableLength() {
    // calculated columns are written as var len
    return(getType().isVariableLength() || isCalculated());
  }

  /**
   * Sets whether of not the new column allows unicode compression.
   */
  public ColumnBuilder setHyperlink(boolean hyperlink) {
    _hyperlink = hyperlink;
    return this;
  }

  public boolean isHyperlink() {
    return _hyperlink;
  }

  /**
   * Sets the column property with the given name to the given value.  Attempts
   * to determine the type of the property (see
   * {@link PropertyMap#put(String,Object)} for details on determining the
   * property type).
   */
  public ColumnBuilder putProperty(String name, Object value) {
    return putProperty(name, null, value);
  }

  /**
   * Sets the column property with the given name and type to the given value.
   */
  public ColumnBuilder putProperty(String name, DataType type, Object value) {
    setProperty(name, PropertyMapImpl.createProperty(name, type, value));
    return this;
  }

  public Map<String,PropertyMap.Property> getProperties() {
    return _props;
  }

  private void setProperty(String name, PropertyMap.Property prop) {
    if(prop == null) {
      return;
    }
    if(_props == null) {
      _props = new HashMap<String,PropertyMap.Property>();
    }
    _props.put(name, prop);
  }

  private PropertyMap.Property getProperty(String name) {
    return ((_props != null) ? _props.get(name) : null);
  }

  /**
   * Sets all attributes except name from the given Column template (including
   * all column properties except GUID).
   */
  public ColumnBuilder setFromColumn(Column template)
    throws IOException
  {
    DataType type = template.getType();
    setType(type);
    setLengthInUnits(template.getLengthInUnits());
    setAutoNumber(template.isAutoNumber());
    if(type.getHasScalePrecision()) {
      setScale(template.getScale());
      setPrecision(template.getPrecision());
    }
    setCalculated(template.isCalculated());
    setCompressedUnicode(template.isCompressedUnicode());
    setHyperlink(template.isHyperlink());
    if(template instanceof ColumnImpl) {
      setTextSortOrder(((ColumnImpl)template).getTextSortOrder());
    }

    PropertyMap colProps = template.getProperties();
    for(PropertyMap.Property colProp : colProps) {
      // copy everything but guid
      if(!PropertyMap.GUID_PROP.equalsIgnoreCase(colProp.getName())) {
        setProperty(colProp.getName(), colProp);
      }
    }

    return this;
  }

  /**
   * Sets all attributes except name from the given Column template.
   */
  public ColumnBuilder setFromColumn(ColumnBuilder template) {
    DataType type = template.getType();
    _type = type;
    _length = template._length;
    _autoNumber = template._autoNumber;
    if(type.getHasScalePrecision()) {
      _scale = template._scale;
      _precision = template._precision;
    }
    _calculated = template._calculated;
    _compressedUnicode = template._compressedUnicode;
    _hyperlink = template._hyperlink;
    _sortOrder = template._sortOrder;

    if(template._props != null) {
      _props = new HashMap<String,PropertyMap.Property>(template._props);
    }

    return this;
  }

  /**
   * Escapes the new column's name using {@link TableBuilder#escapeIdentifier}.
   */
  public ColumnBuilder escapeName() {
    _name = TableBuilder.escapeIdentifier(_name);
    return this;
  }

  /**
   * @usage _advanced_method_
   */
  public short getColumnNumber() {
    return _columnNumber;
  }

  /**
   * @usage _advanced_method_
   */
  public void setColumnNumber(short newColumnNumber) {
    _columnNumber = newColumnNumber;
  }

  /**
   * @usage _advanced_method_
   */
  public ColumnImpl.SortOrder getTextSortOrder() {
    return _sortOrder;
  }

  /**
   * @usage _advanced_method_
   */
  public void setTextSortOrder(ColumnImpl.SortOrder newTextSortOrder) {
    _sortOrder = newTextSortOrder;
  }

  /**
   * Checks that this column definition is valid.
   *
   * @throws IllegalArgumentException if this column definition is invalid.
   * @usage _advanced_method_
   */
  public void validate(JetFormat format) {
    DatabaseImpl.validateIdentifierName(
        getName(), format.MAX_COLUMN_NAME_LENGTH, "column");

    if(getType() == null) {
      throw new IllegalArgumentException(withErrorContext("must have type"));
    }
    if(getType().isUnsupported()) {
      throw new IllegalArgumentException(withErrorContext(
          "Cannot create column with unsupported type " + getType()));
    }
    if(!format.isSupportedDataType(getType())) {
      throw new IllegalArgumentException(withErrorContext(
          "Database format " + format + " does not support type " + getType()));
    }

    if(!getType().isVariableLength()) {
      if(getLength() < getType().getFixedSize()) {
        throw new IllegalArgumentException(withErrorContext(
            "Invalid fixed length size " + getLength()));
      }
    } else if(!getType().isLongValue()) {
      if(!getType().isValidSize(getLength())) {
        throw new IllegalArgumentException(withErrorContext(
            "Var length must be from " + getType().getMinSize() + " to " +
            getType().getMaxSize() + " inclusive, found " + getLength()));
      }
    }

    if(getType().getHasScalePrecision()) {
      if(!getType().isValidScale(getScale())) {
        throw new IllegalArgumentException(withErrorContext(
            "Scale must be from " + getType().getMinScale() + " to " +
            getType().getMaxScale() + " inclusive, found " + getScale()));
      }
      if(!getType().isValidPrecision(getPrecision())) {
        throw new IllegalArgumentException(withErrorContext(
            "Precision must be from " + getType().getMinPrecision() + " to " +
            getType().getMaxPrecision() + " inclusive, found " +
            getPrecision()));
      }
    }

    if(isAutoNumber()) {
      if(!getType().mayBeAutoNumber()) {
        throw new IllegalArgumentException(withErrorContext(
            "Auto number column must be long integer or guid"));
      }
    }

    if(isCompressedUnicode()) {
      if(!getType().isTextual()) {
        throw new IllegalArgumentException(withErrorContext(
            "Only textual columns allow unicode compression (text/memo)"));
      }
    }

    if(isHyperlink()) {
      if(getType() != DataType.MEMO) {
        throw new IllegalArgumentException(withErrorContext(
            "Only memo columns can be hyperlinks"));
      }
    }

    if(isCalculated()) {
      if(!format.isSupportedCalculatedDataType(getType())) {
        throw new IllegalArgumentException(withErrorContext(
            "Database format " + format + " does not support calculated type " +
            getType()));
      }

      // must have an expression
      if(getProperty(PropertyMap.EXPRESSION_PROP) == null) {
        throw new IllegalArgumentException(withErrorContext(
            "No expression provided for calculated type " + getType()));
      }

      // must have result type (just fill in if missing)
      if(getProperty(PropertyMap.RESULT_TYPE_PROP) == null) {
        putProperty(PropertyMap.RESULT_TYPE_PROP, getType().getValue());
      }
    }
  }

  /**
   * Creates a new Column with the currently configured attributes.
   */
  public ColumnBuilder toColumn() {
    // for backwards compat w/ old code
    return this;
  }

  /**
   * Adds a new Column to the given Table with the currently configured
   * attributes.
   */
  public Column addToTable(Table table) throws IOException {
    return addToTableDefinition(table);
  }

  /**
   * Adds a new Column to the given TableDefinition with the currently
   * configured attributes.
   */
  public Column addToTableDefinition(TableDefinition table) throws IOException {
      return new TableUpdater((TableImpl)table).addColumn(this);
  }

  private String withErrorContext(String msg) {
    return msg + "(Column=" + getName() + ")";
  }
}
