/*
Copyright (c) 2011 James Ahlborn

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.IndexData;
import com.healthmarketscience.jackcess.impl.IndexImpl;
import com.healthmarketscience.jackcess.impl.JetFormat;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.impl.TableUpdater;

/**
 * Builder style class for constructing an {@link Index}.  See {@link
 * TableBuilder} for example usage.  Additionally, an Index can be added to an
 * existing Table using the {@link #addToTable(Table)} method.
 *
 * @author James Ahlborn
 * @see TableBuilder
 * @usage _general_class_
 */
public class IndexBuilder
{
  /** name typically used by MS Access for the primary key index */
  public static final String PRIMARY_KEY_NAME = "PrimaryKey";

  /** name of the new index */
  private String _name;
  /** the type of the index */
  private byte _type;
  /** additional index flags (UNKNOWN_INDEX_FLAG always seems to be set in
      access 2000+) */
  private byte _flags = IndexData.UNKNOWN_INDEX_FLAG;
  /** the names and orderings of the indexed columns */
  private final List<Column> _columns = new ArrayList<Column>();
  /** 0-based index number */
  private int _indexNumber;

  public IndexBuilder(String name) {
    _name = name;
  }

  public String getName() {
    return _name;
  }

  public byte getType() {
    return _type;
  }

  public byte getFlags() {
    return _flags;
  }

  public boolean isPrimaryKey() {
    return (getType() == IndexImpl.PRIMARY_KEY_INDEX_TYPE);
  }

  public boolean isUnique() {
      return ((getFlags() & IndexData.UNIQUE_INDEX_FLAG) != 0);
  }

  public boolean isIgnoreNulls() {
      return ((getFlags() & IndexData.IGNORE_NULLS_INDEX_FLAG) != 0);
  }

  public List<Column> getColumns() {
    return _columns;
  }

  /**
   * Sets the name of the index.
   */
  public IndexBuilder setName(String name) {
    _name = name;
    return this;
  }

  /**
   * Adds the columns with ASCENDING ordering to the index.
   */
  public IndexBuilder addColumns(String... names) {
    return addColumns(true, names);
  }

  /**
   * Adds the columns with the given ordering to the index.
   */
  public IndexBuilder addColumns(boolean ascending, String... names) {
    if(names != null) {
      for(String name : names) {
        _columns.add(new Column(name, ascending));
      }
    }
    return this;
  }

  /**
   * Sets this index to be a primary key index (additionally sets the index as
   * unique and required).
   */
  public IndexBuilder setPrimaryKey() {
    _type = IndexImpl.PRIMARY_KEY_INDEX_TYPE;
    setRequired();
    return setUnique();
  }

  /**
   * @usage _advanced_method_
   */
  public IndexBuilder setType(byte type) {
    _type = type;
    return this;
  }

  /**
   * Sets this index to enforce uniqueness.
   */
  public IndexBuilder setUnique() {
    _flags |= IndexData.UNIQUE_INDEX_FLAG;
    return this;
  }

  /**
   * Sets this index to enforce required.
   */
  public IndexBuilder setRequired() {
    _flags |= IndexData.REQUIRED_INDEX_FLAG;
    return this;
  }

  /**
   * Sets this index to ignore null values.
   */
  public IndexBuilder setIgnoreNulls() {
    _flags |= IndexData.IGNORE_NULLS_INDEX_FLAG;
    return this;
  }

  /**
   * @usage _advanced_method_
   */
  public int getIndexNumber() {
    return _indexNumber;
  }

  /**
   * @usage _advanced_method_
   */
  public void setIndexNumber(int newIndexNumber) {
    _indexNumber = newIndexNumber;
  }

  /**
   * Checks that this index definition is valid.
   *
   * @throws IllegalArgumentException if this index definition is invalid.
   * @usage _advanced_method_
   */
  public void validate(Set<String> tableColNames, JetFormat format) {

    DatabaseImpl.validateIdentifierName(
        getName(), format.MAX_INDEX_NAME_LENGTH, "index");

    if(getColumns().isEmpty()) {
      throw new IllegalArgumentException(withErrorContext(
          "index has no columns"));
    }
    if(getColumns().size() > IndexData.MAX_COLUMNS) {
      throw new IllegalArgumentException(withErrorContext(
          "index has too many columns, max " + IndexData.MAX_COLUMNS));
    }

    Set<String> idxColNames = new HashSet<String>();
    for(Column col : getColumns()) {
      String idxColName = col.getName().toUpperCase();
      if(!idxColNames.add(idxColName)) {
        throw new IllegalArgumentException(withErrorContext(
            "duplicate column name " + col.getName() + " in index"));
      }
      if(!tableColNames.contains(idxColName)) {
        throw new IllegalArgumentException(withErrorContext(
            "column named " + col.getName() + " not found in table"));
      }
    }
  }

  /**
   * Adds a new Index to the given Table with the currently configured
   * attributes.
   */
  public Index addToTable(Table table) throws IOException {
    return addToTableDefinition(table);
  }

  /**
   * Adds a new Index to the given TableDefinition with the currently
   * configured attributes.
   */
  public Index addToTableDefinition(TableDefinition table) throws IOException {
      return new TableUpdater((TableImpl)table).addIndex(this);
  }

  private String withErrorContext(String msg) {
    return msg + "(Index=" + getName() + ")";
  }

  /**
   * Information about a column in this index (name and ordering).
   */
  public static class Column
  {
    /** name of the column to be indexed */
    private String _name;
    /** column flags (ordering) */
    private final byte _flags;

    private Column(String name, boolean ascending) {
      _name = name;
      _flags = (ascending ? IndexData.ASCENDING_COLUMN_FLAG : 0);
    }

    public String getName() {
      return _name;
    }

    public Column setName(String name) {
      _name = name;
      return this;
    }

    public boolean isAscending() {
      return ((getFlags() & IndexData.ASCENDING_COLUMN_FLAG) != 0);
    }

    public byte getFlags() {
      return _flags;
    }
  }

}
