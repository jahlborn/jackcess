/*
Copyright (c) 2011 James Ahlborn

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Builder style class for constructing an Index.
 *
 * @author James Ahlborn
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
    return (getType() == Index.PRIMARY_KEY_INDEX_TYPE);
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
   * unique).
   */
  public IndexBuilder setPrimaryKey() {
    _type = Index.PRIMARY_KEY_INDEX_TYPE;
    return setUnique();
  }

  /**
   * Sets this index to enforce uniqueness.
   */
  public IndexBuilder setUnique() {
    _flags |= IndexData.UNIQUE_INDEX_FLAG;
    return this;
  }    

  /**
   * Sets this index to ignore null values.
   */
  public IndexBuilder setIgnoreNulls() {
    _flags |= IndexData.IGNORE_NULLS_INDEX_FLAG;
    return this;
  }    

  public void validate(Set<String> tableColNames) {
    if(getColumns().isEmpty()) {
      throw new IllegalArgumentException("index " + getName() +
                                         " has no columns");
    }
    if(getColumns().size() > IndexData.MAX_COLUMNS) {
      throw new IllegalArgumentException("index " + getName() +
                                         " has too many columns, max " +
                                         IndexData.MAX_COLUMNS);
    }

    Set<String> idxColNames = new HashSet<String>();
    for(Column col : getColumns()) {
      String idxColName = col.getName().toUpperCase();
      if(!idxColNames.add(idxColName)) {
        throw new IllegalArgumentException("duplicate column name " +
                                           col.getName() + " in index " +
                                           getName());
      }
      if(!tableColNames.contains(idxColName)) {
        throw new IllegalArgumentException("column named " + col.getName() +
                                           " not found in table");
      }
    }
  }

  /**
   * Information about a column in this index (name and ordering).
   */
  public static class Column
  {
    /** name of the column to be indexed */
    private String _name;
    /** column flags (ordering) */
    private byte _flags;

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
