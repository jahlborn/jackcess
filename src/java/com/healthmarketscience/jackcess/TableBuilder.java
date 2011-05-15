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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builder style class for constructing a Column.
 *
 * @author James Ahlborn
 */
public class TableBuilder {

  /** name of the new table */
  private String _name;
  /** columns for the new table */
  private List<Column> _columns = new ArrayList<Column>();
  /** indexes for the new table */
  private List<IndexBuilder> _indexes = new ArrayList<IndexBuilder>();
  /** whether or not table/column/index names are automatically escaped */
  private boolean _escapeIdentifiers;
  

  public TableBuilder(String name) {
    this(name, false);
  }
  
  public TableBuilder(String name, boolean escapeIdentifiers) {
    _name = name;
    _escapeIdentifiers = escapeIdentifiers;
    if(_escapeIdentifiers) {
      _name = Database.escapeIdentifier(_name);
    }
  }


  /**
   * Adds a Column to the new table.
   */
  public TableBuilder addColumn(Column column) {
    if(_escapeIdentifiers) {
      column.setName(Database.escapeIdentifier(column.getName()));
    }
    _columns.add(column);
    return this;
  }

  /**
   * Adds a Column to the new table.
   */
  public TableBuilder addColumn(ColumnBuilder columnBuilder) {
    return addColumn(columnBuilder.toColumn());
  }

  /**
   * Adds an IndexBuilder to the new table.
   */
  public TableBuilder addIndex(IndexBuilder index) {
    if(_escapeIdentifiers) {
      index.setName(Database.escapeIdentifier(index.getName()));
      for(IndexBuilder.Column col : index.getColumns()) {
        col.setName(Database.escapeIdentifier(col.getName()));
      }
    }
    _indexes.add(index);
    return this;
  }

  /**
   * Sets whether or not subsequently added columns will have their names
   * automatically escaped
   */
  public TableBuilder setEscapeIdentifiers(boolean escapeIdentifiers) {
    _escapeIdentifiers = escapeIdentifiers;
    return this;
  }

  /**
   * Sets the names of the primary key columns for this table.  Convenience
   * method for creating a primary key index on a table.
   */
  public TableBuilder setPrimaryKey(String... colNames) {
    addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
             .addColumns(colNames).setPrimaryKey());
    return this;
  }
  
  /**
   * Escapes the new table's name using {@link Database#escapeIdentifier}.
   */
  public TableBuilder escapeName()
  {
    _name = Database.escapeIdentifier(_name);
    return this;
  }

  /**
   * Creates a new Table in the given Database with the currently configured
   * attributes.
   */
  public Table toTable(Database db)
    throws IOException
  {
    db.createTable(_name, _columns, _indexes);
    return db.getTable(_name);
  }
  
}
