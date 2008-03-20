// Copyright (c) 2008 Health Market Science, Inc.

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
  
  public TableBuilder(String name) {
    _name = name;
  }

  /**
   * Adds a Column to the new table.
   */
  public TableBuilder addColumn(Column column) {
    _columns.add(column);
    return this;
  }

  /**
   * Creates a new Table in the given Database with the currently configured
   * attributes.
   */
  public Table toTable(Database db)
    throws IOException
  {
    db.createTable(_name, _columns);
    return db.getTable(_name);
  }
  
}
