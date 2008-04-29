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
