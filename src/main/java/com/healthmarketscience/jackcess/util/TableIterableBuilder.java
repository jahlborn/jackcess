/*
Copyright (c) 2015 James Ahlborn

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

package com.healthmarketscience.jackcess.util;

import java.util.Iterator;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;

/**
 * Builder style class for constructing a {@link Database} Iterable/Iterator
 * for {@link Table}s.  By default, normal (non-system, non-linked tables) and
 * linked tables are included and system tables are not.
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public class TableIterableBuilder implements Iterable<Table>
{
  private final Database _db;
  private boolean _includeNormalTables = true;
  private boolean _includeSystemTables;
  private boolean _includeLinkedTables = true;
  
  public TableIterableBuilder(Database db) {
    _db = db;
  }

  public boolean isIncludeNormalTables() {
    return _includeNormalTables;
  }
  
  public boolean isIncludeSystemTables() {
    return _includeSystemTables;
  }
  
  public boolean isIncludeLinkedTables() {
    return _includeLinkedTables;
  }
  
  public TableIterableBuilder setIncludeNormalTables(boolean includeNormalTables) {
    _includeNormalTables = includeNormalTables;
    return this;
  }
  
  public TableIterableBuilder setIncludeSystemTables(boolean includeSystemTables) {
    _includeSystemTables = includeSystemTables;
    return this;
  }

  public TableIterableBuilder setIncludeLinkedTables(boolean includeLinkedTables) {
    _includeLinkedTables = includeLinkedTables;
    return this;
  }

  /**
   * Convenience method to set the flags to include only non-linked (local)
   * user tables.
   */
  public TableIterableBuilder withLocalUserTablesOnly() {
    setIncludeNormalTables(true);
    setIncludeSystemTables(false);
    return setIncludeLinkedTables(false);
  }
  
  /**
   * Convenience method to set the flags to include only system tables.
   */
  public TableIterableBuilder withSystemTablesOnly() {
    setIncludeNormalTables(false);
    setIncludeSystemTables(true);
    return setIncludeLinkedTables(false);
  }
  
  public Iterator<Table> iterator() {
    return ((DatabaseImpl)_db).iterator(this);
  }
}
