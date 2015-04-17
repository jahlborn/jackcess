/*
Copyright (c) 2015 James Ahlborn

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
