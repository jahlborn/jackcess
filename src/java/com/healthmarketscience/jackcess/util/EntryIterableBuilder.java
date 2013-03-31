/*
Copyright (c) 2013 James Ahlborn

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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.impl.IndexCursorImpl;

/**
 * Builder style class for constructing an IndexCursor entry
 * Iterable/Iterator.
 *
 * @author James Ahlborn
 */
public class EntryIterableBuilder implements Iterable<Row>
{
  private final IndexCursor _cursor;

  private Collection<String> _columnNames;
  private Object[] _entryValues;
  private ColumnMatcher _columnMatcher;

  public EntryIterableBuilder(IndexCursor cursor, Object... entryValues) {
    _cursor = cursor;
    _entryValues = entryValues;
  }

  public Collection<String> getColumnNames() {
    return _columnNames;
  }

  public ColumnMatcher getColumnMatcher() {
    return _columnMatcher;
  }

  public Object[] getEntryValues() {
    return _entryValues;
  }

  public EntryIterableBuilder setColumnNames(Collection<String> columnNames) {
    _columnNames = columnNames;
    return this;
  }

  public EntryIterableBuilder addColumnNames(Iterable<String> columnNames) {
    if(columnNames != null) {
      for(String name : columnNames) {
        addColumnName(name);
      }
    }
    return this;
  }

  public EntryIterableBuilder addColumns(Iterable<? extends Column> cols) {
    if(cols != null) {
      for(Column col : cols) {
        addColumnName(col.getName());
      }
    }
    return this;
  }

  public EntryIterableBuilder addColumnNames(String... columnNames) {
    if(columnNames != null) {
      for(String name : columnNames) {
        addColumnName(name);
      }
    }
    return this;
  }

  private void addColumnName(String columnName) {
    if(_columnNames == null) {
      _columnNames = new HashSet<String>();
    }
    _columnNames.add(columnName);
  }

  public EntryIterableBuilder setEntryValues(Object... entryValues) {
    _entryValues = entryValues;
    return this;
  }

  public EntryIterableBuilder setColumnMatcher(ColumnMatcher columnMatcher) {
    _columnMatcher = columnMatcher;
    return this;
  }

  public Iterator<Row> iterator() {
    return ((IndexCursorImpl)_cursor).entryIterator(this);
  }  
}
