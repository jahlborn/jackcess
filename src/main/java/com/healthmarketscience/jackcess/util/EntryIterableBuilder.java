/*
Copyright (c) 2013 James Ahlborn

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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.impl.IndexCursorImpl;

/**
 * Builder style class for constructing an {@link IndexCursor} entry
 * Iterable/Iterator.
 *
 * @author James Ahlborn
 * @usage _general_class_
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
