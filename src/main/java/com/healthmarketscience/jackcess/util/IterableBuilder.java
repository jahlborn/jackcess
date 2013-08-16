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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.impl.CursorImpl;

/**
 * Builder style class for constructing a {@link Cursor} Iterable/Iterator.
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public class IterableBuilder implements Iterable<Row>
{
  public enum Type {
    SIMPLE, COLUMN_MATCH, ROW_MATCH;
  }

  private final Cursor _cursor;
  private Type _type = Type.SIMPLE;
  private boolean _forward = true;
  private boolean _reset = true;
  private Collection<String> _columnNames;
  private ColumnMatcher _columnMatcher;
  private Object _matchPattern;

  public IterableBuilder(Cursor cursor) {
    _cursor = cursor;
  }

  public Collection<String> getColumnNames() {
    return _columnNames;
  }

  public ColumnMatcher getColumnMatcher() {
    return _columnMatcher;
  }

  public boolean isForward() {
    return _forward;
  }

  public boolean isReset() {
    return _reset;
  }

  /**
   * @usage _advanced_method_
   */
  public Object getMatchPattern() {
    return _matchPattern;
  }

  /**
   * @usage _advanced_method_
   */
  public Type getType() {
    return _type;
  }

  public IterableBuilder forward() {
    return setForward(true);
  }

  public IterableBuilder reverse() {
    return setForward(false);
  }

  public IterableBuilder setForward(boolean forward) {
    _forward = forward;
    return this;
  }

  public IterableBuilder reset(boolean reset) {
    _reset = reset;
    return this;
  }

  public IterableBuilder setColumnNames(Collection<String> columnNames) {
    _columnNames = columnNames;
    return this;
  }

  public IterableBuilder addColumnNames(Iterable<String> columnNames) {
    if(columnNames != null) {
      for(String name : columnNames) {
        addColumnName(name);
      }
    }
    return this;
  }

  public IterableBuilder addColumns(Iterable<? extends Column> cols) {
    if(cols != null) {
      for(Column col : cols) {
        addColumnName(col.getName());
      }
    }
    return this;
  }

  public IterableBuilder addColumnNames(String... columnNames) {
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

  public IterableBuilder setMatchPattern(Column columnPattern, 
                                         Object valuePattern) {
    _type = Type.COLUMN_MATCH;
    _matchPattern = new AbstractMap.SimpleImmutableEntry<Column,Object>(
        columnPattern, valuePattern);
    return this;
  }

  public IterableBuilder setMatchPattern(String columnNamePattern, 
                                         Object valuePattern) {
    return setMatchPattern(_cursor.getTable().getColumn(columnNamePattern),
                           valuePattern);
  }

  public IterableBuilder setMatchPattern(Map<String,?> rowPattern) {
    _type = Type.ROW_MATCH;
    _matchPattern = rowPattern;
    return this;
  }

  public IterableBuilder addMatchPattern(String columnNamePattern, 
                                         Object valuePattern)
  {
    _type = Type.ROW_MATCH;
    @SuppressWarnings("unchecked")
    Map<String,Object> matchPattern = ((Map<String,Object>)_matchPattern);
    if(matchPattern == null) {
      matchPattern = new HashMap<String,Object>();
      _matchPattern = matchPattern;
    }
    matchPattern.put(columnNamePattern, valuePattern);
    return this;
  }    

  public IterableBuilder setColumnMatcher(ColumnMatcher columnMatcher) {
    _columnMatcher = columnMatcher;
    return this;
  }

  public Iterator<Row> iterator() {
    return ((CursorImpl)_cursor).iterator(this);
  }
}
