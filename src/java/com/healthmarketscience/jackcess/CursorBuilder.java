/*
Copyright (c) 2007 Health Market Science, Inc.

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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;

/**
 * Builder style class for constructing a Cursor.  By default, a cursor is
 * created at the beginning of the table, and any start/end rows are
 * inclusive.
 *
 * @author James Ahlborn
 */
public class CursorBuilder {
  /** the table which the cursor will traverse */
  private final Table _table;
  /** optional index to use in traversal */
  private Index _index;
  /** optional start row for an index cursor */
  private Object[] _startRow;
  /** whether or not start row for an index cursor is inclusive */
  private boolean _startRowInclusive = true;
  /** optional end row for an index cursor */
  private Object[] _endRow;
  /** whether or not end row for an index cursor is inclusive */
  private boolean _endRowInclusive = true;
  /** whether to start at beginning or end of cursor */
  private boolean _beforeFirst = true;
  /** optional save point to restore to the cursor */
  private Cursor.Savepoint _savepoint;

  public CursorBuilder(Table table) {
    _table = table;
  }

  /**
   * Sets the cursor so that it will start at the beginning (unless a
   * savepoint is given).
   */
  public CursorBuilder beforeFirst() {
    _beforeFirst = true;
    return this;
  }
  
  /**
   * Sets the cursor so that it will start at the end (unless a savepoint is
   * given).
   */
  public CursorBuilder afterLast() {
    _beforeFirst = false;
    return this;
  }

  /**
   * Sets a savepoint to restore for the initial position of the cursor.
   */
  public CursorBuilder restoreSavepoint(Cursor.Savepoint savepoint) {
    _savepoint = savepoint;
    return this;
  }

  /**
   * Sets an index to use for the cursor.
   */
  public CursorBuilder setIndex(Index index) {
    _index = index;
    return this;
  }

  /**
   * Sets an index to use for the cursor by searching the table for an index
   * with the given name.
   * @throws IllegalArgumentException if no index can be found on the table
   *         with the given name
   */
  public CursorBuilder setIndexByName(String indexName) {
    return setIndex(_table.getIndex(indexName));
  }

  /**
   * Sets an index to use for the cursor by searching the table for an index
   * with exactly the given columns.
   * @throws IllegalArgumentException if no index can be found on the table
   *         with the given name
   */
  public CursorBuilder setIndexByColumns(Column... columns) {
    List<Column> searchColumns = Arrays.asList(columns);
    boolean found = false;
    for(Index index : _table.getIndexes()) {
      
      Collection<Index.ColumnDescriptor> indexColumns = index.getColumns();
      if(indexColumns.size() != searchColumns.size()) {
        continue;
      }
      Iterator<Column> sIter = searchColumns.iterator();
      Iterator<Index.ColumnDescriptor> iIter = indexColumns.iterator();
      boolean matches = true;
      while(sIter.hasNext()) {
        Column sCol = sIter.next();
        Index.ColumnDescriptor iCol = iIter.next();
        if(!ObjectUtils.equals(sCol.getName(), iCol.getName())) {
          matches = false;
          break;
        }
      }

      if(matches) {
        _index = index;
        found = true;
        break;
      }
    }
    if(!found) {
      throw new IllegalArgumentException("Index with columns " +
                                         searchColumns +
                                         " does not exist in table " + _table);
    }
    return this;
  }

  /**
   * Sets the starting and ending row for a range based index cursor.
   * <p>
   * A valid index must be specified before calling this method.
   */
  public CursorBuilder setSpecificRow(Object[] specificRow) {
    setStartRow(specificRow);
    setEndRow(specificRow);
    return this;
  }
  
  /**
   * Sets the starting and ending row for a range based index cursor to the
   * given entry (where the given values correspond to the index's columns).
   * <p>
   * A valid index must be specified before calling this method.
   */
  public CursorBuilder setSpecificEntry(Object... specificEntry) {
    if(specificEntry != null) {
      setSpecificRow(_index.constructIndexRowFromEntry(specificEntry));
    }
    return this;
  }

  
  /**
   * Sets the starting row for a range based index cursor.
   * <p>
   * A valid index must be specified before calling this method.
   */
  public CursorBuilder setStartRow(Object[] startRow) {
    _startRow = startRow;
    return this;
  }
  
  /**
   * Sets the starting row for a range based index cursor to the given entry
   * (where the given values correspond to the index's columns).
   * <p>
   * A valid index must be specified before calling this method.
   */
  public CursorBuilder setStartEntry(Object... startEntry) {
    if(startEntry != null) {
      setStartRow(_index.constructIndexRowFromEntry(startEntry));
    }
    return this;
  }

  /**
   * Sets whether the starting row for a range based index cursor is inclusive
   * or exclusive.
   */
  public CursorBuilder setStartRowInclusive(boolean inclusive) {
    _startRowInclusive = inclusive;
    return this;
  }

  /**
   * Sets the ending row for a range based index cursor.
   * <p>
   * A valid index must be specified before calling this method.
   */
  public CursorBuilder setEndRow(Object[] endRow) {
    _endRow = endRow;
    return this;
  }
  
  /**
   * Sets the ending row for a range based index cursor to the given entry
   * (where the given values correspond to the index's columns).
   * <p>
   * A valid index must be specified before calling this method.
   */
  public CursorBuilder setEndEntry(Object... endEntry) {
    if(endEntry != null) {
      setEndRow(_index.constructIndexRowFromEntry(endEntry));
    }
    return this;
  }

  /**
   * Sets whether the ending row for a range based index cursor is inclusive
   * or exclusive.
   */
  public CursorBuilder setEndRowInclusive(boolean inclusive) {
    _endRowInclusive = inclusive;
    return this;
  }

  /**
   * Returns a new cursor for the table, constructed to the given
   * specifications.
   */
  public Cursor toCursor()
    throws IOException
  {
    Cursor cursor = null;
    if(_index == null) {
      cursor = Cursor.createCursor(_table);
    } else {
      cursor = Cursor.createIndexCursor(_table, _index,
                                        _startRow, _startRowInclusive,
                                        _endRow, _endRowInclusive);
    }
    if(_savepoint == null) {
      if(!_beforeFirst) {
        cursor.afterLast();
      }
    } else {
      cursor.restoreSavepoint(_savepoint);
    }
    return cursor;
  }
  
}
