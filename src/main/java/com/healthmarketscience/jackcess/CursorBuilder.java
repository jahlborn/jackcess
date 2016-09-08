/*
Copyright (c) 2007 Health Market Science, Inc.

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

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.impl.IndexImpl;
import com.healthmarketscience.jackcess.impl.CursorImpl;
import com.healthmarketscience.jackcess.impl.IndexCursorImpl;
import com.healthmarketscience.jackcess.util.ColumnMatcher;


/**
 * Builder style class for constructing a {@link Cursor}.  By default, a
 * cursor is created at the beginning of the table, and any start/end rows are
 * inclusive.
 * <p/>
 * Simple example traversal:
 * <pre>
 *   for(Row row : table.newCursor().toCursor()) {
 *     // ... process each row ...
 *   }
 * </pre>
 * <p/>
 * Simple example search:
 * <pre>
 *   Row row = CursorBuilder.findRow(table, Collections.singletonMap(col, "foo"));
 * </pre>
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public class CursorBuilder {
  /** the table which the cursor will traverse */
  private final TableImpl _table;
  /** optional index to use in traversal */
  private IndexImpl _index;
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
  /** ColumnMatcher to be used when matching column values */
  private ColumnMatcher _columnMatcher;

  public CursorBuilder(Table table) {
    _table = (TableImpl)table;
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
    _index = (IndexImpl)index;
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
   *         with the given columns
   */
  public CursorBuilder setIndexByColumnNames(String... columnNames) {
    return setIndexByColumns(Arrays.asList(columnNames));
  }

  /**
   * Sets an index to use for the cursor by searching the table for an index
   * with exactly the given columns.
   * @throws IllegalArgumentException if no index can be found on the table
   *         with the given columns
   */
  public CursorBuilder setIndexByColumns(Column... columns) {
    List<String> colNames = new ArrayList<String>();
    for(Column col : columns) {
      colNames.add(col.getName());
    }
    return setIndexByColumns(colNames);
  }

  /**
   * Searches for an index with the given column names.
   */
  private CursorBuilder setIndexByColumns(List<String> searchColumns) {
    IndexImpl index = _table.findIndexForColumns(searchColumns, false);
    if(index == null) {
      throw new IllegalArgumentException("Index with columns " +
                                         searchColumns +
                                         " does not exist in table " + _table);
    }
    _index = index;
    return this;
  }

  /**
   * Sets the starting and ending row for a range based index cursor.
   * <p>
   * A valid index must be specified before calling this method.
   */
  public CursorBuilder setSpecificRow(Object... specificRow) {
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
  public CursorBuilder setStartRow(Object... startRow) {
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
  public CursorBuilder setEndRow(Object... endRow) {
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
   * Sets the ColumnMatcher to use for matching row patterns.
   */
  public CursorBuilder setColumnMatcher(ColumnMatcher columnMatcher) {
    _columnMatcher = columnMatcher;
    return this;
  }

  /**
   * Returns a new cursor for the table, constructed to the given
   * specifications.
   */
  public Cursor toCursor() throws IOException
  {
    CursorImpl cursor = null;
    if(_index == null) {
      cursor = CursorImpl.createCursor(_table);
    } else {
      cursor = IndexCursorImpl.createCursor(_table, _index,
                                            _startRow, _startRowInclusive,
                                            _endRow, _endRowInclusive);
    }
    cursor.setColumnMatcher(_columnMatcher);
    if(_savepoint == null) {
      if(!_beforeFirst) {
        cursor.afterLast();
      }
    } else {
      cursor.restoreSavepoint(_savepoint);
    }
    return cursor;
  }
  
  /**
   * Returns a new index cursor for the table, constructed to the given
   * specifications.
   */
  public IndexCursor toIndexCursor() throws IOException
  {
    return (IndexCursorImpl)toCursor();
  }

  /**
   * Creates a normal, un-indexed cursor for the given table.
   * @param table the table over which this cursor will traverse
   */
  public static Cursor createCursor(Table table) throws IOException {
    return table.newCursor().toCursor();
  }

  /**
   * Creates an indexed cursor for the given table.
   * <p>
   * Note, index based table traversal may not include all rows, as certain
   * types of indexes do not include all entries (namely, some indexes ignore
   * null entries, see {@link Index#shouldIgnoreNulls}).
   * 
   * @param index index for the table which will define traversal order as
   *              well as enhance certain lookups
   */
  public static IndexCursor createCursor(Index index)
    throws IOException
  {
    return index.getTable().newCursor().setIndex(index).toIndexCursor();
  }

  /**
   * Creates an indexed cursor for the primary key cursor of the given table.
   * @param table the table over which this cursor will traverse
   */
  public static IndexCursor createPrimaryKeyCursor(Table table)
    throws IOException
  {
    return createCursor(table.getPrimaryKeyIndex());
  }
  
  /**
   * Creates an indexed cursor for the given table, narrowed to the given
   * range.
   * <p>
   * Note, index based table traversal may not include all rows, as certain
   * types of indexes do not include all entries (namely, some indexes ignore
   * null entries, see {@link Index#shouldIgnoreNulls}).
   * 
   * @param index index for the table which will define traversal order as
   *              well as enhance certain lookups
   * @param startRow the first row of data for the cursor (inclusive), or
   *                 {@code null} for the first entry
   * @param endRow the last row of data for the cursor (inclusive), or
   *               {@code null} for the last entry
   */
  public static IndexCursor createCursor(Index index,
                                         Object[] startRow, Object[] endRow)
    throws IOException
  {
    return index.getTable().newCursor().setIndex(index)
      .setStartRow(startRow)
      .setEndRow(endRow)
      .toIndexCursor();
  }
  
  /**
   * Creates an indexed cursor for the given table, narrowed to the given
   * range.
   * <p>
   * Note, index based table traversal may not include all rows, as certain
   * types of indexes do not include all entries (namely, some indexes ignore
   * null entries, see {@link Index#shouldIgnoreNulls}).
   * 
   * @param index index for the table which will define traversal order as
   *              well as enhance certain lookups
   * @param startRow the first row of data for the cursor, or {@code null} for
   *                 the first entry
   * @param startInclusive whether or not startRow is inclusive or exclusive
   * @param endRow the last row of data for the cursor, or {@code null} for
   *               the last entry
   * @param endInclusive whether or not endRow is inclusive or exclusive
   */
  public static IndexCursor createCursor(Index index,
                                         Object[] startRow,
                                         boolean startInclusive,
                                         Object[] endRow,
                                         boolean endInclusive)
    throws IOException
  {
    return index.getTable().newCursor().setIndex(index)
      .setStartRow(startRow)
      .setStartRowInclusive(startInclusive)
      .setEndRow(endRow)
      .setEndRowInclusive(endInclusive)
      .toIndexCursor();
  }

  /**
   * Convenience method for finding a specific row in a table which matches a
   * given row "pattern".  See {@link Cursor#findFirstRow(Map)} for details on
   * the rowPattern.
   * <p>
   * Warning, this method <i>always</i> starts searching from the beginning of
   * the Table (you cannot use it to find successive matches).
   * 
   * @param table the table to search
   * @param rowPattern pattern to be used to find the row
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Row findRow(Table table, Map<String,?> rowPattern)
    throws IOException
  {
    Cursor cursor = createCursor(table);
    if(cursor.findFirstRow(rowPattern)) {
      return cursor.getCurrentRow();
    }
    return null;
  }
  
  /**
   * Convenience method for finding a specific row (as defined by the cursor)
   * where the index entries match the given values.  See {@link
   * IndexCursor#findRowByEntry(Object...)} for details on the entryValues.
   * 
   * @param index the index to search
   * @param entryValues the column values for the index's columns.
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Row findRowByEntry(Index index, Object... entryValues)
    throws IOException
  {
    return createCursor(index).findRowByEntry(entryValues);
  }
  
  /**
   * Convenience method for finding a specific row by the primary key of the
   * table.  See {@link IndexCursor#findRowByEntry(Object...)} for details on
   * the entryValues.
   * 
   * @param table the table to search
   * @param entryValues the column values for the table's primary key columns.
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Row findRowByPrimaryKey(Table table, Object... entryValues)
    throws IOException
  {
    return findRowByEntry(table.getPrimaryKeyIndex(), entryValues);
  }
  
  /**
   * Convenience method for finding a specific row in a table which matches a
   * given row "pattern".  See {@link Cursor#findFirstRow(Column,Object)} for
   * details on the pattern.
   * <p>
   * Note, a {@code null} result value is ambiguous in that it could imply no
   * match or a matching row with {@code null} for the desired value.  If
   * distinguishing this situation is important, you will need to use a Cursor
   * directly instead of this convenience method.
   * 
   * @param table the table to search
   * @param column column whose value should be returned
   * @param columnPattern column being matched by the valuePattern
   * @param valuePattern value from the columnPattern which will match the
   *                     desired row
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Object findValue(Table table, Column column,
                                 Column columnPattern, Object valuePattern)
    throws IOException
  {
    Cursor cursor = createCursor(table);
    if(cursor.findFirstRow(columnPattern, valuePattern)) {
      return cursor.getCurrentRowValue(column);
    }
    return null;
  }
  
  /**
   * Convenience method for finding a specific row in an indexed table which
   * matches a given row "pattern".  See {@link Cursor#findFirstRow(Map)} for
   * details on the rowPattern.
   * <p>
   * Warning, this method <i>always</i> starts searching from the beginning of
   * the Table (you cannot use it to find successive matches).
   * 
   * @param index index to assist the search
   * @param rowPattern pattern to be used to find the row
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Row findRow(Index index, Map<String,?> rowPattern)
    throws IOException
  {
    Cursor cursor = createCursor(index);
    if(cursor.findFirstRow(rowPattern)) {
      return cursor.getCurrentRow();
    }
    return null;
  }
  
  /**
   * Convenience method for finding a specific row in a table which matches a
   * given row "pattern".  See {@link Cursor#findFirstRow(Column,Object)} for
   * details on the pattern.
   * <p>
   * Note, a {@code null} result value is ambiguous in that it could imply no
   * match or a matching row with {@code null} for the desired value.  If
   * distinguishing this situation is important, you will need to use a Cursor
   * directly instead of this convenience method.
   * 
   * @param index index to assist the search
   * @param column column whose value should be returned
   * @param columnPattern column being matched by the valuePattern
   * @param valuePattern value from the columnPattern which will match the
   *                     desired row
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Object findValue(Index index, Column column,
                                 Column columnPattern, Object valuePattern)
    throws IOException
  {
    Cursor cursor = createCursor(index);
    if(cursor.findFirstRow(columnPattern, valuePattern)) {
      return cursor.getCurrentRowValue(column);
    }
    return null;
  }
}
