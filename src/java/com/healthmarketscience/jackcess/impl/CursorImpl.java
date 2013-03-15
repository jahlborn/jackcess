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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.healthmarketscience.jackcess.impl.TableImpl.RowState;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.util.ErrorHandler;
import com.healthmarketscience.jackcess.util.ColumnMatcher;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.RuntimeIOException;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.util.SimpleColumnMatcher;

/**
 * Manages iteration for a Table.  Different cursors provide different methods
 * of traversing a table.  Cursors should be fairly robust in the face of
 * table modification during traversal (although depending on how the table is
 * traversed, row updates may or may not be seen).  Multiple cursors may
 * traverse the same table simultaneously.
 * <p>
 * The Cursor provides a variety of static utility methods to construct
 * cursors with given characteristics or easily search for specific values.
 * For even friendlier and more flexible construction, see
 * {@link CursorBuilder}.
 * <p>
 * Is not thread-safe.
 *
 * @author James Ahlborn
 */
public abstract class CursorImpl implements Cursor
{  
  private static final Log LOG = LogFactory.getLog(CursorImpl.class);  

  /** boolean value indicating forward movement */
  public static final boolean MOVE_FORWARD = true;
  /** boolean value indicating reverse movement */
  public static final boolean MOVE_REVERSE = false;
  
  /** first position for the TableScanCursor */
  private static final ScanPosition FIRST_SCAN_POSITION =
    new ScanPosition(RowIdImpl.FIRST_ROW_ID);
  /** last position for the TableScanCursor */
  private static final ScanPosition LAST_SCAN_POSITION =
    new ScanPosition(RowIdImpl.LAST_ROW_ID);

  /** identifier for this cursor */
  private final IdImpl _id;
  /** owning table */
  private final TableImpl _table;
  /** State used for reading the table rows */
  private final RowState _rowState;
  /** the first (exclusive) row id for this cursor */
  private final PositionImpl _firstPos;
  /** the last (exclusive) row id for this cursor */
  private final PositionImpl _lastPos;
  /** the previous row */
  protected PositionImpl _prevPos;
  /** the current row */
  protected PositionImpl _curPos;
  /** ColumnMatcher to be used when matching column values */
  protected ColumnMatcher _columnMatcher = SimpleColumnMatcher.INSTANCE;

  protected CursorImpl(IdImpl id, TableImpl table, PositionImpl firstPos,
                       PositionImpl lastPos) {
    _id = id;
    _table = table;
    _rowState = _table.createRowState();
    _firstPos = firstPos;
    _lastPos = lastPos;
    _curPos = firstPos;
    _prevPos = firstPos;
  }

  /**
   * Creates a normal, un-indexed cursor for the given table.
   * @param table the table over which this cursor will traverse
   */
  public static CursorImpl createCursor(TableImpl table) {
    return new TableScanCursor(table);
  }

  /**
   * Creates an indexed cursor for the given table.
   * <p>
   * Note, index based table traversal may not include all rows, as certain
   * types of indexes do not include all entries (namely, some indexes ignore
   * null entries, see {@link Index#shouldIgnoreNulls}).
   * 
   * @param table the table over which this cursor will traverse
   * @param index index for the table which will define traversal order as
   *              well as enhance certain lookups
   */
  public static CursorImpl createIndexCursor(TableImpl table, IndexImpl index)
    throws IOException
  {
    return IndexCursorImpl.createCursor(table, index);
  }
  
  /**
   * Creates an indexed cursor for the given table, narrowed to the given
   * range.
   * <p>
   * Note, index based table traversal may not include all rows, as certain
   * types of indexes do not include all entries (namely, some indexes ignore
   * null entries, see {@link Index#shouldIgnoreNulls}).
   * 
   * @param table the table over which this cursor will traverse
   * @param index index for the table which will define traversal order as
   *              well as enhance certain lookups
   * @param startRow the first row of data for the cursor (inclusive), or
   *                 {@code null} for the first entry
   * @param endRow the last row of data for the cursor (inclusive), or
   *               {@code null} for the last entry
   */
  public static CursorImpl createIndexCursor(TableImpl table, IndexImpl index,
                                         Object[] startRow, Object[] endRow)
    throws IOException
  {
    return IndexCursorImpl.createCursor(table, index, startRow, endRow);
  }
  
  /**
   * Creates an indexed cursor for the given table, narrowed to the given
   * range.
   * <p>
   * Note, index based table traversal may not include all rows, as certain
   * types of indexes do not include all entries (namely, some indexes ignore
   * null entries, see {@link Index#shouldIgnoreNulls}).
   * 
   * @param table the table over which this cursor will traverse
   * @param index index for the table which will define traversal order as
   *              well as enhance certain lookups
   * @param startRow the first row of data for the cursor, or {@code null} for
   *                 the first entry
   * @param startInclusive whether or not startRow is inclusive or exclusive
   * @param endRow the last row of data for the cursor, or {@code null} for
   *               the last entry
   * @param endInclusive whether or not endRow is inclusive or exclusive
   */
  public static CursorImpl createIndexCursor(TableImpl table, IndexImpl index,
                                         Object[] startRow,
                                         boolean startInclusive,
                                         Object[] endRow,
                                         boolean endInclusive)
    throws IOException
  {
    return IndexCursorImpl.createCursor(table, index, startRow, startInclusive,
                                    endRow, endInclusive);
  }

  /**
   * Convenience method for finding a specific row in a table which matches a
   * given row "pattern".  See {@link #findFirstRow(Map)} for details on the
   * rowPattern.
   * <p>
   * Warning, this method <i>always</i> starts searching from the beginning of
   * the Table (you cannot use it to find successive matches).
   * 
   * @param table the table to search
   * @param rowPattern pattern to be used to find the row
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Row findRow(TableImpl table,
                                           Map<String,?> rowPattern)
    throws IOException
  {
    CursorImpl cursor = createCursor(table);
    if(cursor.findFirstRow(rowPattern)) {
      return cursor.getCurrentRow();
    }
    return null;
  }
  
  /**
   * Convenience method for finding a specific row in a table which matches a
   * given row "pattern".  See {@link #findFirstRow(Column,Object)} for
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
  public static Object findValue(TableImpl table, ColumnImpl column,
                                 ColumnImpl columnPattern, Object valuePattern)
    throws IOException
  {
    CursorImpl cursor = createCursor(table);
    if(cursor.findFirstRow(columnPattern, valuePattern)) {
      return cursor.getCurrentRowValue(column);
    }
    return null;
  }
  
  /**
   * Convenience method for finding a specific row in an indexed table which
   * matches a given row "pattern".  See {@link #findFirstRow(Map)} for
   * details on the rowPattern.
   * <p>
   * Warning, this method <i>always</i> starts searching from the beginning of
   * the Table (you cannot use it to find successive matches).
   * 
   * @param table the table to search
   * @param index index to assist the search
   * @param rowPattern pattern to be used to find the row
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Row findRow(TableImpl table, IndexImpl index,
                                           Map<String,?> rowPattern)
    throws IOException
  {
    CursorImpl cursor = createIndexCursor(table, index);
    if(cursor.findFirstRow(rowPattern)) {
      return cursor.getCurrentRow();
    }
    return null;
  }
  
  /**
   * Convenience method for finding a specific row in a table which matches a
   * given row "pattern".  See {@link #findFirstRow(Column,Object)} for
   * details on the pattern.
   * <p>
   * Note, a {@code null} result value is ambiguous in that it could imply no
   * match or a matching row with {@code null} for the desired value.  If
   * distinguishing this situation is important, you will need to use a Cursor
   * directly instead of this convenience method.
   * 
   * @param table the table to search
   * @param index index to assist the search
   * @param column column whose value should be returned
   * @param columnPattern column being matched by the valuePattern
   * @param valuePattern value from the columnPattern which will match the
   *                     desired row
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Object findValue(TableImpl table, IndexImpl index, ColumnImpl column,
                                 ColumnImpl columnPattern, Object valuePattern)
    throws IOException
  {
    CursorImpl cursor = createIndexCursor(table, index);
    if(cursor.findFirstRow(columnPattern, valuePattern)) {
      return cursor.getCurrentRowValue(column);
    }
    return null;
  }

  public RowState getRowState() {
    return _rowState;
  }
  
  public IdImpl getId() {
    return _id;
  }

  public TableImpl getTable() {
    return _table;
  }

  public JetFormat getFormat() {
    return getTable().getFormat();
  }

  public PageChannel getPageChannel() {
    return getTable().getPageChannel();
  }

  public ErrorHandler getErrorHandler() {
    return _rowState.getErrorHandler();
  }

  public void setErrorHandler(ErrorHandler newErrorHandler) {
    _rowState.setErrorHandler(newErrorHandler);
  }    

  public ColumnMatcher getColumnMatcher() {
    return _columnMatcher;
  }

  public void setColumnMatcher(ColumnMatcher columnMatcher) {
    if(columnMatcher == null) {
      columnMatcher = getDefaultColumnMatcher();
    }
    _columnMatcher = columnMatcher;
  }

  /**
   * Returns the default ColumnMatcher for this Cursor.
   */
  protected ColumnMatcher getDefaultColumnMatcher() {
    return SimpleColumnMatcher.INSTANCE;
  }

  public SavepointImpl getSavepoint() {
    return new SavepointImpl(_id, _curPos, _prevPos);
  }

  public void restoreSavepoint(Savepoint savepoint)
    throws IOException
  {
    restoreSavepoint((SavepointImpl)savepoint);
  }

  public void restoreSavepoint(SavepointImpl savepoint)
    throws IOException
  {
    if(!_id.equals(savepoint.getCursorId())) {
      throw new IllegalArgumentException(
          "Savepoint " + savepoint + " is not valid for this cursor with id "
          + _id);
    }
    restorePosition(savepoint.getCurrentPosition(),
                    savepoint.getPreviousPosition());
  }
  
  /**
   * Returns the first row id (exclusive) as defined by this cursor.
   */
  protected PositionImpl getFirstPosition() {
    return _firstPos;
  }
  
  /**
   * Returns the last row id (exclusive) as defined by this cursor.
   */
  protected PositionImpl getLastPosition() {
    return _lastPos;
  }

  public void reset() {
    beforeFirst();
  }  

  public void beforeFirst() {
    reset(MOVE_FORWARD);
  }
  
  public void afterLast() {
    reset(MOVE_REVERSE);
  }

  public boolean isBeforeFirst() throws IOException
  {
    if(getFirstPosition().equals(_curPos)) {
      return !recheckPosition(MOVE_REVERSE);
    }
    return false;
  }
  
  public boolean isAfterLast() throws IOException
  {
    if(getLastPosition().equals(_curPos)) {
      return !recheckPosition(MOVE_FORWARD);
    }
    return false;
  }

  public boolean isCurrentRowDeleted() throws IOException
  {
    // we need to ensure that the "deleted" flag has been read for this row
    // (or re-read if the table has been recently modified)
    TableImpl.positionAtRowData(_rowState, _curPos.getRowId());
    return _rowState.isDeleted();
  }
  
  /**
   * Resets this cursor for traversing the given direction.
   */
  protected void reset(boolean moveForward) {
    _curPos = getDirHandler(moveForward).getBeginningPosition();
    _prevPos = _curPos;
    _rowState.reset();
  }  

  public Iterable<Row> reverseIterable() {
    return reverseIterable(null);
  }
  
  public Iterable<Row> reverseIterable(
      final Collection<String> columnNames)
  {
    return new Iterable<Row>() {
      public Iterator<Row> iterator() {
        return new RowIterator(columnNames, MOVE_REVERSE);
      }
    };
  }
  
  public Iterator<Row> iterator()
  {
    return iterator(null);
  }
  
  public Iterable<Row> iterable(
      final Collection<String> columnNames)
  {
    return new Iterable<Row>() {
      public Iterator<Row> iterator() {
        return CursorImpl.this.iterator(columnNames);
      }
    };
  }
  
  /**
   * Calls <code>beforeFirst</code> on this table and returns a modifiable
   * Iterator which will iterate through all the rows of this table, returning
   * only the given columns.  Use of the Iterator follows the same
   * restrictions as a call to <code>getNextRow</code>.
   * @throws RuntimeIOException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Row> iterator(Collection<String> columnNames)
  {
    return new RowIterator(columnNames, MOVE_FORWARD);
  }

  public Iterable<Row> columnMatchIterable(
      Column columnPattern, Object valuePattern)
  {
    return columnMatchIterable((ColumnImpl)columnPattern, valuePattern);
  }

  public Iterable<Row> columnMatchIterable(
      ColumnImpl columnPattern, Object valuePattern)
  {
    return columnMatchIterable(null, columnPattern, valuePattern);
  }

  /**
   * Calls <code>beforeFirst</code> on this cursor and returns a modifiable
   * Iterator which will iterate through all the rows of this table which
   * match the given column pattern.  Use of the Iterator follows the same
   * restrictions as a call to <code>getNextRow</code>.  See
   * {@link #findFirstRow(Column,Object)} for details on the columnPattern.
   * @throws RuntimeIOException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Row> columnMatchIterator(
      Column columnPattern, Object valuePattern)
  {
    return columnMatchIterator((ColumnImpl)columnPattern, valuePattern);
  } 
 
  public Iterator<Row> columnMatchIterator(
      ColumnImpl columnPattern, Object valuePattern)
  {
    return columnMatchIterator(null, columnPattern, valuePattern);
  }

  public Iterable<Row> columnMatchIterable(
      Collection<String> columnNames,
      Column columnPattern, Object valuePattern)
  {
    return columnMatchIterable(columnNames, (ColumnImpl)columnPattern, 
                               valuePattern);
  } 
 
  public Iterable<Row> columnMatchIterable(
      final Collection<String> columnNames,
      final ColumnImpl columnPattern, final Object valuePattern)
  {
    return new Iterable<Row>() {
      public Iterator<Row> iterator() {
        return CursorImpl.this.columnMatchIterator(
            columnNames, columnPattern, valuePattern);
      }
    };
  }

  /**
   * Calls <code>beforeFirst</code> on this table and returns a modifiable
   * Iterator which will iterate through all the rows of this table which
   * match the given column pattern, returning only the given columns.  Use of
   * the Iterator follows the same restrictions as a call to
   * <code>getNextRow</code>.  See {@link #findFirstRow(Column,Object)} for
   * details on the columnPattern.
   * @throws RuntimeIOException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Row> columnMatchIterator(
      Collection<String> columnNames, Column columnPattern, 
      Object valuePattern)
  {
    return columnMatchIterator(columnNames, (ColumnImpl)columnPattern, 
                               valuePattern);
  } 
 
  public Iterator<Row> columnMatchIterator(
      Collection<String> columnNames, ColumnImpl columnPattern, 
      Object valuePattern)
  {
    return new ColumnMatchIterator(columnNames, columnPattern, valuePattern);
  }

  public Iterable<Row> rowMatchIterable(
      Map<String,?> rowPattern)
  {
    return rowMatchIterable(null, rowPattern);
  }
  
  /**
   * Calls <code>beforeFirst</code> on this cursor and returns a modifiable
   * Iterator which will iterate through all the rows of this table which
   * match the given row pattern.  Use of the Iterator follows the same
   * restrictions as a call to <code>getNextRow</code>.  See
   * {@link #findFirstRow(Map)} for details on the rowPattern.
   * @throws RuntimeIOException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Row> rowMatchIterator(
      Map<String,?> rowPattern)
  {
    return rowMatchIterator(null, rowPattern);
  }
  
  public Iterable<Row> rowMatchIterable(
      final Collection<String> columnNames,
      final Map<String,?> rowPattern)
  {
    return new Iterable<Row>() {
      public Iterator<Row> iterator() {
        return CursorImpl.this.rowMatchIterator(
            columnNames, rowPattern);
      }
    };
  }
  
  /**
   * Calls <code>beforeFirst</code> on this table and returns a modifiable
   * Iterator which will iterate through all the rows of this table which
   * match the given row pattern, returning only the given columns.  Use of
   * the Iterator follows the same restrictions as a call to
   * <code>getNextRow</code>.  See {@link #findFirstRow(Map)} for details on
   * the rowPattern.
   * @throws RuntimeIOException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Row> rowMatchIterator(
      Collection<String> columnNames, Map<String,?> rowPattern)
  {
    return new RowMatchIterator(columnNames, rowPattern);
  }

  public void deleteCurrentRow() throws IOException {
    _table.deleteRow(_rowState, _curPos.getRowId());
  }

  public void updateCurrentRow(Object... row) throws IOException {
    _table.updateRow(_rowState, _curPos.getRowId(), row);
  }

  public void updateCurrentRowFromMap(Map<String,?> row) throws IOException {
    _table.updateRow(_rowState, _curPos.getRowId(), _table.asUpdateRow(row));
  }

  public Row getNextRow() throws IOException {
    return getNextRow(null);
  }

  public Row getNextRow(Collection<String> columnNames) 
    throws IOException
  {
    return getAnotherRow(columnNames, MOVE_FORWARD);
  }

  public Row getPreviousRow() throws IOException {
    return getPreviousRow(null);
  }

  public Row getPreviousRow(Collection<String> columnNames) 
    throws IOException
  {
    return getAnotherRow(columnNames, MOVE_REVERSE);
  }


  /**
   * Moves to another row in the table based on the given direction and
   * returns it.
   * @param columnNames Only column names in this collection will be returned
   * @return another row in this table (Column name -> Column value), where
   *         "next" may be backwards if moveForward is {@code false}, or
   *         {@code null} if there is not another row in the given direction.
   */
  private Row getAnotherRow(Collection<String> columnNames,
                                            boolean moveForward) 
    throws IOException
  {
    if(moveToAnotherRow(moveForward)) {
      return getCurrentRow(columnNames);
    }
    return null;
  }  

  public boolean moveToNextRow() throws IOException
  {
    return moveToAnotherRow(MOVE_FORWARD);
  }

  public boolean moveToPreviousRow() throws IOException
  {
    return moveToAnotherRow(MOVE_REVERSE);
  }

  /**
   * Moves to another row in the given direction as defined by this cursor.
   * @return {@code true} if another valid row was found in the given
   *         direction, {@code false} otherwise
   */
  private boolean moveToAnotherRow(boolean moveForward)
    throws IOException
  {
    if(_curPos.equals(getDirHandler(moveForward).getEndPosition())) {
      // already at end, make sure nothing has changed
      return recheckPosition(moveForward);
    }

    return moveToAnotherRowImpl(moveForward);
  }

  /**
   * Restores a current position for the cursor (current position becomes
   * previous position).
   */
  protected void restorePosition(PositionImpl curPos)
    throws IOException
  {
    restorePosition(curPos, _curPos);
  }
    
  /**
   * Restores a current and previous position for the cursor if the given
   * positions are different from the current positions.
   */
  protected final void restorePosition(PositionImpl curPos, 
                                       PositionImpl prevPos)
    throws IOException
  {
    if(!curPos.equals(_curPos) || !prevPos.equals(_prevPos)) {
      restorePositionImpl(curPos, prevPos);
    }
  }

  /**
   * Restores a current and previous position for the cursor.
   */
  protected void restorePositionImpl(PositionImpl curPos, PositionImpl prevPos)
    throws IOException
  {
    // make the current position previous, and the new position current
    _prevPos = _curPos;
    _curPos = curPos;
    _rowState.reset();
  }
  
  /**
   * Rechecks the current position if the underlying data structures have been
   * modified.
   * @return {@code true} if the cursor ended up in a new position,
   *         {@code false} otherwise.
   */
  private boolean recheckPosition(boolean moveForward)
    throws IOException
  {
    if(isUpToDate()) {
      // nothing has changed
      return false;
    }

    // move the cursor back to the previous position
    restorePosition(_prevPos);
    return moveToAnotherRowImpl(moveForward);
  }

  /**
   * Does the grunt work of moving the cursor to another position in the given
   * direction.
   */
  private boolean moveToAnotherRowImpl(boolean moveForward)
    throws IOException
  {
    _rowState.reset();
    _prevPos = _curPos;
    _curPos = findAnotherPosition(_rowState, _curPos, moveForward);
    TableImpl.positionAtRowHeader(_rowState, _curPos.getRowId());
    return(!_curPos.equals(getDirHandler(moveForward).getEndPosition()));
  }

  public boolean findFirstRow(Column columnPattern, Object valuePattern)
    throws IOException
  {
    return findFirstRow((ColumnImpl)columnPattern, valuePattern);
  } 
 
  public boolean findFirstRow(ColumnImpl columnPattern, Object valuePattern)
    throws IOException
  {
    PositionImpl curPos = _curPos;
    PositionImpl prevPos = _prevPos;
    boolean found = false;
    try {
      beforeFirst();
      found = findNextRowImpl(columnPattern, valuePattern);
      return found;
    } finally {
      if(!found) {
        try {
          restorePosition(curPos, prevPos);
        } catch(IOException e) {
          LOG.error("Failed restoring position", e);
        }
      }
    }
  }

  public boolean findNextRow(Column columnPattern, Object valuePattern)
    throws IOException
  {
    return findNextRow((ColumnImpl)columnPattern, valuePattern);
  } 
 
  public boolean findNextRow(ColumnImpl columnPattern, Object valuePattern)
    throws IOException
  {
    PositionImpl curPos = _curPos;
    PositionImpl prevPos = _prevPos;
    boolean found = false;
    try {
      found = findNextRowImpl(columnPattern, valuePattern);
      return found;
    } finally {
      if(!found) {
        try {
          restorePosition(curPos, prevPos);
        } catch(IOException e) {
          LOG.error("Failed restoring position", e);
        }
      }
    }
  }
  
  public boolean findFirstRow(Map<String,?> rowPattern) throws IOException
  {
    PositionImpl curPos = _curPos;
    PositionImpl prevPos = _prevPos;
    boolean found = false;
    try {
      beforeFirst();
      found = findNextRowImpl(rowPattern);
      return found;
    } finally {
      if(!found) {
        try {
          restorePosition(curPos, prevPos);
        } catch(IOException e) {
          LOG.error("Failed restoring position", e);
        }
      }
    }
  }

  public boolean findNextRow(Map<String,?> rowPattern)
    throws IOException
  {
    PositionImpl curPos = _curPos;
    PositionImpl prevPos = _prevPos;
    boolean found = false;
    try {
      found = findNextRowImpl(rowPattern);
      return found;
    } finally {
      if(!found) {
        try {
          restorePosition(curPos, prevPos);
        } catch(IOException e) {
          LOG.error("Failed restoring position", e);
        }
      }
    }
  }

  public boolean currentRowMatches(Column columnPattern, Object valuePattern)
    throws IOException
  {
    return currentRowMatches((ColumnImpl)columnPattern, valuePattern);
  }

  public boolean currentRowMatches(ColumnImpl columnPattern, Object valuePattern)
    throws IOException
  {
    return _columnMatcher.matches(getTable(), columnPattern.getName(),
                                  valuePattern,
                                  getCurrentRowValue(columnPattern));
  }
  
  public boolean currentRowMatches(Map<String,?> rowPattern)
    throws IOException
  {
    Row row = getCurrentRow(rowPattern.keySet());

    if(rowPattern.size() != row.size()) {
      return false;
    }

    for(Map.Entry<String,Object> e : row.entrySet()) {
      String columnName = e.getKey();
      if(!_columnMatcher.matches(getTable(), columnName,
                                 rowPattern.get(columnName), e.getValue())) {
        return false;
      }
    }

    return true;
  }
  
  /**
   * Moves to the next row (as defined by the cursor) where the given column
   * has the given value.  Caller manages save/restore on failure.
   * <p>
   * Default implementation scans the table from beginning to end.
   *
   * @param columnPattern column from the table for this cursor which is being
   *                      matched by the valuePattern
   * @param valuePattern value which is equal to the corresponding value in
   *                     the matched row
   * @return {@code true} if a valid row was found with the given value,
   *         {@code false} if no row was found
   */
  protected boolean findNextRowImpl(ColumnImpl columnPattern, Object valuePattern)
    throws IOException
  {
    while(moveToNextRow()) {
      if(currentRowMatches(columnPattern, valuePattern)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Moves to the next row (as defined by the cursor) where the given columns
   * have the given values.  Caller manages save/restore on failure.
   * <p>
   * Default implementation scans the table from beginning to end.
   *
   * @param rowPattern column names and values which must be equal to the
   *                   corresponding values in the matched row
   * @return {@code true} if a valid row was found with the given values,
   *         {@code false} if no row was found
   */
  protected boolean findNextRowImpl(Map<String,?> rowPattern)
    throws IOException
  {
    while(moveToNextRow()) {
      if(currentRowMatches(rowPattern)) {
        return true;
      }
    }
    return false;
  }  

  public int moveNextRows(int numRows) throws IOException
  {
    return moveSomeRows(numRows, MOVE_FORWARD);
  }

  public int movePreviousRows(int numRows) throws IOException
  {
    return moveSomeRows(numRows, MOVE_REVERSE);
  }

  /**
   * Moves as many rows as possible in the given direction up to the given
   * number of rows.
   * @return the number of rows moved.
   */
  private int moveSomeRows(int numRows, boolean moveForward)
    throws IOException
  {
    int numMovedRows = 0;
    while((numMovedRows < numRows) && moveToAnotherRow(moveForward)) {
      ++numMovedRows;
    }
    return numMovedRows;
  }

  public Row getCurrentRow() throws IOException
  {
    return getCurrentRow(null);
  }

  public Row getCurrentRow(Collection<String> columnNames)
    throws IOException
  {
    return _table.getRow(_rowState, _curPos.getRowId(), columnNames);
  }

  public Object getCurrentRowValue(Column column)
    throws IOException
  {
    return getCurrentRowValue((ColumnImpl)column);
  }

  public Object getCurrentRowValue(ColumnImpl column)
    throws IOException
  {
    return _table.getRowValue(_rowState, _curPos.getRowId(), column);
  }

  public void setCurrentRowValue(Column column, Object value)
    throws IOException
  {
    setCurrentRowValue((ColumnImpl)column, value);
  }

  public void setCurrentRowValue(ColumnImpl column, Object value)
    throws IOException
  {
    Object[] row = new Object[_table.getColumnCount()];
    Arrays.fill(row, Column.KEEP_VALUE);
    column.setRowValue(row, value);
    _table.updateRow(_rowState, _curPos.getRowId(), row);
  }

  /**
   * Returns {@code true} if this cursor is up-to-date with respect to the
   * relevant table and related table objects, {@code false} otherwise.
   */
  protected boolean isUpToDate() {
    return _rowState.isUpToDate();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " CurPosition " + _curPos +
      ", PrevPosition " + _prevPos;
  }
    
  /**
   * Finds the next non-deleted row after the given row (as defined by this
   * cursor) and returns the id of the row, where "next" may be backwards if
   * moveForward is {@code false}.  If there are no more rows, the returned
   * rowId should equal the value returned by {@link #getLastPosition} if
   * moving forward and {@link #getFirstPosition} if moving backward.
   */
  protected abstract PositionImpl findAnotherPosition(RowState rowState,
                                                      PositionImpl curPos,
                                                      boolean moveForward)
    throws IOException;

  /**
   * Returns the DirHandler for the given movement direction.
   */
  protected abstract DirHandler getDirHandler(boolean moveForward);


  /**
   * Base implementation of iterator for this cursor, modifiable.
   */
  protected abstract class BaseIterator
    implements Iterator<Row>
  {
    protected final Collection<String> _columnNames;
    protected Boolean _hasNext;
    protected boolean _validRow;
    
    protected BaseIterator(Collection<String> columnNames)
    {
      _columnNames = columnNames;
    }

    public boolean hasNext() {
      if(_hasNext == null) {
        try {
          _hasNext = findNext();
          _validRow = _hasNext;
        } catch(IOException e) {
          throw new RuntimeIOException(e);
        }
      }
      return _hasNext; 
    }
    
    public Row next() {
      if(!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        Row rtn = getCurrentRow(_columnNames);
        _hasNext = null;
        return rtn;
      } catch(IOException e) {
        throw new RuntimeIOException(e);
      }
    }

    public void remove() {
      if(_validRow) {
        try {
          deleteCurrentRow();
          _validRow = false;
        } catch(IOException e) {
          throw new RuntimeIOException(e);
        }
      } else {
        throw new IllegalStateException("Not at valid row");
      }
    }

    protected abstract boolean findNext() throws IOException;
  }

  
  /**
   * Row iterator for this cursor, modifiable.
   */
  private final class RowIterator extends BaseIterator
  {
    private final boolean _moveForward;
    
    private RowIterator(Collection<String> columnNames, boolean moveForward)
    {
      super(columnNames);
      _moveForward = moveForward;
      reset(_moveForward);
    }

    @Override
    protected boolean findNext() throws IOException {
      return moveToAnotherRow(_moveForward);
    }
  }


  /**
   * Row iterator for this cursor, modifiable.
   */
  private final class ColumnMatchIterator extends BaseIterator
  {
    private final ColumnImpl _columnPattern;
    private final Object _valuePattern;
    
    private ColumnMatchIterator(Collection<String> columnNames,
                                ColumnImpl columnPattern, Object valuePattern)
    {
      super(columnNames);
      _columnPattern = columnPattern;
      _valuePattern = valuePattern;
      beforeFirst();
    }

    @Override
    protected boolean findNext() throws IOException {
      return findNextRow(_columnPattern, _valuePattern);
    }
  }


  /**
   * Row iterator for this cursor, modifiable.
   */
  private final class RowMatchIterator extends BaseIterator
  {
    private final Map<String,?> _rowPattern;
    
    private RowMatchIterator(Collection<String> columnNames,
                             Map<String,?> rowPattern)
    {
      super(columnNames);
      _rowPattern = rowPattern;
      beforeFirst();
    }

    @Override
    protected boolean findNext() throws IOException {
      return findNextRow(_rowPattern);
    }
  }


  /**
   * Handles moving the cursor in a given direction.  Separates cursor
   * logic from value storage.
   */
  protected abstract class DirHandler
  {
    public abstract PositionImpl getBeginningPosition();
    public abstract PositionImpl getEndPosition();
  }

  
  /**
   * Simple un-indexed cursor.
   */
  private static final class TableScanCursor extends CursorImpl
  {
    /** ScanDirHandler for forward traversal */
    private final ScanDirHandler _forwardDirHandler =
      new ForwardScanDirHandler();
    /** ScanDirHandler for backward traversal */
    private final ScanDirHandler _reverseDirHandler =
      new ReverseScanDirHandler();
    /** Cursor over the pages that this table owns */
    private final UsageMap.PageCursor _ownedPagesCursor;
    
    private TableScanCursor(TableImpl table) {
      super(new IdImpl(table, null), table,
            FIRST_SCAN_POSITION, LAST_SCAN_POSITION);
      _ownedPagesCursor = table.getOwnedPagesCursor();
    }

    @Override
    protected ScanDirHandler getDirHandler(boolean moveForward) {
      return (moveForward ? _forwardDirHandler : _reverseDirHandler);
    }

    @Override
    protected boolean isUpToDate() {
      return(super.isUpToDate() && _ownedPagesCursor.isUpToDate());
    }
    
    @Override
    protected void reset(boolean moveForward) {
      _ownedPagesCursor.reset(moveForward);
      super.reset(moveForward);
    }

    @Override
    protected void restorePositionImpl(PositionImpl curPos, PositionImpl prevPos)
      throws IOException
    {
      if(!(curPos instanceof ScanPosition) ||
         !(prevPos instanceof ScanPosition)) {
        throw new IllegalArgumentException(
            "Restored positions must be scan positions");
      }
      _ownedPagesCursor.restorePosition(curPos.getRowId().getPageNumber(),
                                        prevPos.getRowId().getPageNumber());
      super.restorePositionImpl(curPos, prevPos);
    }

    @Override
    protected PositionImpl findAnotherPosition(
        RowState rowState, PositionImpl curPos, boolean moveForward)
      throws IOException
    {
      ScanDirHandler handler = getDirHandler(moveForward);
      
      // figure out how many rows are left on this page so we can find the
      // next row
      RowIdImpl curRowId = curPos.getRowId();
      TableImpl.positionAtRowHeader(rowState, curRowId);
      int currentRowNumber = curRowId.getRowNumber();
    
      // loop until we find the next valid row or run out of pages
      while(true) {

        currentRowNumber = handler.getAnotherRowNumber(currentRowNumber);
        curRowId = new RowIdImpl(curRowId.getPageNumber(), currentRowNumber);
        TableImpl.positionAtRowHeader(rowState, curRowId);
        
        if(!rowState.isValid()) {
          
          // load next page
          curRowId = new RowIdImpl(handler.getAnotherPageNumber(),
                               RowIdImpl.INVALID_ROW_NUMBER);
          TableImpl.positionAtRowHeader(rowState, curRowId);
          
          if(!rowState.isHeaderPageNumberValid()) {
            //No more owned pages.  No more rows.
            return handler.getEndPosition();
          }

          // update row count and initial row number
          currentRowNumber = handler.getInitialRowNumber(
              rowState.getRowsOnHeaderPage());

        } else if(!rowState.isDeleted()) {
          
          // we found a valid, non-deleted row, return it
          return new ScanPosition(curRowId);
        }
        
      }
    }

    /**
     * Handles moving the table scan cursor in a given direction.  Separates
     * cursor logic from value storage.
     */
    private abstract class ScanDirHandler extends DirHandler {
      public abstract int getAnotherRowNumber(int curRowNumber);
      public abstract int getAnotherPageNumber();
      public abstract int getInitialRowNumber(int rowsOnPage);
    }
    
    /**
     * Handles moving the table scan cursor forward.
     */
    private final class ForwardScanDirHandler extends ScanDirHandler {
      @Override
      public PositionImpl getBeginningPosition() {
        return getFirstPosition();
      }
      @Override
      public PositionImpl getEndPosition() {
        return getLastPosition();
      }
      @Override
      public int getAnotherRowNumber(int curRowNumber) {
        return curRowNumber + 1;
      }
      @Override
      public int getAnotherPageNumber() {
        return _ownedPagesCursor.getNextPage();
      }
      @Override
      public int getInitialRowNumber(int rowsOnPage) {
        return -1;
      }
    }
    
    /**
     * Handles moving the table scan cursor backward.
     */
    private final class ReverseScanDirHandler extends ScanDirHandler {
      @Override
      public PositionImpl getBeginningPosition() {
        return getLastPosition();
      }
      @Override
      public PositionImpl getEndPosition() {
        return getFirstPosition();
      }
      @Override
      public int getAnotherRowNumber(int curRowNumber) {
        return curRowNumber - 1;
      }
      @Override
      public int getAnotherPageNumber() {
        return _ownedPagesCursor.getPreviousPage();
      }
      @Override
      public int getInitialRowNumber(int rowsOnPage) {
        return rowsOnPage;
      }
    }
    
  }


  /**
   * Identifier for a cursor.  Will be equal to any other cursor of the same
   * type for the same table.  Primarily used to check the validity of a
   * Savepoint.
   */
  protected static final class IdImpl implements Id
  {
    private final String _tableName;
    private final String _indexName;

    protected IdImpl(TableImpl table, Index index) {
      _tableName = table.getName();
      _indexName = ((index != null) ? index.getName() : null);
    }

    @Override
    public int hashCode() {
      return _tableName.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return((this == o) ||
             ((o != null) && (getClass() == o.getClass()) &&
              ObjectUtils.equals(_tableName, ((IdImpl)o)._tableName) &&
              ObjectUtils.equals(_indexName, ((IdImpl)o)._indexName)));
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " " + _tableName + ":" + _indexName;
    }
  }

  /**
   * Value object which maintains the current position of the cursor.
   */
  protected static abstract class PositionImpl implements Position
  {
    protected PositionImpl() {
    }

    @Override
    public final int hashCode() {
      return getRowId().hashCode();
    }
    
    @Override
    public final boolean equals(Object o) {
      return((this == o) ||
             ((o != null) && (getClass() == o.getClass()) && equalsImpl(o)));
    }

    /**
     * Returns the unique RowId of the position of the cursor.
     */
    public abstract RowIdImpl getRowId();

    /**
     * Returns {@code true} if the subclass specific info in a Position is
     * equal, {@code false} otherwise.
     * @param o object being tested for equality, guaranteed to be the same
     *          class as this object
     */
    protected abstract boolean equalsImpl(Object o);
  }

  /**
   * Value object which represents a complete save state of the cursor.
   */
  protected static final class SavepointImpl implements Savepoint
  {
    private final IdImpl _cursorId;
    private final PositionImpl _curPos;
    private final PositionImpl _prevPos;

    private SavepointImpl(IdImpl cursorId, PositionImpl curPos, 
                          PositionImpl prevPos) {
      _cursorId = cursorId;
      _curPos = curPos;
      _prevPos = prevPos;
    }

    public IdImpl getCursorId() {
      return _cursorId;
    }

    public PositionImpl getCurrentPosition() {
      return _curPos;
    }

    private PositionImpl getPreviousPosition() {
      return _prevPos;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " " + _cursorId + " CurPosition " + 
        _curPos + ", PrevPosition " + _prevPos;
    }
  }
  
  /**
   * Value object which maintains the current position of a TableScanCursor.
   */
  private static final class ScanPosition extends PositionImpl
  {
    private final RowIdImpl _rowId;

    private ScanPosition(RowIdImpl rowId) {
      _rowId = rowId;
    }

    @Override
    public RowIdImpl getRowId() {
      return _rowId;
    }

    @Override
    protected boolean equalsImpl(Object o) {
      return getRowId().equals(((ScanPosition)o).getRowId());
    }
    
    @Override
    public String toString() {
      return "RowId = " + getRowId();
    }
  }
  
}
