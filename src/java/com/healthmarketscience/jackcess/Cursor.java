// Copyright (c) 2007 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.healthmarketscience.jackcess.Table.RowState;
import org.apache.commons.lang.ObjectUtils;


/**
 * Manages iteration for a Table.  Different cursors provide different methods
 * of traversing a table.  Cursors should be fairly robust in the face of
 * table modification during traversal (although depending on how the table is
 * traversed, row updates may or may not be seen).  Multiple cursors may
 * traverse the same table simultaneously.
 * <p>
 * Is not thread-safe.
 *
 * @author james
 */
public abstract class Cursor implements Iterable<Map<String, Object>>
{  
  /** owning table */
  private final Table _table;
  /** State used for reading the table rows */
  private final RowState _rowState;
  /** the first (exclusive) row id for this iterator */
  private final RowId _firstRowId;
  /** the last (exclusive) row id for this iterator */
  private final RowId _lastRowId;
  /** the previous row */
  private RowId _previousRowId;
  /** the current row */
  private RowId _currentRowId;
  

  protected Cursor(Table table, RowId firstRowId, RowId lastRowId) {
    _table = table;
    _rowState = _table.createRowState();
    _firstRowId = firstRowId;
    _lastRowId = lastRowId;
    _currentRowId = firstRowId;
    _previousRowId = firstRowId;
  }

  /**
   * Creates a normal, un-indexed cursor for the given table.
   * @param table the table over which this cursor will traverse
   */
  public static Cursor createCursor(Table table) {
    return new TableScanCursor(table);
  }

  /**
   * Convenience method for finding a specific row in a table which matches a
   * given row "pattern.  See {@link #findRow(Map)} for details on the
   * rowPattern.
   * 
   * @param table the table to search
   * @param rowPattern pattern to be used to find the row
   * @return the matching row or {@code null} if a match could not be found.
   */
  public static Map<String,Object> findRow(Table table,
                                           Map<String,Object> rowPattern)
    throws IOException
  {
    Cursor cursor = createCursor(table);
    if(cursor.findRow(rowPattern)) {
      return cursor.getCurrentRow();
    }
    return null;
  }
  
  /**
   * Convenience method for finding a specific row in a table which matches a
   * given row "pattern.  See {@link #findRow(Column,Object)} for details on
   * the pattern.
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
    if(cursor.findRow(columnPattern, valuePattern)) {
      return cursor.getCurrentRowValue(column);
    }
    return null;
  }
  
  public Table getTable() {
    return _table;
  }
  
  public JetFormat getFormat() {
    return getTable().getFormat();
  }

  public PageChannel getPageChannel() {
    return getTable().getPageChannel();
  }

  public RowId getCurrentRowId() {
    return _currentRowId;
  }
  
  /**
   * Returns the first row id (exclusive) as defined by this cursor.
   */
  protected RowId getFirstRowId() {
    return _firstRowId;
  }
  
  /**
   * Returns the last row id (exclusive) as defined by this cursor.
   */
  protected RowId getLastRowId() {
    return _lastRowId;
  }

  /**
   * Resets this cursor for forward iteration.  Calls {@link #beforeFirst}.
   */
  public void reset() {
    beforeFirst();
  }  

  /**
   * Resets this cursor for forward iteration (sets cursor to before the first
   * row).
   */
  public void beforeFirst() {
    reset(true);
  }
  
  /**
   * Resets this cursor for reverse iteration (sets cursor to after the last
   * row).
   */
  public void afterLast() {
    reset(false);
  }

  /**
   * Returns {@code true} if the cursor is currently positioned before the
   * first row, {@code false} otherwise.
   */
  public boolean isBeforeFirst()
    throws IOException
  {
    if(getFirstRowId().equals(_currentRowId)) {
      return !recheckPosition(false);
    }
    return false;
  }
  
  /**
   * Returns {@code true} if the cursor is currently positioned after the
   * last row, {@code false} otherwise.
   */
  public boolean isAfterLast()
    throws IOException
  {
    if(getLastRowId().equals(_currentRowId)) {
      return !recheckPosition(true);
    }
    return false;
  }

  /**
   * Returns {@code true} if the row at which the cursor is currently
   * positioned is deleted, {@code false} otherwise (including invalid rows).
   */
  public boolean isCurrentRowDeleted()
    throws IOException
  {
    // we need to ensure that the "deleted" flag has been read for this row
    // (or re-read if the table has been recently modified)
    Table.positionAtRowData(_rowState, _currentRowId);
    return _rowState.isDeleted();
  }
  
  /**
   * Resets this cursor for iterating the given direction.
   */
  protected void reset(boolean moveForward) {
    _currentRowId = getDirHandler(moveForward).getBeginningRowId();
    _previousRowId = _currentRowId;
    _rowState.reset();
  }  

  /**
   * Returns an Iterable whose iterator() method calls <code>afterLast</code>
   * on this cursor and returns an unmodifiable Iterator which will iterate
   * through all the rows of this table in reverse order.  Use of the Iterator
   * follows the same restrictions as a call to <code>getPreviousRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Map<String, Object>> reverseIterable() {
    return reverseIterable(null);
  }
  
  /**
   * Returns an Iterable whose iterator() method calls <code>afterLast</code>
   * on this table and returns an unmodifiable Iterator which will iterate
   * through all the rows of this table in reverse order, returning only the
   * given columns.  Use of the Iterator follows the same restrictions as a
   * call to <code>getPreviousRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Map<String, Object>> reverseIterable(
      final Collection<String> columnNames)
  {
    return new Iterable<Map<String, Object>>() {
      public Iterator<Map<String, Object>> iterator() {
        return new RowIterator(columnNames, false);
      }
    };
  }
  
  /**
   * Calls <code>beforeFirst</code> on this cursor and returns an unmodifiable
   * Iterator which will iterate through all the rows of this table.  Use of
   * the Iterator follows the same restrictions as a call to
   * <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String, Object>> iterator()
  {
    return iterator(null);
  }
  
  /**
   * Calls <code>beforeFirst</code> on this table and returns an unmodifiable
   * Iterator which will iterate through all the rows of this table, returning
   * only the given columns.  Use of the Iterator follows the same
   * restrictions as a call to <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String, Object>> iterator(Collection<String> columnNames)
  {
    return new RowIterator(columnNames, true);
  }

  /**
   * Delete the current row.
   * @throws IllegalStateException if the current row is not valid (at
   *         beginning or end of table), or already deleted.
   */
  public void deleteCurrentRow() throws IOException {
    _table.deleteRow(_rowState, _currentRowId);
  }

  /**
   * Moves to the next row in the table and returns it.
   * @return The next row in this table (Column name -> Column value), or
   *         {@code null} if no next row is found
   */
  public Map<String, Object> getNextRow() throws IOException {
    return getNextRow(null);
  }

  /**
   * Moves to the next row in the table and returns it.
   * @param columnNames Only column names in this collection will be returned
   * @return The next row in this table (Column name -> Column value), or
   *         {@code null} if no next row is found
   */
  public Map<String, Object> getNextRow(Collection<String> columnNames) 
    throws IOException
  {
    return getAnotherRow(columnNames, true);
  }

  /**
   * Moves to the previous row in the table and returns it.
   * @return The previous row in this table (Column name -> Column value), or
   *         {@code null} if no previous row is found
   */
  public Map<String, Object> getPreviousRow() throws IOException {
    return getPreviousRow(null);
  }

  /**
   * Moves to the previous row in the table and returns it.
   * @param columnNames Only column names in this collection will be returned
   * @return The previous row in this table (Column name -> Column value), or
   *         {@code null} if no previous row is found
   */
  public Map<String, Object> getPreviousRow(Collection<String> columnNames) 
    throws IOException
  {
    return getAnotherRow(columnNames, false);
  }


  /**
   * Moves to another row in the table based on the given direction and
   * returns it.
   * @param columnNames Only column names in this collection will be returned
   * @return another row in this table (Column name -> Column value), where
   *         "next" may be backwards if moveForward is {@code false}, or
   *         {@code null} if there is not another row in the given direction.
   */
  private Map<String, Object> getAnotherRow(Collection<String> columnNames,
                                            boolean moveForward) 
    throws IOException
  {
    if(moveToAnotherRow(moveForward)) {
      return getCurrentRow(columnNames);
    }
    return null;
  }  

  /**
   * Moves to the next row as defined by this cursor.
   * @return {@code true} if a valid next row was found, {@code false}
   *         otherwise
   */
  public boolean moveToNextRow()
    throws IOException
  {
    return moveToAnotherRow(true);
  }

  /**
   * Moves to the previous row as defined by this cursor.
   * @return {@code true} if a valid previous row was found, {@code false}
   *         otherwise
   */
  public boolean moveToPreviousRow()
    throws IOException
  {
    return moveToAnotherRow(false);
  }

  /**
   * Moves to another row in the given direction as defined by this cursor.
   * @return {@code true} if another valid row was found in the given
   *         direction, {@code false} otherwise
   */
  private boolean moveToAnotherRow(boolean moveForward)
    throws IOException
  {
    RowId endRowId = getDirHandler(moveForward).getEndRowId();
    if(_currentRowId.equals(endRowId)) {
      // already at end, make sure nothing has changed
      return recheckPosition(moveForward);
    }

    return moveToAnotherRowImpl(moveForward);
  }

  /**
   * Restores the current position to the previous position.
   */
  protected void restorePreviousPosition()
    throws IOException
  {
    // essentially swap current and previous
    RowId tmp = _previousRowId;
    _previousRowId = _currentRowId;
    _currentRowId = tmp;
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
    restorePreviousPosition();
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
    _previousRowId = _currentRowId;
    _currentRowId = findAnotherRowId(_rowState, _currentRowId, moveForward);
    Table.positionAtRowHeader(_rowState, _currentRowId);
    return(!_currentRowId.equals(getDirHandler(moveForward).getEndRowId()));
  }
  
  /**
   * Moves to the first row (as defined by the cursor) where the given column
   * has the given value.  This may be more efficient on some cursors than
   * others.  The location of the cursor when a match is not found is
   * undefined.
   *
   * @param columnPattern column from the table for this cursor which is being
   *                      matched by the valuePattern
   * @param valuePattern value which is equal to the corresponding value in
   *                     the matched row
   * @return {@code true} if a valid row was found with the given value,
   *         {@code false} if no row was found
   */
  public boolean findRow(Column columnPattern, Object valuePattern)
    throws IOException
  {
    // FIXME, add save restore?

    beforeFirst();
    while(moveToNextRow()) {
      if(ObjectUtils.equals(valuePattern, getCurrentRowValue(columnPattern))) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Moves to the first row (as defined by the cursor) where the given columns
   * have the given values.  This may be more efficient on some cursors than
   * others.  The location of the cursor when a match is not found is
   * undefined.
   *
   * @param rowPattern column names and values which must be equal to the
   *                   corresponding values in the matched row
   * @return {@code true} if a valid row was found with the given values,
   *         {@code false} if no row was found
   */
  public boolean findRow(Map<String,Object> rowPattern)
    throws IOException
  {
    // FIXME, add save restore?

    beforeFirst();
    Collection<String> columnNames = rowPattern.keySet();
    while(moveToNextRow()) {
      if(ObjectUtils.equals(rowPattern, getCurrentRow(columnNames))) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Skips as many rows as possible up to the given number of rows.
   * @return the number of rows skipped.
   */
  public int skipNextRows(int numRows)
    throws IOException
  {
    return skipSomeRows(numRows, true);
  }

  /**
   * Skips as many rows as possible up to the given number of rows.
   * @return the number of rows skipped.
   */
  public int skipPreviousRows(int numRows)
    throws IOException
  {
    return skipSomeRows(numRows, false);
  }

  /**
   * Skips as many rows as possible in the given direction up to the given
   * number of rows.
   * @return the number of rows skipped.
   */
  private int skipSomeRows(int numRows, boolean moveForward)
    throws IOException
  {
    int numSkippedRows = 0;
    while((numSkippedRows < numRows) && moveToAnotherRow(moveForward)) {
      ++numSkippedRows;
    }
    return numSkippedRows;
  }

  /**
   * Returns the current row in this cursor (Column name -> Column value).
   * @param columnNames Only column names in this collection will be returned
   */
  public Map<String, Object> getCurrentRow()
    throws IOException
  {
    return getCurrentRow(null);
  }

  /**
   * Returns the current row in this cursor (Column name -> Column value).
   * @param columnNames Only column names in this collection will be returned
   */
  public Map<String, Object> getCurrentRow(Collection<String> columnNames)
    throws IOException
  {
    return _table.getRow(_rowState, _currentRowId, columnNames);
  }

  /**
   * Returns the given column from the current row.
   */
  @SuppressWarnings("foo")
  public Object getCurrentRowValue(Column column)
    throws IOException
  {
    return _table.getRowValue(_rowState, _currentRowId, column);
  }

  /**
   * Returns {@code true} if this cursor is up-to-date with respect to the
   * relevant table and related table objects, {@code false} otherwise.
   */
  protected boolean isUpToDate() {
    return _rowState.isUpToDate();
  }
  
  /**
   * Finds the next non-deleted row after the given row (as defined by this
   * cursor) and returns the id of the row, where "next" may be backwards if
   * moveForward is {@code false}.  If there are no more rows, the returned
   * rowId should equal the value returned by {@link #getLastRowId} if moving
   * forward and {@link #getFirstRowId} if moving backward.
   */
  protected abstract RowId findAnotherRowId(RowState rowState,
                                            RowId currentRowId,
                                            boolean moveForward)
    throws IOException;

  /**
   * Returns the DirHandler for the given movement direction.
   */
  protected abstract DirHandler getDirHandler(boolean moveForward);
  
  /**
   * Row iterator for this table, supports modification.
   */
  private final class RowIterator implements Iterator<Map<String, Object>>
  {
    private final Collection<String> _columnNames;
    private final boolean _moveForward;
    private boolean _hasNext = false;
    
    private RowIterator(Collection<String> columnNames, boolean moveForward)
    {
      try {
        _columnNames = columnNames;
        _moveForward = moveForward;
        reset(_moveForward);
        _hasNext = moveToAnotherRow(_moveForward);
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public boolean hasNext() { return _hasNext; }

    public void remove() {
      throw new UnsupportedOperationException();
    }
    
    public Map<String, Object> next() {
      if(!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        Map<String, Object> rtn = getCurrentRow(_columnNames);
        _hasNext = moveToAnotherRow(_moveForward);
        return rtn;
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }
    }
    
  }

  /**
   * Handles moving the cursor in a given direction.  Separates cursor
   * logic from value storage.
   */
  protected abstract class DirHandler
  {
    public abstract RowId getBeginningRowId();
    public abstract RowId getEndRowId();
  }

  
  /**
   * Simple un-indexed cursor.
   */
  private static class TableScanCursor extends Cursor
  {
    /** ScanDirHandler for forward traversal */
    private final ScanDirHandler _forwardDirHandler =
      new ForwardScanDirHandler();
    /** ScanDirHandler for backward traversal */
    private final ScanDirHandler _reverseDirHandler =
      new ReverseScanDirHandler();
    /** Cursor over the pages that this table owns */
    private final UsageMap.PageCursor _ownedPagesCursor;
    
    private TableScanCursor(Table table) {
      super(table, RowId.FIRST_ROW_ID, RowId.LAST_ROW_ID);
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
    protected void restorePreviousPosition()
      throws IOException
    {
      super.restorePreviousPosition();
      _ownedPagesCursor.setCurrentPage(getCurrentRowId().getPageNumber());
    }
    
    /**
     * Position the buffer at the next row in the table
     * @return a ByteBuffer narrowed to the next row, or null if none
     */
    @Override
    protected RowId findAnotherRowId(RowState rowState, RowId currentRowId,
                                     boolean moveForward)
      throws IOException
    {
      ScanDirHandler handler = getDirHandler(moveForward);
      
      // figure out how many rows are left on this page so we can find the
      // next row
      Table.positionAtRowHeader(rowState, currentRowId);
      int currentRowNumber = currentRowId.getRowNumber();
    
      // loop until we find the next valid row or run out of pages
      while(true) {

        currentRowNumber = handler.getAnotherRowNumber(currentRowNumber);
        currentRowId = new RowId(currentRowId.getPageNumber(),
                                 currentRowNumber);
        Table.positionAtRowHeader(rowState, currentRowId);
        
        if(!rowState.isValid()) {
          
          // load next page
          currentRowId = new RowId(handler.getAnotherPageNumber(),
                                   RowId.INVALID_ROW_NUMBER);
          Table.positionAtRowHeader(rowState, currentRowId);
          
          if(!rowState.isHeaderPageNumberValid()) {
            //No more owned pages.  No more rows.
            return handler.getEndRowId();
          }

          // update row count and initial row number
          currentRowNumber = handler.getInitialRowNumber(
              rowState.getRowsOnHeaderPage());

        } else if(!rowState.isDeleted()) {
          
          // we found a valid, non-deleted row, return it
          return currentRowId;
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
      public RowId getBeginningRowId() {
        return getFirstRowId();
      }
      @Override
      public RowId getEndRowId() {
        return getLastRowId();
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
      public RowId getBeginningRowId() {
        return getLastRowId();
      }
      @Override
      public RowId getEndRowId() {
        return getFirstRowId();
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
  
}
