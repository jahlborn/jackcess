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

import static com.healthmarketscience.jackcess.PageChannel.INVALID_PAGE_NUMBER;
import static com.healthmarketscience.jackcess.RowId.INVALID_ROW_NUMBER;


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
  private static final int FIRST_PAGE_NUMBER = INVALID_PAGE_NUMBER;
  private static final int LAST_PAGE_NUMBER = Integer.MAX_VALUE;

  public static final RowId FIRST_ROW_ID = new RowId(
      FIRST_PAGE_NUMBER, INVALID_ROW_NUMBER);

  public static final RowId LAST_ROW_ID = new RowId(
      LAST_PAGE_NUMBER, INVALID_ROW_NUMBER);

  
  /** owning table */
  protected final Table _table;
  /** State used for reading the table rows */
  protected final RowState _rowState;
  /** the first (exclusive) row id for this iterator */
  protected final RowId _firstRowId;
  /** the last (exclusive) row id for this iterator */
  protected final RowId _lastRowId;
  /** the current row */
  protected RowId _currentRowId;
  

  protected Cursor(Table table, RowId firstRowId, RowId lastRowId) {
    _table = table;
    _rowState = _table.createRowState();
    _firstRowId = firstRowId;
    _lastRowId = lastRowId;
    _currentRowId = firstRowId;
  }

  /**
   * Creates a normal, un-indexed cursor for the given table.
   */
  public static Cursor createCursor(Table table) {
    return new TableScanCursor(table);
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
  
  public void reset() {
    _currentRowId = getFirstRowId();
    _rowState.reset();
  }  

  /**
   * Calls <code>reset</code> on this table and returns a modifiable Iterator
   * which will iterate through all the rows of this table.  Use of the
   * Iterator follows the same restrictions as a call to
   * <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String, Object>> iterator()
  {
    return iterator(null);
  }
  
  /**
   * Calls <code>reset</code> on this table and returns a modifiable Iterator
   * which will iterate through all the rows of this table, returning only the
   * given columns.  Use of the Iterator follows the same restrictions as a
   * call to <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String, Object>> iterator(Collection<String> columnNames)
  {
    return new RowIterator(columnNames);
  }

  /**
   * Delete the current row (retrieved by a call to {@link #getNextRow}).
   */
  public void deleteCurrentRow() throws IOException {
    _table.deleteRow(_rowState, _currentRowId);
  }

  /**
   * @return The next row in this table (Column name -> Column value)
   */
  public Map<String, Object> getNextRow() throws IOException {
    return getNextRow(null);
  }

  /**
   * @param columnNames Only column names in this collection will be returned
   * @return The next row in this table (Column name -> Column value)
   */
  public Map<String, Object> getNextRow(Collection<String> columnNames) 
    throws IOException
  {
    if(moveToNextRow()) {
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
    if(_currentRowId.equals(getLastRowId())) {
      // already at end
      return false;
    }
    
    _rowState.reset();
    _currentRowId = findNextRowId(_currentRowId);
    return(!_currentRowId.equals(getLastRowId()));
  }

  /**
   * Moves to the first row (as defined by the cursor) where the given column
   * has the given value.  This may be more efficient on some cursors than
   * others.
   * 
   * @return {@code true} if a valid row was found with the given value,
   *         {@code false} if no row was found (and the cursor is now pointing
   *         past the end of the table)
   */
  public boolean moveToRow(Column column, Object value)
    throws IOException
  {
    while(moveToNextRow()) {
      if(ObjectUtils.equals(value, getCurrentRowSingleColumn(column))) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Moves to the first row (as defined by the cursor) where the given columns
   * have the given values.  This may be more efficient on some cursors than
   * others.
   * 
   * @return {@code true} if a valid row was found with the given values,
   *         {@code false} if no row was found (and the cursor is now pointing
   *         past the end of the table)
   */
  public boolean moveToRow(Map<String,Object> row)
    throws IOException
  {
    while(moveToNextRow()) {
      if(ObjectUtils.equals(row, getCurrentRow(row.keySet()))) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Skips as many rows as possible up to the given number of rows.
   * @return the number of rows skipped.
   */
  public int skipRows(int numRows)
    throws IOException
  {
    int numSkippedRows = 0;
    while((numSkippedRows < numRows) && moveToNextRow()) {
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
    return _table.getRow(_rowState, columnNames);
  }

  /**
   * Returns the given column from the current row.
   */
  public Object getCurrentRowSingleColumn(Column column)
    throws IOException
  {
    return _table.getRowSingleColumn(_rowState, column);
  }
  
  /**
   * Returns {@code true} if the row is marked as deleted, {@code false}
   * otherwise.  This method will not modify the rowState (it only looks at
   * the "main" row, which is where the deleted flag is located).
   */
  protected final boolean isCurrentRowDeleted()
    throws IOException
  {
    ByteBuffer rowBuffer = _rowState.getFinalPage();
    int rowNum = _rowState.getFinalRowNumber();
    
    // note, we don't use findRowStart here cause we need the unmasked value
    return Table.isDeletedRow(
        rowBuffer.getShort(Table.getRowStartOffset(rowNum, getFormat())));
  }

  /**
   * Returns the row count for the current page.  If the page number is
   * invalid or the page is not a DATA page, 0 is returned.
   */
  protected final int getRowsOnCurrentDataPage(ByteBuffer rowBuffer)
    throws IOException
  {
    int rowsOnPage = 0;
    if((rowBuffer != null) && (rowBuffer.get(0) == PageTypes.DATA)) {
      rowsOnPage = 
        rowBuffer.getShort(getFormat().OFFSET_NUM_ROWS_ON_DATA_PAGE);
    }
    return rowsOnPage;
  }

  /**
   * Finds the next non-deleted row after the given row as defined by this
   * cursor and returns the id of the row.  If there are no more rows, the
   * returned rowId should equal the value returned by {@link #getLastRowId}.
   */
  protected abstract RowId findNextRowId(RowId currentRowId)
    throws IOException;
  
  /**
   * Row iterator for this table, supports modification.
   */
  private final class RowIterator implements Iterator<Map<String, Object>>
  {
    private Collection<String> _columnNames;
    private boolean _hasNext = false;
    
    private RowIterator(Collection<String> columnNames)
    {
      try {
        reset();
        _columnNames = columnNames;
        _hasNext = moveToNextRow();
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public boolean hasNext() { return _hasNext; }

    public void remove() {
      try {
        deleteCurrentRow();
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }        
    }
    
    public Map<String, Object> next() {
      if(!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        Map<String, Object> rtn = getCurrentRow(_columnNames);
        _hasNext = moveToNextRow();
        return rtn;
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }
    }
    
  }

  /**
   * Simple un-indexed cursor.
   */
  private static class TableScanCursor extends Cursor
  {
    /** Iterator over the pages that this table owns */
    private final UsageMap.PageIterator _ownedPagesIterator;
    
    private TableScanCursor(Table table) {
      super(table, FIRST_ROW_ID, LAST_ROW_ID);
      _ownedPagesIterator = table.getOwnedPagesIterator();
    }
    
    @Override
    public void reset() {
      _ownedPagesIterator.reset();
      super.reset();
    }

    /**
     * Position the buffer at the next row in the table
     * @return a ByteBuffer narrowed to the next row, or null if none
     */
    @Override
    protected RowId findNextRowId(RowId currentRowId)
      throws IOException
    {

      // prepare to read next row
      _rowState.reset();
      int currentPageNumber = currentRowId.getPageNumber();
      int currentRowNumber = currentRowId.getRowNumber();

      int rowsOnPage = getRowsOnCurrentDataPage(
          _rowState.setRow(currentPageNumber, currentRowNumber));
    
      // loop until we find the next valid row or run out of pages
      while(true) {

        currentRowNumber++;
        if(currentRowNumber < rowsOnPage) {
          _rowState.setRow(currentPageNumber, currentRowNumber);
        } else {

          // load next page
          currentRowNumber = INVALID_ROW_NUMBER;
          currentPageNumber = _ownedPagesIterator.getNextPage();
        
          ByteBuffer rowBuffer = _rowState.setRow(
              currentPageNumber, currentRowNumber);
          if(rowBuffer == null) {
            //No more owned pages.  No more rows.
            return getLastRowId();
          }          

          // update row count
          rowsOnPage = getRowsOnCurrentDataPage(rowBuffer);

          // start again from the top
          continue;
        }

        if(!isCurrentRowDeleted()) {
          // we found a non-deleted row, return it
          return new RowId(currentPageNumber, currentRowNumber);
        }
      }
    }
    
  }
  
}
