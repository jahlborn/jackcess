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

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.RuntimeIOException;
import com.healthmarketscience.jackcess.impl.TableImpl.RowState;
import com.healthmarketscience.jackcess.util.ColumnMatcher;
import com.healthmarketscience.jackcess.util.ErrorHandler;
import com.healthmarketscience.jackcess.util.IterableBuilder;
import com.healthmarketscience.jackcess.util.SimpleColumnMatcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

  public boolean isBeforeFirst() throws IOException {
    return isAtBeginning(MOVE_FORWARD);
  }
  
  public boolean isAfterLast() throws IOException {
    return isAtBeginning(MOVE_REVERSE);
  }

  protected boolean isAtBeginning(boolean moveForward) throws IOException {
    if(getDirHandler(moveForward).getBeginningPosition().equals(_curPos)) {
      return !recheckPosition(!moveForward);
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
  
  public Iterator<Row> iterator() {
    return new RowIterator(null, true, MOVE_FORWARD);
  }

  public IterableBuilder newIterable() {
    return new IterableBuilder(this);
  }

  public Iterator<Row> iterator(IterableBuilder iterBuilder) {

    switch(iterBuilder.getType()) {
    case SIMPLE:
      return new RowIterator(iterBuilder.getColumnNames(),
                             iterBuilder.isReset(), iterBuilder.isForward());
    case COLUMN_MATCH: {
      @SuppressWarnings("unchecked")
      Map.Entry<Column,Object> matchPattern = (Map.Entry<Column,Object>)
        iterBuilder.getMatchPattern();
      return new ColumnMatchIterator(
          iterBuilder.getColumnNames(), (ColumnImpl)matchPattern.getKey(), 
          matchPattern.getValue(), iterBuilder.isReset(), 
          iterBuilder.isForward(), iterBuilder.getColumnMatcher());
    }
    case ROW_MATCH: {
      @SuppressWarnings("unchecked")
      Map<String,?> matchPattern = (Map<String,?>)
        iterBuilder.getMatchPattern();
      return new RowMatchIterator(
          iterBuilder.getColumnNames(), matchPattern,iterBuilder.isReset(), 
          iterBuilder.isForward(), iterBuilder.getColumnMatcher());
    }
    default:
      throw new RuntimeException("unknown match type " + iterBuilder.getType());
    }
  }
  
  public void deleteCurrentRow() throws IOException {
    _table.deleteRow(_rowState, _curPos.getRowId());
  }

  public Object[] updateCurrentRow(Object... row) throws IOException {
    return _table.updateRow(_rowState, _curPos.getRowId(), row);
  }

  public <M extends Map<String,Object>> M updateCurrentRowFromMap(M row) 
    throws IOException 
  {
    return _table.updateRowFromMap(_rowState, _curPos.getRowId(), row);
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
  protected boolean moveToAnotherRow(boolean moveForward)
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

  public boolean findRow(RowId rowId) throws IOException
  {
    RowIdImpl rowIdImpl = (RowIdImpl)rowId;
    PositionImpl curPos = _curPos;
    PositionImpl prevPos = _prevPos;
    boolean found = false;
    try {
      reset(MOVE_FORWARD);
      if(TableImpl.positionAtRowHeader(_rowState, rowIdImpl) == null) {
        return false;
      }
      restorePosition(getRowPosition(rowIdImpl));
      if(!isCurrentRowValid()) {
        return false;
      }
      found = true;
      return true;
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

  public boolean findFirstRow(Column columnPattern, Object valuePattern)
    throws IOException
  {
    return findFirstRow((ColumnImpl)columnPattern, valuePattern);
  } 
 
  public boolean findFirstRow(ColumnImpl columnPattern, Object valuePattern)
    throws IOException
  {
    return findAnotherRow(columnPattern, valuePattern, true, MOVE_FORWARD,
                          _columnMatcher, 
                          prepareSearchInfo(columnPattern, valuePattern));
  }

  public boolean findNextRow(Column columnPattern, Object valuePattern)
    throws IOException
  {
    return findNextRow((ColumnImpl)columnPattern, valuePattern);
  } 
 
  public boolean findNextRow(ColumnImpl columnPattern, Object valuePattern)
    throws IOException
  {
    return findAnotherRow(columnPattern, valuePattern, false, MOVE_FORWARD,
                          _columnMatcher, 
                          prepareSearchInfo(columnPattern, valuePattern));
  }
  
  protected boolean findAnotherRow(ColumnImpl columnPattern, Object valuePattern,
                                   boolean reset, boolean moveForward,
                                   ColumnMatcher columnMatcher, Object searchInfo)
    throws IOException
  {
    PositionImpl curPos = _curPos;
    PositionImpl prevPos = _prevPos;
    boolean found = false;
    try {
      if(reset) {
        reset(moveForward);
      }
      found = findAnotherRowImpl(columnPattern, valuePattern, moveForward,
                                 columnMatcher, searchInfo);
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
    return findAnotherRow(rowPattern, true, MOVE_FORWARD, _columnMatcher,
                          prepareSearchInfo(rowPattern));
  }

  public boolean findNextRow(Map<String,?> rowPattern)
    throws IOException
  {
    return findAnotherRow(rowPattern, false, MOVE_FORWARD, _columnMatcher,
                          prepareSearchInfo(rowPattern));
  }

  protected boolean findAnotherRow(Map<String,?> rowPattern, boolean reset,
                                   boolean moveForward, 
                                   ColumnMatcher columnMatcher, Object searchInfo)
    throws IOException
  {
    PositionImpl curPos = _curPos;
    PositionImpl prevPos = _prevPos;
    boolean found = false;
    try {
      if(reset) {
        reset(moveForward);
      }
      found = findAnotherRowImpl(rowPattern, moveForward, columnMatcher,        
                                 searchInfo);
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
    return currentRowMatchesImpl(columnPattern, valuePattern, _columnMatcher);
  }
  
  protected boolean currentRowMatchesImpl(ColumnImpl columnPattern, 
                                          Object valuePattern,
                                          ColumnMatcher columnMatcher)
    throws IOException
  {
    return columnMatcher.matches(getTable(), columnPattern.getName(),
                                 valuePattern,
                                 getCurrentRowValue(columnPattern));
  }
  
  public boolean currentRowMatches(Map<String,?> rowPattern)
    throws IOException
  {
    return currentRowMatchesImpl(rowPattern, _columnMatcher);
  }

  protected boolean currentRowMatchesImpl(Map<String,?> rowPattern,
                                          ColumnMatcher columnMatcher)
    throws IOException
  {
    Row row = getCurrentRow(rowPattern.keySet());

    if(rowPattern.size() != row.size()) {
      return false;
    }

    for(Map.Entry<String,Object> e : row.entrySet()) {
      String columnName = e.getKey();
      if(!columnMatcher.matches(getTable(), columnName,
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
  protected boolean findAnotherRowImpl(
      ColumnImpl columnPattern, Object valuePattern, boolean moveForward,
      ColumnMatcher columnMatcher, Object searchInfo)
    throws IOException
  {
    while(moveToAnotherRow(moveForward)) {
      if(currentRowMatchesImpl(columnPattern, valuePattern, columnMatcher)) {
        return true;
      }
      if(!keepSearching(columnMatcher, searchInfo)) {
        break;
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
  protected boolean findAnotherRowImpl(Map<String,?> rowPattern, 
                                       boolean moveForward, 
                                       ColumnMatcher columnMatcher,
                                       Object searchInfo)
    throws IOException
  {
    while(moveToAnotherRow(moveForward)) {
      if(currentRowMatchesImpl(rowPattern, columnMatcher)) {
        return true;
      }
      if(!keepSearching(columnMatcher, searchInfo)) {
        break;
      }
    }
    return false;
  }  

  /**
   * Called before a search commences to allow for search specific data to be
   * generated (which is cached for re-use by the iterators).
   */
  protected Object prepareSearchInfo(ColumnImpl columnPattern, Object valuePattern)
  {
    return null;
  }

  /**
   * Called before a search commences to allow for search specific data to be
   * generated (which is cached for re-use by the iterators).
   */
  protected Object prepareSearchInfo(Map<String,?> rowPattern)
  {
    return null;
  }

  /**
   * Called by findAnotherRowImpl to determine if the search should continue
   * after finding a row which does not match the current pattern.
   */
  protected boolean keepSearching(ColumnMatcher columnMatcher, 
                                  Object searchInfo) 
    throws IOException
  {
    return true;
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

  /**
   * Returns {@code true} of the current row is valid, {@code false} otherwise.
   */
  protected boolean isCurrentRowValid() throws IOException {
    return(_curPos.getRowId().isValid() && !isCurrentRowDeleted() &&
           !isBeforeFirst() && !isAfterLast());
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + " CurPosition " + _curPos +
      ", PrevPosition " + _prevPos;
  }

  /**
   * Returns the appropriate position information for the given row (which is
   * the current row and is valid).
   */
  protected abstract PositionImpl getRowPosition(RowIdImpl rowId) 
    throws IOException;
    
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
  protected abstract class BaseIterator implements Iterator<Row>
  {
    protected final Collection<String> _columnNames;
    protected final boolean _moveForward;
    protected final ColumnMatcher _colMatcher;
    protected Boolean _hasNext;
    protected boolean _validRow;
    
    protected BaseIterator(Collection<String> columnNames,
                           boolean reset, boolean moveForward,
                           ColumnMatcher columnMatcher)
    {
      _columnNames = columnNames;
      _moveForward = moveForward;
      _colMatcher = ((columnMatcher != null) ? columnMatcher : _columnMatcher);
      try {
        if(reset) {
          reset(_moveForward);
        } else if(isCurrentRowValid()) {
          _hasNext = _validRow = true;
        }
      } catch(IOException e) {
        throw new RuntimeIOException(e);
      }
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
    private RowIterator(Collection<String> columnNames, boolean reset,
                        boolean moveForward)
    {
      super(columnNames, reset, moveForward, null);
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
    private final Object _searchInfo;
    
    private ColumnMatchIterator(Collection<String> columnNames,
                                ColumnImpl columnPattern, Object valuePattern,
                                boolean reset, boolean moveForward,
                                ColumnMatcher columnMatcher)
    {
      super(columnNames, reset, moveForward, columnMatcher);
      _columnPattern = columnPattern;
      _valuePattern = valuePattern;
      _searchInfo = prepareSearchInfo(columnPattern, valuePattern);
    }

    @Override
    protected boolean findNext() throws IOException {
      return findAnotherRow(_columnPattern, _valuePattern, false, _moveForward,
                            _colMatcher, _searchInfo);
    }
  }


  /**
   * Row iterator for this cursor, modifiable.
   */
  private final class RowMatchIterator extends BaseIterator
  {
    private final Map<String,?> _rowPattern;
    private final Object _searchInfo;
    
    private RowMatchIterator(Collection<String> columnNames,
                             Map<String,?> rowPattern,
                             boolean reset, boolean moveForward,
                             ColumnMatcher columnMatcher)
    {
      super(columnNames, reset, moveForward, columnMatcher);
      _rowPattern = rowPattern;
      _searchInfo = prepareSearchInfo(rowPattern);
    }

    @Override
    protected boolean findNext() throws IOException {
      return findAnotherRow(_rowPattern, false, _moveForward, _colMatcher,
                            _searchInfo);
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
   * Identifier for a cursor.  Will be equal to any other cursor of the same
   * type for the same table.  Primarily used to check the validity of a
   * Savepoint.
   */
  protected static final class IdImpl implements Id
  {
    private final int _tablePageNumber;
    private final int _indexNumber;

    protected IdImpl(TableImpl table, IndexImpl index) {
      _tablePageNumber = table.getTableDefPageNumber();
      _indexNumber = ((index != null) ? index.getIndexNumber() : -1);
    }

    @Override
    public int hashCode() {
      return _tablePageNumber;
    }

    @Override
    public boolean equals(Object o) {
      return((this == o) ||
             ((o != null) && (getClass() == o.getClass()) &&
              (_tablePageNumber == ((IdImpl)o)._tablePageNumber) &&
              (_indexNumber == ((IdImpl)o)._indexNumber)));
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " " + _tablePageNumber + ":" + _indexNumber;
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
  
}
