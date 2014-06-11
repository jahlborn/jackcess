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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;

import com.healthmarketscience.jackcess.impl.TableImpl.RowState;


/**
 * Simple un-indexed cursor.
 *
 * @author James Ahlborn
 */
public class TableScanCursor extends CursorImpl 
{
  /** first position for the TableScanCursor */
  private static final ScanPosition FIRST_SCAN_POSITION =
    new ScanPosition(RowIdImpl.FIRST_ROW_ID);
  /** last position for the TableScanCursor */
  private static final ScanPosition LAST_SCAN_POSITION =
    new ScanPosition(RowIdImpl.LAST_ROW_ID);


  /** ScanDirHandler for forward traversal */
  private final ScanDirHandler _forwardDirHandler =
    new ForwardScanDirHandler();
  /** ScanDirHandler for backward traversal */
  private final ScanDirHandler _reverseDirHandler =
    new ReverseScanDirHandler();
  /** Cursor over the pages that this table owns */
  private final UsageMap.PageCursor _ownedPagesCursor;
    
  public TableScanCursor(TableImpl table) {
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
  protected PositionImpl getRowPosition(RowIdImpl rowId) throws IOException
  {
    return new ScanPosition(rowId);
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
