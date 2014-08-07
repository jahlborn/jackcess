/*
Copyright (c) 2014 James Ahlborn

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
import java.nio.ByteBuffer;

import com.healthmarketscience.jackcess.DataType;

/**
 * ColumnImpl subclass which is used for long value data types.
 * 
 * @author James Ahlborn
 * @usage _advanced_class_
 */
class LongValueColumnImpl extends ColumnImpl 
{
  /** Holds additional info for writing long values */
  private LongValueBufferHolder _lvalBufferH;

  LongValueColumnImpl(TableImpl table, ByteBuffer buffer, int offset, 
                      int displayIndex, DataType type, byte flags)
    throws IOException
  {
    super(table, buffer, offset, displayIndex, type, flags);
  }

  @Override
  LongValueBufferHolder getLongValueBufferHolder() {
    return _lvalBufferH;
  }
    
  @Override
  public int getOwnedPageCount() {
    return ((_lvalBufferH == null) ? 0 : _lvalBufferH.getOwnedPageCount());
  }
  
  @Override
  void setUsageMaps(UsageMap ownedPages, UsageMap freeSpacePages) {
    _lvalBufferH = new UmapLongValueBufferHolder(ownedPages, freeSpacePages);
  }

  @Override
  void postTableLoadInit() throws IOException {
    if(_lvalBufferH == null) {
      _lvalBufferH = new LegacyLongValueBufferHolder();
    }
    super.postTableLoadInit();
  }
  
  /**
   * Manages a common, shared extra page for long values.  This is legacy
   * behavior from before it was understood that there were additional usage
   * maps for each columns.
   */
  private final class LegacyLongValueBufferHolder extends LongValueBufferHolder
  {
    @Override
    protected TempPageHolder getBufferHolder() {
      return getTable().getLongValueBuffer();
    }
  }

  /**
   * Manages the column usage maps for long values.
   */
  private final class UmapLongValueBufferHolder extends LongValueBufferHolder
  {
    /** Usage map of pages that this column owns */
    private final UsageMap _ownedPages;
    /** Usage map of pages that this column owns with free space on them */
    private final UsageMap _freeSpacePages;
    /** page buffer used to write "long value" data */
    private final TempPageHolder _longValueBufferH =
      TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);

    private UmapLongValueBufferHolder(UsageMap ownedPages,
                                      UsageMap freeSpacePages) {
      _ownedPages = ownedPages;
      _freeSpacePages = freeSpacePages;
    }

    @Override
    protected TempPageHolder getBufferHolder() {
      return _longValueBufferH;
    }

    @Override
    public int getOwnedPageCount() {
      return _ownedPages.getPageCount();
    }

    @Override
    protected ByteBuffer findNewPage(int dataLength) throws IOException {

      // grab last owned page and check for free space.  
      ByteBuffer newPage = TableImpl.findFreeRowSpace(      
          _ownedPages, _freeSpacePages, _longValueBufferH);
      
      if(newPage != null) {
        if(TableImpl.rowFitsOnDataPage(dataLength, newPage, getFormat())) {
          return newPage;
        }
        // discard this page and allocate a new one
        clear();
      }

      // nothing found on current pages, need new page
      newPage = super.findNewPage(dataLength);
      int pageNumber = getPageNumber();
      _ownedPages.addPageNumber(pageNumber);
      _freeSpacePages.addPageNumber(pageNumber);
      return newPage;
    }

    @Override
    public void clear() throws IOException {
      int pageNumber = getPageNumber();
      if(pageNumber != PageChannel.INVALID_PAGE_NUMBER) {
        _freeSpacePages.removePageNumber(pageNumber, true);
      }
      super.clear();
    }
  }
}
