/*
Copyright (c) 2005 Health Market Science, Inc.

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
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Describes which database pages a particular table uses
 * @author Tim McCune
 */
public class UsageMap
{
  private static final Log LOG = LogFactory.getLog(UsageMap.class);
  
  /** Inline map type */
  public static final byte MAP_TYPE_INLINE = 0x0;
  /** Reference map type, for maps that are too large to fit inline */
  public static final byte MAP_TYPE_REFERENCE = 0x1;

  /** bit index value for an invalid page number */
  private static final int INVALID_BIT_INDEX = -1;
  
  /** owning database */
  private final Database _database;
  /** Page number of the map table declaration */
  private final int _tablePageNum;
  /** Offset of the data page at which the usage map data starts */
  private int _startOffset;
  /** Offset of the data page at which the usage map declaration starts */
  private final short _rowStart;
  /** First page that this usage map applies to */
  private int _startPage;
  /** Last page that this usage map applies to */
  private int _endPage;
  /** bits representing page numbers used, offset from _startPage */
  private BitSet _pageNumbers = new BitSet();
  /** Buffer that contains the usage map table declaration page */
  private final ByteBuffer _tableBuffer;
  /** modification count on the usage map, used to keep the cursors in
      sync */
  private int _modCount;
  /** the current handler implementation for reading/writing the specific
      usage map type.  note, this may change over time. */
  private Handler _handler;

  /** Error message prefix used when map type is unrecognized. */
  static final String MSG_PREFIX_UNRECOGNIZED_MAP = "Unrecognized map type: ";

    /**
   * @param database database that contains this usage map
   * @param tableBuffer Buffer that contains this map's declaration
   * @param pageNum Page number that this usage map is contained in
   * @param rowStart Offset at which the declaration starts in the buffer
   */
  private UsageMap(Database database, ByteBuffer tableBuffer,
                   int pageNum, short rowStart)
    throws IOException
  {
    _database = database;
    _tableBuffer = tableBuffer;
    _tablePageNum = pageNum;
    _rowStart = rowStart;
    _tableBuffer.position(_rowStart + getFormat().OFFSET_USAGE_MAP_START);
    _startOffset = _tableBuffer.position();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Usage map block:\n" + ByteUtil.toHexString(_tableBuffer, _rowStart,
          tableBuffer.limit() - _rowStart));
    }
  }

  public Database getDatabase() {
    return _database;
  }
  
  public JetFormat getFormat() {
    return getDatabase().getFormat();
  }

  public PageChannel getPageChannel() {
    return getDatabase().getPageChannel();
  }
  
  /**
   * @param database database that contains this usage map
   * @param pageNum Page number that this usage map is contained in
   * @param rowNum Number of the row on the page that contains this usage map
   * @return Either an InlineUsageMap or a ReferenceUsageMap, depending on
   *         which type of map is found
   */
  public static UsageMap read(Database database, int pageNum,
                              int rowNum, boolean assumeOutOfRangeBitsOn)
    throws IOException
  {
    JetFormat format = database.getFormat();
    PageChannel pageChannel = database.getPageChannel();
    ByteBuffer tableBuffer = pageChannel.createPageBuffer();
    pageChannel.readPage(tableBuffer, pageNum);
    short rowStart = Table.findRowStart(tableBuffer, rowNum, format);
    int rowEnd = Table.findRowEnd(tableBuffer, rowNum, format);
    tableBuffer.limit(rowEnd);    
    byte mapType = tableBuffer.get(rowStart);
    UsageMap rtn = new UsageMap(database, tableBuffer, pageNum, rowStart);
    rtn.initHandler(mapType, assumeOutOfRangeBitsOn);
    return rtn;
  }

  private void initHandler(byte mapType, boolean assumeOutOfRangeBitsOn)
    throws IOException
  {
    if (mapType == MAP_TYPE_INLINE) {
      _handler = new InlineHandler(assumeOutOfRangeBitsOn);
    } else if (mapType == MAP_TYPE_REFERENCE) {
      _handler = new ReferenceHandler();
    } else {
      throw new IOException(MSG_PREFIX_UNRECOGNIZED_MAP + mapType);
    }
  }
  
  public PageCursor cursor() {
    return new PageCursor();
  }

  public int getPageCount() {
    return _pageNumbers.cardinality();
  }
  
  protected short getRowStart() {
    return _rowStart;
  }

  protected int getRowEnd() {
    return getTableBuffer().limit();
  }

  protected void setStartOffset(int startOffset) {
    _startOffset = startOffset;
  }
  
  protected int getStartOffset() {
    return _startOffset;
  }
  
  protected ByteBuffer getTableBuffer() {
    return _tableBuffer;
  }
  
  protected int getTablePageNumber() {
    return _tablePageNum;
  }
  
  protected int getStartPage() {
    return _startPage;
  }
    
  protected int getEndPage() {
    return _endPage;
  }
    
  protected BitSet getPageNumbers() {
    return _pageNumbers;
  }

  protected void setPageRange(int newStartPage, int newEndPage) {
    _startPage = newStartPage;
    _endPage = newEndPage;
  }

  protected boolean isPageWithinRange(int pageNumber)
  {
    return((pageNumber >= _startPage) && (pageNumber < _endPage));
  }

  protected int getFirstPageNumber() {
    return bitIndexToPageNumber(getNextBitIndex(-1), RowId.LAST_PAGE_NUMBER);
  }

  protected int getNextPageNumber(int curPage) {
    return bitIndexToPageNumber(
        getNextBitIndex(pageNumberToBitIndex(curPage)),
        RowId.LAST_PAGE_NUMBER);
  }    
  
  protected int getNextBitIndex(int curIndex) {
    return _pageNumbers.nextSetBit(curIndex + 1);
  }    
  
  protected int getLastPageNumber() {
    return bitIndexToPageNumber(getPrevBitIndex(_pageNumbers.length()),
                                RowId.FIRST_PAGE_NUMBER);
  }

  protected int getPrevPageNumber(int curPage) {
    return bitIndexToPageNumber(
        getPrevBitIndex(pageNumberToBitIndex(curPage)),
        RowId.FIRST_PAGE_NUMBER);
  }    
  
  protected int getPrevBitIndex(int curIndex) {
    --curIndex;
    while((curIndex >= 0) && !_pageNumbers.get(curIndex)) {
      --curIndex;
    }
    return curIndex;
  }    
  
  protected int bitIndexToPageNumber(int bitIndex,
                                     int invalidPageNumber) {
    return((bitIndex >= 0) ? (_startPage + bitIndex) : invalidPageNumber);
  }

  protected int pageNumberToBitIndex(int pageNumber) {
    return((pageNumber >= 0) ? (pageNumber - _startPage) :
           INVALID_BIT_INDEX);
  }

  protected void clearTableAndPages()
  {
    // reset some values
    _pageNumbers.clear();
    _startPage = 0;
    _endPage = 0;
    ++_modCount;
    
    // clear out the table data (everything except map type)
    int tableStart = getRowStart() + 1;
    int tableEnd = getRowEnd();
    ByteUtil.clearRange(_tableBuffer, tableStart, tableEnd);
  }
  
  protected void writeTable()
    throws IOException
  {
    // note, we only want to write the row data with which we are working
    getPageChannel().writePage(_tableBuffer, _tablePageNum, _rowStart);
  }
  
  /**
   * Read in the page numbers in this inline map
   */
  protected void processMap(ByteBuffer buffer, int bufferStartPage)
  {
    int byteCount = 0;
    while (buffer.hasRemaining()) {
      byte b = buffer.get();
      if(b != (byte)0) {
        for (int i = 0; i < 8; i++) {
          if ((b & (1 << i)) != 0) {
            int pageNumberOffset = (byteCount * 8 + i) + bufferStartPage;
            int pageNumber = bitIndexToPageNumber(
                pageNumberOffset,
                PageChannel.INVALID_PAGE_NUMBER);
            if(!isPageWithinRange(pageNumber)) {
              throw new IllegalStateException(
                  "found page number " + pageNumber
                  + " in usage map outside of expected range " +
                  _startPage + " to " + _endPage);
            }
            _pageNumbers.set(pageNumberOffset);
          }
        }
      }
      byteCount++;
    }
  }

  /**
   * Determines if the given page number is contained in this map.
   */
  public boolean containsPageNumber(int pageNumber) {
    return _handler.containsPageNumber(pageNumber);
  }
  
  /**
   * Add a page number to this usage map
   */
  public void addPageNumber(int pageNumber) throws IOException {
    ++_modCount;
    _handler.addOrRemovePageNumber(pageNumber, true, false);
  }
  
  /**
   * Remove a page number from this usage map
   */
  public void removePageNumber(int pageNumber) throws IOException {
    removePageNumber(pageNumber, false);
  }

  /**
   * Remove a page number from this usage map
   */
  protected void removePageNumber(int pageNumber, boolean force) 
    throws IOException 
  {
    ++_modCount;
    _handler.addOrRemovePageNumber(pageNumber, false, force);
  }
  
  protected void updateMap(int absolutePageNumber,
                           int bufferRelativePageNumber,
                           ByteBuffer buffer, boolean add, boolean force)
    throws IOException
  {
    //Find the byte to which to apply the bitmask and create the bitmask
    int offset = bufferRelativePageNumber / 8;
    int bitmask = 1 << (bufferRelativePageNumber % 8);
    byte b = buffer.get(_startOffset + offset);

    // check current value for this page number
    int pageNumberOffset = pageNumberToBitIndex(absolutePageNumber);
    boolean isOn = _pageNumbers.get(pageNumberOffset);
    if((isOn == add) && !force) {
      throw new IOException("Page number " + absolutePageNumber + " already " +
                            ((add) ? "added to" : "removed from") +
                            " usage map, expected range " +
                            _startPage + " to " + _endPage);
    }
    
    //Apply the bitmask
    if (add) {
      b |= bitmask;
      _pageNumbers.set(pageNumberOffset);
    } else {
      b &= ~bitmask;
      _pageNumbers.clear(pageNumberOffset);
    }
    buffer.put(_startOffset + offset, b);
  }

  /**
   * Promotes and inline usage map to a reference usage map.
   */
  private void promoteInlineHandlerToReferenceHandler(int newPageNumber)
    throws IOException
  {
    // copy current page number info to new references and then clear old
    int oldStartPage = _startPage;
    BitSet oldPageNumbers = (BitSet)_pageNumbers.clone();

    // clear out the main table (inline usage map data and start page)
    clearTableAndPages();
    
    // set the new map type
    _tableBuffer.put(getRowStart(), MAP_TYPE_REFERENCE);

    // write the new table data
    writeTable();
    
    // set new handler
    _handler = new ReferenceHandler();

    // update new handler with old data
    reAddPages(oldStartPage, oldPageNumbers, newPageNumber);
  }

  private void reAddPages(int oldStartPage, BitSet oldPageNumbers,
                          int newPageNumber)
    throws IOException
  {
    // add all the old pages back in
    for(int i = oldPageNumbers.nextSetBit(0); i >= 0;
        i = oldPageNumbers.nextSetBit(i + 1)) {
      addPageNumber(oldStartPage + i);
    }

    if(newPageNumber > PageChannel.INVALID_PAGE_NUMBER) {
      // and then add the new page
      addPageNumber(newPageNumber);
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(
        "(" + _handler.getClass().getSimpleName() +
        ") page numbers (range " + _startPage + " " + _endPage + "): [");

    PageCursor pCursor = cursor();
    int curRangeStart = Integer.MIN_VALUE;
    int prevPage = Integer.MIN_VALUE;
    while(true) {
      int nextPage = pCursor.getNextPage();
      if(nextPage < 0) {
        break;
      }

      if(nextPage != (prevPage + 1)) {
        if(prevPage >= 0) {
          rangeToString(builder, curRangeStart, prevPage);
        }
        curRangeStart = nextPage;
      }
      prevPage = nextPage;
    }
    if(prevPage >= 0) {
      rangeToString(builder, curRangeStart, prevPage);
    }

    builder.append("]");
    return builder.toString();
  }

  private static void rangeToString(StringBuilder builder, int rangeStart,
                                    int rangeEnd)
  {
    builder.append(rangeStart);
    if(rangeEnd > rangeStart) {
      builder.append("-").append(rangeEnd);
    }
    builder.append(", ");
  }
  
  private abstract class Handler
  {
    protected Handler() {
    }

    public boolean containsPageNumber(int pageNumber) {
      return(isPageWithinRange(pageNumber) &&
             getPageNumbers().get(pageNumberToBitIndex(pageNumber)));
    }
    
    /**
     * @param pageNumber Page number to add or remove from this map
     * @param add True to add it, false to remove it
     * @param force true to force add/remove and ignore certain inconsistencies
     */
    public abstract void addOrRemovePageNumber(int pageNumber, boolean add,
                                               boolean force)
      throws IOException;
  }

  /**
   * Usage map whose map is written inline in the same page.  For Jet4, this
   * type of map can usually contains a maximum of 512 pages.  Free space maps
   * are always inline, used space maps may be inline or reference.  It has a
   * start page, which all page numbers in its map are calculated as starting
   * from.
   * @author Tim McCune
   */
  private class InlineHandler extends Handler
  {
    private final boolean _assumeOutOfRangeBitsOn;
    private final int _maxInlinePages;
    
    private InlineHandler(boolean assumeOutOfRangeBitsOn)
      throws IOException
    {
      _assumeOutOfRangeBitsOn = assumeOutOfRangeBitsOn;
      _maxInlinePages = (getInlineDataEnd() - getInlineDataStart()) * 8;
      int startPage = getTableBuffer().getInt(getRowStart() + 1);
      setInlinePageRange(startPage);
      processMap(getTableBuffer(), 0);
    }

    private int getMaxInlinePages() {
      return _maxInlinePages;
    }

    private int getInlineDataStart() {
      return getRowStart() + getFormat().OFFSET_USAGE_MAP_START;
    }

    private int getInlineDataEnd() {
      return getRowEnd();
    }
    
    /**
     * Sets the page range for an inline usage map starting from the given
     * page.
     */
    private void setInlinePageRange(int startPage) {
      setPageRange(startPage, startPage + getMaxInlinePages());
    }      

    @Override
    public boolean containsPageNumber(int pageNumber) {
      return(super.containsPageNumber(pageNumber) ||
             (_assumeOutOfRangeBitsOn && (pageNumber >= 0) &&
              !isPageWithinRange(pageNumber)));
    }
    
    @Override
    public void addOrRemovePageNumber(int pageNumber, boolean add,
                                      boolean force)
      throws IOException
    {
      if(isPageWithinRange(pageNumber)) {

        // easy enough, just update the inline data
        int bufferRelativePageNumber = pageNumberToBitIndex(pageNumber);
        updateMap(pageNumber, bufferRelativePageNumber, getTableBuffer(), add,
                  force);
        // Write the updated map back to disk
        writeTable();
        
      } else {

        // uh-oh, we've split our britches.  what now?  determine what our
        // status is
        int firstPage = getFirstPageNumber();
        int lastPage = getLastPageNumber();
        
        if(add) {

          // we can ignore out-of-range page addition if we are already
          // assuming out-of-range bits are "on".  Note, we are leaving small
          // holes in the database here (leaving behind some free pages), but
          // it's not the end of the world.
          if(!_assumeOutOfRangeBitsOn) {
            
            // we are adding, can we shift the bits and stay inline?
            if(firstPage <= PageChannel.INVALID_PAGE_NUMBER) {
              // no pages currently
              firstPage = pageNumber;
              lastPage = pageNumber;
            } else if(pageNumber > lastPage) {
              lastPage = pageNumber;
            } else {
              firstPage = pageNumber;
            }
            if((lastPage - firstPage + 1) < getMaxInlinePages()) {

              // we can still fit within an inline map
              moveToNewStartPage(firstPage, pageNumber);
            
            } else {
              // not going to happen, need to promote the usage map to a
              // reference map
              promoteInlineHandlerToReferenceHandler(pageNumber);
            }
          }
        } else {

          // we are removing, what does that mean?
          if(_assumeOutOfRangeBitsOn) {

            // we are using an inline map and assuming that anything not
            // within the current range is "on".  so, if we attempt to set a
            // bit which is before the current page, ignore it, we are not
            // going back for it.
            if((firstPage <= PageChannel.INVALID_PAGE_NUMBER) ||
               (pageNumber > lastPage)) {

              // move to new start page, filling in as we move
              moveToNewStartPageForRemove(firstPage, pageNumber);
              
            }
            
          } else if(!force) {

            // this should not happen, we are removing a page which is not in
            // the map
            throw new IOException("Page number " + pageNumber +
                                  " already removed from usage map" +
                                  ", expected range " +
                                  _startPage + " to " + _endPage);
          }
        }

      }
    }

    /**
     * Shifts the inline usage map so that it now starts with the given page.
     * @param newStartPage new page at which to start
     * @param newPageNumber optional page number to add once the map has been
     *                      shifted to the new start page
     */
    private void moveToNewStartPage(int newStartPage, int newPageNumber)
      throws IOException
    {
      int oldStartPage = getStartPage();
      BitSet oldPageNumbers = (BitSet)getPageNumbers().clone();

      // clear out the main table (inline usage map data and start page)
      clearTableAndPages();

      // write new start page
      ByteBuffer tableBuffer = getTableBuffer();
      tableBuffer.position(getRowStart() + 1);
      tableBuffer.putInt(newStartPage);

      // write the new table data
      writeTable();

      // set new page range
      setInlinePageRange(newStartPage);

      // put the pages back in
      reAddPages(oldStartPage, oldPageNumbers, newPageNumber);
    }

    /**
     * Shifts the inline usage map so that it now starts with the given
     * firstPage (if valid), otherwise the newPageNumber.  Any page numbers
     * added to the end of the usage map are set to "on".
     * @param firstPage current first used page
     * @param newPageNumber page number to remove once the map has been
     *                      shifted to the new start page
     */
    private void moveToNewStartPageForRemove(int firstPage, int newPageNumber)
      throws IOException
    {
      int oldEndPage = getEndPage();
      int newStartPage = 
        ((firstPage <= PageChannel.INVALID_PAGE_NUMBER) ? newPageNumber :
         // just shift a little and discard any initial unused pages.
         (newPageNumber - (getMaxInlinePages() / 2)));

      // move the current data
      moveToNewStartPage(newStartPage, PageChannel.INVALID_PAGE_NUMBER);

      if(firstPage <= PageChannel.INVALID_PAGE_NUMBER) {

        // this is the common case where we left everything behind
        ByteUtil.fillRange(_tableBuffer, getInlineDataStart(),
                           getInlineDataEnd());

        // write out the updated table
        writeTable();

        // "add" all the page numbers
        getPageNumbers().set(0, getMaxInlinePages());

      } else {

        // add every new page manually
        for(int i = oldEndPage; i < getEndPage(); ++i) {
          addPageNumber(i);
        }
      }

      // lastly, remove the new page
      removePageNumber(newPageNumber);
    }
  }

  /**
   * Usage map whose map is written across one or more entire separate pages
   * of page type USAGE_MAP.  For Jet4, this type of map can contain 32736
   * pages per reference page, and a maximum of 17 reference map pages for a
   * total maximum of 556512 pages (2 GB).
   * @author Tim McCune
   */
  private class ReferenceHandler extends Handler
  {
    /** Buffer that contains the current reference map page */ 
    private final TempPageHolder _mapPageHolder =
      TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);
  
    private ReferenceHandler()
      throws IOException
    {
      int numUsagePages = (getRowEnd() - getRowStart() - 1) / 4;
      setStartOffset(getFormat().OFFSET_USAGE_MAP_PAGE_DATA);
      setPageRange(0, (numUsagePages * getMaxPagesPerUsagePage()));
      
      // there is no "start page" for a reference usage map, so we get an
      // extra page reference on top of the number of page references that fit
      // in the table
      for (int i = 0; i < numUsagePages; i++) {
        int mapPageNum = getTableBuffer().getInt(
            calculateMapPagePointerOffset(i));
        if (mapPageNum > 0) {
          ByteBuffer mapPageBuffer =
            _mapPageHolder.setPage(getPageChannel(), mapPageNum);
          byte pageType = mapPageBuffer.get();
          if (pageType != PageTypes.USAGE_MAP) {
            throw new IOException("Looking for usage map at page " +
                                  mapPageNum + ", but page type is " +
                                  pageType);
          }
          mapPageBuffer.position(getFormat().OFFSET_USAGE_MAP_PAGE_DATA);
          processMap(mapPageBuffer, (getMaxPagesPerUsagePage() * i));
        }
      }
    }

    private int getMaxPagesPerUsagePage() {
      return((getFormat().PAGE_SIZE - getFormat().OFFSET_USAGE_MAP_PAGE_DATA)
             * 8);
    }
        
    @Override
    public void addOrRemovePageNumber(int pageNumber, boolean add,
                                      boolean force)
      throws IOException
    {
      if(!isPageWithinRange(pageNumber)) {
        if(force) {
          return;
        }
        throw new IOException("Page number " + pageNumber +
                              " is out of supported range");
      }
      int pageIndex = (pageNumber / getMaxPagesPerUsagePage());
      int mapPageNum = getTableBuffer().getInt(
          calculateMapPagePointerOffset(pageIndex));
      ByteBuffer mapPageBuffer = null;
      if(mapPageNum > 0) {
        mapPageBuffer = _mapPageHolder.setPage(getPageChannel(), mapPageNum);
      } else {
        // Need to create a new usage map page
        mapPageBuffer = createNewUsageMapPage(pageIndex);
        mapPageNum = _mapPageHolder.getPageNumber();
      }
      updateMap(pageNumber,
                (pageNumber - (getMaxPagesPerUsagePage() * pageIndex)),
                mapPageBuffer, add, force);
      getPageChannel().writePage(mapPageBuffer, mapPageNum);
    }
  
    /**
     * Create a new usage map page and update the map declaration with a
     * pointer to it.
     * @param pageIndex Index of the page reference within the map declaration 
     */
    private ByteBuffer createNewUsageMapPage(int pageIndex) throws IOException
    {
      ByteBuffer mapPageBuffer = _mapPageHolder.setNewPage(getPageChannel());
      mapPageBuffer.put(PageTypes.USAGE_MAP);
      mapPageBuffer.put((byte) 0x01);  //Unknown
      mapPageBuffer.putShort((short) 0); //Unknown
      int mapPageNum = _mapPageHolder.getPageNumber();
      getTableBuffer().putInt(calculateMapPagePointerOffset(pageIndex),
                              mapPageNum);
      writeTable();
      return mapPageBuffer;
    }
  
    private int calculateMapPagePointerOffset(int pageIndex) {
      return getRowStart() + getFormat().OFFSET_REFERENCE_MAP_PAGE_NUMBERS +
        (pageIndex * 4);
    }
  }

  /**
   * Utility class to traverse over the pages in the UsageMap.  Remains valid
   * in the face of usage map modifications.
   */
  public final class PageCursor
  {
    /** handler for moving the page cursor forward */
    private final DirHandler _forwardDirHandler = new ForwardDirHandler();
    /** handler for moving the page cursor backward */
    private final DirHandler _reverseDirHandler = new ReverseDirHandler();
    /** the current used page number */
    private int _curPageNumber;
    /** the previous used page number */
    private int _prevPageNumber;
    /** the last read modification count on the UsageMap.  we track this so
        that the cursor can detect updates to the usage map while traversing
        and act accordingly */
    private int _lastModCount;

    private PageCursor() {
      reset();
    }

    public UsageMap getUsageMap() {
      return UsageMap.this;
    }
    
    /**
     * Returns the DirHandler for the given direction
     */
    private DirHandler getDirHandler(boolean moveForward) {
      return (moveForward ? _forwardDirHandler : _reverseDirHandler);
    }

    /**
     * Returns {@code true} if this cursor is up-to-date with respect to its
     * usage map.
     */
    public boolean isUpToDate() {
      return(UsageMap.this._modCount == _lastModCount);
    }    

    /**
     * @return valid page number if there was another page to read,
     *         {@link RowId#LAST_PAGE_NUMBER} otherwise
     */
    public int getNextPage() {
      return getAnotherPage(Cursor.MOVE_FORWARD);
    }

    /**
     * @return valid page number if there was another page to read,
     *         {@link RowId#FIRST_PAGE_NUMBER} otherwise
     */
    public int getPreviousPage() {
      return getAnotherPage(Cursor.MOVE_REVERSE);
    }

    /**
     * Gets another page in the given direction, returning the new page.
     */
    private int getAnotherPage(boolean moveForward) {
      DirHandler handler = getDirHandler(moveForward);
      if(_curPageNumber == handler.getEndPageNumber()) {
        if(!isUpToDate()) {
          restorePosition(_prevPageNumber);
          // drop through and retry moving to another page
        } else {
          // at end, no more
          return _curPageNumber;
        }
      }

      checkForModification();
      
      _prevPageNumber = _curPageNumber;
      _curPageNumber = handler.getAnotherPageNumber(_curPageNumber);
      return _curPageNumber;
    }

    /**
     * After calling this method, getNextPage will return the first page in
     * the map
     */
    public void reset() {
      beforeFirst();
    }

    /**
     * After calling this method, {@link #getNextPage} will return the first
     * page in the map
     */
    public void beforeFirst() {
      reset(Cursor.MOVE_FORWARD);
    }

    /**
     * After calling this method, {@link #getPreviousPage} will return the
     * last page in the map
     */
    public void afterLast() {
      reset(Cursor.MOVE_REVERSE);
    }

    /**
     * Resets this page cursor for traversing the given direction.
     */
    protected void reset(boolean moveForward) {
      _curPageNumber = getDirHandler(moveForward).getBeginningPageNumber();
      _prevPageNumber = _curPageNumber;
      _lastModCount = UsageMap.this._modCount;
    }

    /**
     * Restores a current position for the cursor (current position becomes
     * previous position).
     */
    private void restorePosition(int curPageNumber)
    {
      restorePosition(curPageNumber, _curPageNumber);
    }
    
    /**
     * Restores a current and previous position for the cursor.
     */
    protected void restorePosition(int curPageNumber, int prevPageNumber)
    {
      if((curPageNumber != _curPageNumber) ||
         (prevPageNumber != _prevPageNumber))
      {
        _prevPageNumber = updatePosition(prevPageNumber);
        _curPageNumber = updatePosition(curPageNumber);
        _lastModCount = UsageMap.this._modCount;
      } else {
        checkForModification();
      }
    }

    /**
     * Checks the usage map for modifications an updates state accordingly.
     */
    private void checkForModification() {
      if(!isUpToDate()) {
        _prevPageNumber = updatePosition(_prevPageNumber);
        _curPageNumber = updatePosition(_curPageNumber);
        _lastModCount = UsageMap.this._modCount;
      }
    }

    private int updatePosition(int pageNumber) {
      if(pageNumber < UsageMap.this.getFirstPageNumber()) {
        pageNumber = RowId.FIRST_PAGE_NUMBER;
      } else if(pageNumber > UsageMap.this.getLastPageNumber()) {
        pageNumber = RowId.LAST_PAGE_NUMBER;
      }
      return pageNumber;
    }
    
    @Override
    public String toString() {
      return getClass().getSimpleName() + " CurPosition " + _curPageNumber +
        ", PrevPosition " + _prevPageNumber;
    }
    
    
    /**
     * Handles moving the cursor in a given direction.  Separates cursor
     * logic from value storage.
     */
    private abstract class DirHandler {
      public abstract int getAnotherPageNumber(int curPageNumber);
      public abstract int getBeginningPageNumber();
      public abstract int getEndPageNumber();
    }
        
    /**
     * Handles moving the cursor forward.
     */
    private final class ForwardDirHandler extends DirHandler {
      @Override
      public int getAnotherPageNumber(int curPageNumber) {
        if(curPageNumber == getBeginningPageNumber()) {
          return UsageMap.this.getFirstPageNumber();
        }
        return UsageMap.this.getNextPageNumber(curPageNumber);
      }
      @Override
      public int getBeginningPageNumber() {
        return RowId.FIRST_PAGE_NUMBER;
      }
      @Override
      public int getEndPageNumber() {
        return RowId.LAST_PAGE_NUMBER;
      }
    }
        
    /**
     * Handles moving the cursor backward.
     */
    private final class ReverseDirHandler extends DirHandler {
      @Override
      public int getAnotherPageNumber(int curPageNumber) {
        if(curPageNumber == getBeginningPageNumber()) {
          return UsageMap.this.getLastPageNumber();
        }
        return UsageMap.this.getPrevPageNumber(curPageNumber);
      }
      @Override
      public int getBeginningPageNumber() {
        return RowId.LAST_PAGE_NUMBER;
      }
      @Override
      public int getEndPageNumber() {
        return RowId.FIRST_PAGE_NUMBER;
      }
    }
        
  }  
  
}
