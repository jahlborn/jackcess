/*
Copyright (c) 2005 Health Market Science, Inc.

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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.healthmarketscience.jackcess.util.ToStringBuilder;


/**
 * Describes which database pages a particular table uses
 * @author Tim McCune
 */
public class UsageMap
{
  /** Inline map type */
  public static final byte MAP_TYPE_INLINE = 0x0;
  /** Reference map type, for maps that are too large to fit inline */
  public static final byte MAP_TYPE_REFERENCE = 0x1;

  /** bit index value for an invalid page number */
  private static final int INVALID_BIT_INDEX = -1;

  /** owning database */
  private final DatabaseImpl _database;
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
  private UsageMap(DatabaseImpl database, ByteBuffer tableBuffer,
                   int pageNum, short rowStart)
    throws IOException
  {
    _database = database;
    _tableBuffer = tableBuffer;
    _tablePageNum = pageNum;
    _rowStart = rowStart;
    _tableBuffer.position(_rowStart + getFormat().OFFSET_USAGE_MAP_START);
    _startOffset = _tableBuffer.position();
  }

  public DatabaseImpl getDatabase() {
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
   * @param buf buffer which contains the usage map row info
   * @return Either an InlineUsageMap or a ReferenceUsageMap, depending on
   *         which type of map is found
   */
  public static UsageMap read(DatabaseImpl database, ByteBuffer buf)
    throws IOException
  {
    int umapRowNum = buf.get();
    int umapPageNum = ByteUtil.get3ByteInt(buf);
    return read(database, umapPageNum, umapRowNum, false);
  }

  /**
   * @param database database that contains this usage map
   * @param pageNum Page number that this usage map is contained in
   * @param rowNum Number of the row on the page that contains this usage map
   * @param isGlobal whether or not we are reading the "global" usage map
   * @return Either an InlineUsageMap or a ReferenceUsageMap, depending on
   *         which type of map is found
   */
  static UsageMap read(DatabaseImpl database, int pageNum,
                       int rowNum, boolean isGlobal)
    throws IOException
  {
    if(pageNum <= 0) {
      // usage maps will never appear on page 0 (or less)
      throw new IllegalStateException("Invalid usage map page number " + pageNum);
    }

    JetFormat format = database.getFormat();
    PageChannel pageChannel = database.getPageChannel();
    ByteBuffer tableBuffer = pageChannel.createPageBuffer();
    pageChannel.readPage(tableBuffer, pageNum);
    short rowStart = TableImpl.findRowStart(tableBuffer, rowNum, format);
    int rowEnd = TableImpl.findRowEnd(tableBuffer, rowNum, format);
    tableBuffer.limit(rowEnd);
    byte mapType = tableBuffer.get(rowStart);
    UsageMap rtn = new UsageMap(database, tableBuffer, pageNum, rowStart);
    rtn.initHandler(mapType, isGlobal);
    return rtn;
  }

  private void initHandler(byte mapType, boolean isGlobal)
    throws IOException
  {
    if (mapType == MAP_TYPE_INLINE) {
      _handler = (isGlobal ? new GlobalInlineHandler() :
                  new InlineHandler());
    } else if (mapType == MAP_TYPE_REFERENCE) {
      _handler = (isGlobal ? new GlobalReferenceHandler() :
                  new ReferenceHandler());
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
    return bitIndexToPageNumber(getNextBitIndex(-1),
                                RowIdImpl.LAST_PAGE_NUMBER);
  }

  protected int getNextPageNumber(int curPage) {
    return bitIndexToPageNumber(
        getNextBitIndex(pageNumberToBitIndex(curPage)),
        RowIdImpl.LAST_PAGE_NUMBER);
  }

  protected int getNextBitIndex(int curIndex) {
    return _pageNumbers.nextSetBit(curIndex + 1);
  }

  protected int getLastPageNumber() {
    return bitIndexToPageNumber(getPrevBitIndex(_pageNumbers.length()),
                                RowIdImpl.FIRST_PAGE_NUMBER);
  }

  protected int getPrevPageNumber(int curPage) {
    return bitIndexToPageNumber(
        getPrevBitIndex(pageNumberToBitIndex(curPage)),
        RowIdImpl.FIRST_PAGE_NUMBER);
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
  public void removePageNumber(int pageNumber)
    throws IOException
  {
    removePageNumber(pageNumber, true);
  }

  private void removePageNumber(int pageNumber, boolean force)
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

    List<String> ranges = new ArrayList<String>();
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
          rangeToString(ranges, curRangeStart, prevPage);
        }
        curRangeStart = nextPage;
      }
      prevPage = nextPage;
    }
    if(prevPage >= 0) {
      rangeToString(ranges, curRangeStart, prevPage);
    }

    return ToStringBuilder.valueBuilder(
        _handler.getClass().getSimpleName())
      .append("range", "(" + _startPage + "-" + _endPage + ")")
      .append("pageNumbers", ranges)
      .toString();
  }

  private static void rangeToString(List<String> ranges, int rangeStart,
                                    int rangeEnd)
  {
    if(rangeEnd > rangeStart) {
      ranges.add(rangeStart + "-" + rangeEnd);
    } else {
      ranges.add(String.valueOf(rangeStart));
    }
  }

  private static int toValidStartPage(int startPage) {
    // start page must be a multiple of 8
    return ((startPage / 8) * 8);
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
    private final int _maxInlinePages;

    protected InlineHandler() throws IOException
    {
      _maxInlinePages = (getInlineDataEnd() - getInlineDataStart()) * 8;
      int startPage = getTableBuffer().getInt(getRowStart() + 1);
      setInlinePageRange(startPage);
      processMap(getTableBuffer(), 0);
    }

    protected final int getMaxInlinePages() {
      return _maxInlinePages;
    }

    protected final int getInlineDataStart() {
      return getRowStart() + getFormat().OFFSET_USAGE_MAP_START;
    }

    protected final int getInlineDataEnd() {
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

        // uh-oh, we've split our britches.  what now?
        addOrRemovePageNumberOutsideRange(pageNumber, add, force);
      }
    }

    protected void addOrRemovePageNumberOutsideRange(
        int pageNumber, boolean add, boolean force)
      throws IOException
    {
      // determine what our status is before taking action

      if(add) {

        int firstPage = getFirstPageNumber();
        int lastPage = getLastPageNumber();

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

        firstPage = toValidStartPage(firstPage);

        if((lastPage - firstPage + 1) < getMaxInlinePages()) {

          // we can still fit within an inline map
          moveToNewStartPage(firstPage, pageNumber);

        } else {
          // not going to happen, need to promote the usage map to a
          // reference map
          promoteInlineHandlerToReferenceHandler(pageNumber);
        }

      } else {

        // we are removing, what does that mean?
        if(!force) {

          // this should not happen, we are removing a page which is not in
          // the map
          throw new IOException("Page number " + pageNumber +
                                " already removed from usage map" +
                                ", expected range " +
                                _startPage + " to " + _endPage);
        }
      }
    }

    /**
     * Shifts the inline usage map so that it now starts with the given page.
     * @param newStartPage new page at which to start
     * @param newPageNumber optional page number to add once the map has been
     *                      shifted to the new start page
     */
    protected final void moveToNewStartPage(int newStartPage, int newPageNumber)
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
  }

  /**
   * Modified version of an "inline" usage map used for the global usage map.
   * When an inline usage map is used for the global usage map, we assume
   * out-of-range bits are on.  We never promote the global usage map to a
   * reference usage map (although ms access may).
   *
   * Note, this UsageMap does not implement all the methods "correctly".  Only
   * addPageNumber and removePageNumber should be called by PageChannel.
   */
  private class GlobalInlineHandler extends InlineHandler
  {
    private GlobalInlineHandler() throws IOException {
    }

    @Override
    public boolean containsPageNumber(int pageNumber) {
      // should never be called on global map
      throw new UnsupportedOperationException();
    }

    @Override
    protected void addOrRemovePageNumberOutsideRange(
        int pageNumber, boolean add, boolean force)
      throws IOException
    {
      // determine what our status is

      // for the global usage map, we can ignore out-of-range page addition
      // since we assuming out-of-range bits are "on".  Note, we are leaving
      // small holes in the database here (leaving behind some free pages),
      // but it's not the end of the world.

      if(!add) {

        int firstPage = getFirstPageNumber();
        int lastPage = getLastPageNumber();

        // we are using an inline map and assuming that anything not
        // within the current range is "on".  so, if we attempt to set a
        // bit which is before the current page, ignore it, we are not
        // going back for it.
        if((firstPage <= PageChannel.INVALID_PAGE_NUMBER) ||
           (pageNumber > lastPage)) {

          // move to new start page, filling in as we move
          moveToNewStartPageForRemove(firstPage, pageNumber);
        }
      }
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
        toValidStartPage(
            ((firstPage <= PageChannel.INVALID_PAGE_NUMBER) ? newPageNumber :
             // just shift a little and discard any initial unused pages.
             (newPageNumber - (getMaxInlinePages() / 2))));

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
      removePageNumber(newPageNumber, false);
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
    private final int _maxPagesPerUsageMapPage;

    private ReferenceHandler() throws IOException
    {
      _maxPagesPerUsageMapPage = ((getFormat().PAGE_SIZE -
                                   getFormat().OFFSET_USAGE_MAP_PAGE_DATA) * 8);
      int numUsagePages = (getRowEnd() - getRowStart() - 1) / 4;
      setStartOffset(getFormat().OFFSET_USAGE_MAP_PAGE_DATA);
      setPageRange(0, (numUsagePages * _maxPagesPerUsageMapPage));

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
          processMap(mapPageBuffer, (_maxPagesPerUsageMapPage * i));
        }
      }
    }

    protected final int getMaxPagesPerUsagePage() {
      return _maxPagesPerUsageMapPage;
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
      ByteBuffer mapPageBuffer = allocateNewUsageMapPage(pageIndex);
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

    protected ByteBuffer allocateNewUsageMapPage(int pageIndex)
      throws IOException
    {
      ByteBuffer mapPageBuffer = _mapPageHolder.setNewPage(getPageChannel());
      mapPageBuffer.put(PageTypes.USAGE_MAP);
      mapPageBuffer.put((byte) 0x01);  //Unknown
      mapPageBuffer.putShort((short) 0); //Unknown
      return mapPageBuffer;
    }
  }

  /**
   * Modified version of a "reference" usage map used for the global usage
   * map.  Since reference usage maps require allocating pages for their own
   * use, we need to handle potential cycles where the PageChannel is
   * attempting to allocate a new page (and remove it from the global usage
   * map) and this usage map also needs to allocate a new page.  When that
   * happens, we stash the pending information from the PageChannel and handle
   * it after we have retrieved the new page.
   *
   * Note, this UsageMap does not implement all the methods "correctly".  Only
   * addPageNumber and removePageNumber should be called by PageChannel.
   */
  private class GlobalReferenceHandler extends ReferenceHandler
  {
    private boolean _allocatingPage;
    private Integer _pendingPage;

    private GlobalReferenceHandler() throws IOException {
    }

    @Override
    public boolean containsPageNumber(int pageNumber) {
      // should never be called on global map
      throw new UnsupportedOperationException();
    }

    @Override
    public void addOrRemovePageNumber(int pageNumber, boolean add,
                                      boolean force)
      throws IOException
    {
      if(_allocatingPage && !add) {
        // we are in the midst of allocating a page for ourself, keep track of
        // this new page so we can mark it later...
        if(_pendingPage != null) {
          throw new IllegalStateException("should only have single pending page");
        }
        _pendingPage = pageNumber;
        return;
      }

      super.addOrRemovePageNumber(pageNumber, add, force);

      while(_pendingPage != null) {

        // while updating our usage map, we needed to allocate a new page (and
        // thus mark a new page as used).  we delayed that marking so that we
        // didn't get into an infinite loop.  now that we completed the
        // original updated, handle the new page.  (we use a loop under the
        // off the wall chance that adding this page requires allocating a new
        // page.  in theory, we could do this more than once, but not
        // forever).
        int removedPageNumber = _pendingPage;
        _pendingPage = null;

        super.addOrRemovePageNumber(removedPageNumber, false, true);
      }
    }

    @Override
    protected ByteBuffer allocateNewUsageMapPage(int pageIndex)
      throws IOException
    {
      try {
        // keep track of the fact that we are actively allocating a page for our
        // own use so that we can break the potential cycle.
        _allocatingPage = true;

        ByteBuffer mapPageBuffer = super.allocateNewUsageMapPage(pageIndex);

        // for the global usage map, all pages are "on" by default.  so
        // whenever we add a new backing page to the usage map, we need to
        // turn all the pages that it represents to "on" (we essentially lazy
        // load this map, which is fine because we should only add pages which
        // represent the size of the database currently in use).
        int dataStart = getFormat().OFFSET_USAGE_MAP_PAGE_DATA;
        ByteUtil.fillRange(mapPageBuffer, dataStart,
                           getFormat().PAGE_SIZE - dataStart);

        int maxPagesPerUmapPage = getMaxPagesPerUsagePage();
        int firstNewPage = (pageIndex * maxPagesPerUmapPage);
        int lastNewPage = firstNewPage + maxPagesPerUmapPage;
        _pageNumbers.set(firstNewPage, lastNewPage);

        return mapPageBuffer;

      } finally {
        _allocatingPage = false;
      }
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
     *         {@link RowIdImpl#LAST_PAGE_NUMBER} otherwise
     */
    public int getNextPage() {
      return getAnotherPage(CursorImpl.MOVE_FORWARD);
    }

    /**
     * @return valid page number if there was another page to read,
     *         {@link RowIdImpl#FIRST_PAGE_NUMBER} otherwise
     */
    public int getPreviousPage() {
      return getAnotherPage(CursorImpl.MOVE_REVERSE);
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
      reset(CursorImpl.MOVE_FORWARD);
    }

    /**
     * After calling this method, {@link #getPreviousPage} will return the
     * last page in the map
     */
    public void afterLast() {
      reset(CursorImpl.MOVE_REVERSE);
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
        pageNumber = RowIdImpl.FIRST_PAGE_NUMBER;
      } else if(pageNumber > UsageMap.this.getLastPageNumber()) {
        pageNumber = RowIdImpl.LAST_PAGE_NUMBER;
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
        return RowIdImpl.FIRST_PAGE_NUMBER;
      }
      @Override
      public int getEndPageNumber() {
        return RowIdImpl.LAST_PAGE_NUMBER;
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
        return RowIdImpl.LAST_PAGE_NUMBER;
      }
      @Override
      public int getEndPageNumber() {
        return RowIdImpl.FIRST_PAGE_NUMBER;
      }
    }

  }

}
