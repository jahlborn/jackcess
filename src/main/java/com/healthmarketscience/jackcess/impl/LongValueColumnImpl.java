/*
Copyright (c) 2014 James Ahlborn

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
import java.nio.ByteOrder;


/**
 * ColumnImpl subclass which is used for long value data types.
 * 
 * @author James Ahlborn
 * @usage _advanced_class_
 */
class LongValueColumnImpl extends ColumnImpl 
{
  /**
   * Long value (LVAL) type that indicates that the value is stored on the
   * same page
   */
  private static final byte LONG_VALUE_TYPE_THIS_PAGE = (byte) 0x80;
  /**
   * Long value (LVAL) type that indicates that the value is stored on another
   * page
   */
  private static final byte LONG_VALUE_TYPE_OTHER_PAGE = (byte) 0x40;
  /**
   * Long value (LVAL) type that indicates that the value is stored on
   * multiple other pages
   */
  private static final byte LONG_VALUE_TYPE_OTHER_PAGES = (byte) 0x00;
  /**
   * Mask to apply the long length in order to get the flag bits (only the
   * first 2 bits are type flags).
   */
  private static final int LONG_VALUE_TYPE_MASK = 0xC0000000;


  /** Holds additional info for writing long values */
  private LongValueBufferHolder _lvalBufferH;

  LongValueColumnImpl(InitArgs args) throws IOException
  {
    super(args);
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

  protected int getMaxLengthInUnits() {
    return getType().toUnitSize(getType().getMaxSize());
  }

  @Override
  public Object read(byte[] data, ByteOrder order) throws IOException {
    switch(getType()) {
    case OLE:
      if (data.length > 0) {
        return readLongValue(data);
      }
      return null;
    case MEMO:
      if (data.length > 0) {
        return readLongStringValue(data);
      }
      return null;
    default:
      throw new RuntimeException(withErrorContext(
              "unexpected var length, long value type: " + getType()));
    }    
  }

  @Override
  protected ByteBuffer writeRealData(Object obj, int remainingRowLength,
                                     ByteOrder order)
    throws IOException
  {
    switch(getType()) {
    case OLE:
      // should already be "encoded"
      break;
    case MEMO:
      obj = encodeTextValue(obj, 0, getMaxLengthInUnits(), false).array();
      break;
    default:
      throw new RuntimeException(withErrorContext(
              "unexpected var length, long value type: " + getType()));
    }    

    // create long value buffer
    return writeLongValue(toByteArray(obj), remainingRowLength);
  }  
  
  /**
   * @param lvalDefinition Column value that points to an LVAL record
   * @return The LVAL data
   */
  protected byte[] readLongValue(byte[] lvalDefinition)
    throws IOException
  {
    ByteBuffer def = PageChannel.wrap(lvalDefinition);
    int lengthWithFlags = def.getInt();
    int length = lengthWithFlags & (~LONG_VALUE_TYPE_MASK);

    byte[] rtn = new byte[length];
    byte type = (byte)((lengthWithFlags & LONG_VALUE_TYPE_MASK) >>> 24);

    if(type == LONG_VALUE_TYPE_THIS_PAGE) {

      // inline long value
      def.getInt();  //Skip over lval_dp
      def.getInt();  //Skip over unknown

      int rowLen = def.remaining();
      if(rowLen < length) {
        // warn the caller, but return whatever we can
        LOG.warn(withErrorContext(
                "Value may be truncated: expected length " + 
                length + " found " + rowLen));
        rtn = new byte[rowLen];
      }

      def.get(rtn);

    } else {

      // long value on other page(s)
      if (lvalDefinition.length != getFormat().SIZE_LONG_VALUE_DEF) {
        throw new IOException(withErrorContext(
                "Expected " + getFormat().SIZE_LONG_VALUE_DEF +
                " bytes in long value definition, but found " +
                lvalDefinition.length));
      }

      int rowNum = ByteUtil.getUnsignedByte(def);
      int pageNum = ByteUtil.get3ByteInt(def, def.position());
      ByteBuffer lvalPage = getPageChannel().createPageBuffer();
      
      switch (type) {
      case LONG_VALUE_TYPE_OTHER_PAGE:
        {
          getPageChannel().readPage(lvalPage, pageNum);

          short rowStart = TableImpl.findRowStart(lvalPage, rowNum, getFormat());
          short rowEnd = TableImpl.findRowEnd(lvalPage, rowNum, getFormat());

          int rowLen = rowEnd - rowStart;
          if(rowLen < length) {
            // warn the caller, but return whatever we can
            LOG.warn(withErrorContext(
                    "Value may be truncated: expected length " + 
                    length + " found " + rowLen));
            rtn = new byte[rowLen];
          }
        
          lvalPage.position(rowStart);
          lvalPage.get(rtn);
        }
        break;
        
      case LONG_VALUE_TYPE_OTHER_PAGES:

        ByteBuffer rtnBuf = ByteBuffer.wrap(rtn);
        int remainingLen = length;
        while(remainingLen > 0) {
          lvalPage.clear();
          getPageChannel().readPage(lvalPage, pageNum);

          short rowStart = TableImpl.findRowStart(lvalPage, rowNum, getFormat());
          short rowEnd = TableImpl.findRowEnd(lvalPage, rowNum, getFormat());
          
          // read next page information
          lvalPage.position(rowStart);
          rowNum = ByteUtil.getUnsignedByte(lvalPage);
          pageNum = ByteUtil.get3ByteInt(lvalPage);

          // update rowEnd and remainingLen based on chunkLength
          int chunkLength = (rowEnd - rowStart) - 4;
          if(chunkLength > remainingLen) {
            rowEnd = (short)(rowEnd - (chunkLength - remainingLen));
            chunkLength = remainingLen;
          }
          remainingLen -= chunkLength;
          
          lvalPage.limit(rowEnd);
          rtnBuf.put(lvalPage);
        }
        
        break;
        
      default:
        throw new IOException(withErrorContext(
                "Unrecognized long value type: " + type));
      }
    }
    
    return rtn;
  }
  
  /**
   * @param lvalDefinition Column value that points to an LVAL record
   * @return The LVAL data
   */
  private String readLongStringValue(byte[] lvalDefinition)
    throws IOException
  {
    byte[] binData = readLongValue(lvalDefinition);
    if(binData == null) {
      return null;
    }
    if(binData.length == 0) {
      return "";
    }
    return decodeTextValue(binData);
  }

  /**
   * Write an LVAL column into a ByteBuffer inline if it fits, otherwise in
   * other data page(s).
   * @param value Value of the LVAL column
   * @return A buffer containing the LVAL definition and (possibly) the column
   *         value (unless written to other pages)
   * @usage _advanced_method_
   */
  protected ByteBuffer writeLongValue(byte[] value, int remainingRowLength) 
    throws IOException
  {
    if(value.length > getType().getMaxSize()) {
      throw new IOException(withErrorContext(
              "value too big for column, max " +
              getType().getMaxSize() + ", got " + value.length));
    }

    // determine which type to write
    byte type = 0;
    int lvalDefLen = getFormat().SIZE_LONG_VALUE_DEF;
    if(((getFormat().SIZE_LONG_VALUE_DEF + value.length) <= remainingRowLength)
       && (value.length <= getFormat().MAX_INLINE_LONG_VALUE_SIZE)) {
      type = LONG_VALUE_TYPE_THIS_PAGE;
      lvalDefLen += value.length;
    } else if(value.length <= getFormat().MAX_LONG_VALUE_ROW_SIZE) {
      type = LONG_VALUE_TYPE_OTHER_PAGE;
    } else {
      type = LONG_VALUE_TYPE_OTHER_PAGES;
    }

    ByteBuffer def = PageChannel.createBuffer(lvalDefLen);
    // take length and apply type to first byte
    int lengthWithFlags = value.length | (type << 24);
    def.putInt(lengthWithFlags);

    if(type == LONG_VALUE_TYPE_THIS_PAGE) {
      // write long value inline
      def.putInt(0);
      def.putInt(0);  //Unknown
      def.put(value);
    } else {
      
      ByteBuffer lvalPage = null;
      int firstLvalPageNum = PageChannel.INVALID_PAGE_NUMBER;
      byte firstLvalRow = 0;
      
      // write other page(s)
      switch(type) {
      case LONG_VALUE_TYPE_OTHER_PAGE:
        lvalPage = _lvalBufferH.getLongValuePage(value.length);
        firstLvalPageNum = _lvalBufferH.getPageNumber();
        firstLvalRow = (byte)TableImpl.addDataPageRow(lvalPage, value.length,
                                                  getFormat(), 0);
        lvalPage.put(value);
        getPageChannel().writePage(lvalPage, firstLvalPageNum);
        break;

      case LONG_VALUE_TYPE_OTHER_PAGES:

        ByteBuffer buffer = ByteBuffer.wrap(value);
        int remainingLen = buffer.remaining();
        buffer.limit(0);
        lvalPage = _lvalBufferH.getLongValuePage(remainingLen);
        firstLvalPageNum = _lvalBufferH.getPageNumber();
        firstLvalRow = (byte)TableImpl.getRowsOnDataPage(lvalPage, getFormat());
        int lvalPageNum = firstLvalPageNum;
        ByteBuffer nextLvalPage = null;
        int nextLvalPageNum = 0;
        int nextLvalRowNum = 0;
        while(remainingLen > 0) {
          lvalPage.clear();

          // figure out how much we will put in this page (we need 4 bytes for
          // the next page pointer)
          int chunkLength = Math.min(getFormat().MAX_LONG_VALUE_ROW_SIZE - 4,
                                     remainingLen);

          // figure out if we will need another page, and if so, allocate it
          if(chunkLength < remainingLen) {
            // force a new page to be allocated for the chunk after this
            _lvalBufferH.clear();
            nextLvalPage = _lvalBufferH.getLongValuePage(
                (remainingLen - chunkLength) + 4);
            nextLvalPageNum = _lvalBufferH.getPageNumber();
            nextLvalRowNum = TableImpl.getRowsOnDataPage(nextLvalPage, 
                                                         getFormat());
          } else {
            nextLvalPage = null;
            nextLvalPageNum = 0;
            nextLvalRowNum = 0;
          }

          // add row to this page
          TableImpl.addDataPageRow(lvalPage, chunkLength + 4, getFormat(), 0);
          
          // write next page info
          lvalPage.put((byte)nextLvalRowNum); // row number
          ByteUtil.put3ByteInt(lvalPage, nextLvalPageNum); // page number

          // write this page's chunk of data
          buffer.limit(buffer.limit() + chunkLength);
          lvalPage.put(buffer);
          remainingLen -= chunkLength;

          // write new page to database
          getPageChannel().writePage(lvalPage, lvalPageNum);

          // move to next page
          lvalPage = nextLvalPage;
          lvalPageNum = nextLvalPageNum;
        }
        break;

      default:
        throw new IOException(withErrorContext(
                "Unrecognized long value type: " + type));
      }

      // update def
      def.put(firstLvalRow);
      ByteUtil.put3ByteInt(def, firstLvalPageNum);
      def.putInt(0);  //Unknown
      
    }
      
    def.flip();
    return def;
  }

  /**
   * Writes the header info for a long value page.
   */
  private void writeLongValueHeader(ByteBuffer lvalPage)
  {
    lvalPage.put(PageTypes.DATA); //Page type
    lvalPage.put((byte) 1); //Unknown
    lvalPage.putShort((short)getFormat().DATA_PAGE_INITIAL_FREE_SPACE); //Free space
    lvalPage.put((byte) 'L');
    lvalPage.put((byte) 'V');
    lvalPage.put((byte) 'A');
    lvalPage.put((byte) 'L');
    lvalPage.putInt(0); //unknown
    lvalPage.putShort((short)0); // num rows in page
  }


  /**
   * Manages secondary page buffers for long value writing.
   */
  private abstract class LongValueBufferHolder
  {
    /**
     * Returns a long value data page with space for data of the given length.
     */
    public ByteBuffer getLongValuePage(int dataLength) throws IOException {

      TempPageHolder lvalBufferH = getBufferHolder();
      dataLength = Math.min(dataLength, getFormat().MAX_LONG_VALUE_ROW_SIZE);

      ByteBuffer lvalPage = null;
      if(lvalBufferH.getPageNumber() != PageChannel.INVALID_PAGE_NUMBER) {
        lvalPage = lvalBufferH.getPage(getPageChannel());
        if(TableImpl.rowFitsOnDataPage(dataLength, lvalPage, getFormat())) {
          // the current page has space
          return lvalPage;
        }
      }

      // need new page
      return findNewPage(dataLength);
    }

    protected ByteBuffer findNewPage(int dataLength) throws IOException {
      ByteBuffer lvalPage = getBufferHolder().setNewPage(getPageChannel());
      writeLongValueHeader(lvalPage);
      return lvalPage;
    }

    public int getOwnedPageCount() {
      return 0;
    }

    /**
     * Returns the page number of the current long value data page.
     */
    public int getPageNumber() {
      return getBufferHolder().getPageNumber();
    }

    /**
     * Discards the current the current long value data page.
     */
    public void clear() throws IOException {
      getBufferHolder().clear();
    }

    protected abstract TempPageHolder getBufferHolder();
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
