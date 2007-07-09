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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Describes which database pages a particular table uses
 * @author Tim McCune
 */
public abstract class UsageMap {
  
  private static final Log LOG = LogFactory.getLog(UsageMap.class);
  
  /** Inline map type */
  public static final byte MAP_TYPE_INLINE = 0x0;
  /** Reference map type, for maps that are too large to fit inline */
  public static final byte MAP_TYPE_REFERENCE = 0x1;
  
  /** Page number of the map declaration */
  private int _dataPageNum;
  /** Offset of the data page at which the usage map data starts */
  private int _startOffset;
  /** Offset of the data page at which the usage map declaration starts */
  private short _rowStart;
  /** Format of the database that contains this usage map */
  private JetFormat _format;
  /** First page that this usage map applies to */
  private int _startPage;
  /** bits representing page numbers used, offset from _startPage */
  private BitSet _pageNumbers = new BitSet();
  /** Buffer that contains the usage map declaration page */
  private ByteBuffer _dataBuffer;
  /** Used to read in pages */
  private PageChannel _pageChannel;
  /** modification count on the usage map, used to keep the iterators in
      sync */
  private int _modCount = 0;
  
  /**
   * @param pageChannel Used to read in pages
   * @param pageNum Page number that this usage map is contained in
   * @param rowNum Number of the row on the page that contains this usage map
   * @param format Format of the database that contains this usage map
   * @return Either an InlineUsageMap or a ReferenceUsageMap, depending on which
   *    type of map is found
   */
  public static UsageMap read(PageChannel pageChannel, int pageNum, byte rowNum, JetFormat format)
  throws IOException
  {
    ByteBuffer dataBuffer = pageChannel.createPageBuffer();
    pageChannel.readPage(dataBuffer, pageNum);
    short rowStart = Table.findRowStart(dataBuffer, rowNum, format);
    int rowEnd = Table.findRowEnd(dataBuffer, rowNum, format);
    dataBuffer.limit(rowEnd);    
    byte mapType = dataBuffer.get(rowStart);
    UsageMap rtn;
    if (mapType == MAP_TYPE_INLINE) {
      rtn = new InlineUsageMap(pageChannel, dataBuffer, pageNum, format, rowStart);   
    } else if (mapType == MAP_TYPE_REFERENCE) {
      rtn = new ReferenceUsageMap(pageChannel, dataBuffer, pageNum, format, rowStart);
    } else {
      throw new IOException("Unrecognized map type: " + mapType);
    }
    return rtn;
  }
  
  /**
   * @param pageChannel Used to read in pages
   * @param dataBuffer Buffer that contains this map's declaration
   * @param pageNum Page number that this usage map is contained in
   * @param format Format of the database that contains this usage map
   * @param rowStart Offset at which the declaration starts in the buffer
   */
  protected UsageMap(PageChannel pageChannel, ByteBuffer dataBuffer,
                     int pageNum, JetFormat format, short rowStart)
  throws IOException
  {
    _pageChannel = pageChannel;
    _dataBuffer = dataBuffer;
    _dataPageNum = pageNum;
    _format = format;
    _rowStart = rowStart;
    _dataBuffer.position((int) _rowStart + format.OFFSET_MAP_START);
    _startOffset = _dataBuffer.position();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Usage map block:\n" + ByteUtil.toHexString(_dataBuffer, _rowStart,
          dataBuffer.limit() - _rowStart));
    }
  }

  public PageIterator iterator() {
    return new ForwardPageIterator();
  }

  public PageIterator reverseIterator() {
    return new ReversePageIterator();
  }
  
  protected short getRowStart() {
    return _rowStart;
  }
  
  protected void setStartOffset(int startOffset) {
    _startOffset = startOffset;
  }
  
  protected int getStartOffset() {
    return _startOffset;
  }
  
  protected ByteBuffer getDataBuffer() {
    return _dataBuffer;
  }
  
  protected int getDataPageNumber() {
    return _dataPageNum;
  }
  
  protected PageChannel getPageChannel() {
    return _pageChannel;
  }
  
  protected JetFormat getFormat() {
    return _format;
  }

  protected int getStartPage() {
    return _startPage;
  }

  protected void setStartPage(int newStartPage) {
    _startPage = newStartPage;
  }

  protected BitSet getPageNumbers() {
    return _pageNumbers;
  }
  
  /**
   * Read in the page numbers in this inline map
   */
  protected void processMap(ByteBuffer buffer, int pageIndex, int startPage) {
    int byteCount = 0;
    _startPage = startPage;
    while (buffer.hasRemaining()) {
      byte b = buffer.get();
      for (int i = 0; i < 8; i++) {
        if ((b & (1 << i)) != 0) {
          int pageNumberOffset = (byteCount * 8 + i) +
            (pageIndex * _format.PAGES_PER_USAGE_MAP_PAGE);
          _pageNumbers.set(pageNumberOffset);
        }
      }
      byteCount++;
    }
  }
  
  /**
   * Add a page number to this usage map
   */
  public void addPageNumber(int pageNumber) throws IOException {
    //Sanity check, only on in debug mode for performance considerations
    if (LOG.isDebugEnabled()) {
      int pageNumberOffset = pageNumber - _startPage;
      if((pageNumberOffset < 0) ||
         _pageNumbers.get(pageNumberOffset)) {
        throw new IOException("Page number " + pageNumber +
                              " already in usage map");
      }
    }
    ++_modCount;
    addOrRemovePageNumber(pageNumber, true);
  }
  
  /**
   * Remove a page number from this usage map
   */
  public void removePageNumber(int pageNumber) throws IOException {
    ++_modCount;
    addOrRemovePageNumber(pageNumber, false);
  }
  
  protected void updateMap(int absolutePageNumber, int relativePageNumber,
      int bitmask, ByteBuffer buffer, boolean add)
  {
    //Find the byte to apply the bitmask to
    int offset = relativePageNumber / 8;
    byte b = buffer.get(_startOffset + offset);
    //Apply the bitmask
    int pageNumberOffset = absolutePageNumber - _startPage;
    if (add) {
      b |= bitmask;
      _pageNumbers.set(pageNumberOffset);
    } else {
      b &= ~bitmask;
      _pageNumbers.clear(pageNumberOffset);
    }
    buffer.put(_startOffset + offset, b);
  }
  
  public String toString() {
    StringBuilder builder = new StringBuilder("page numbers: [");
    for(PageIterator iter = iterator(); iter.hasNextPage(); ) {
      builder.append(iter.getNextPage());
      if(iter.hasNextPage()) {
        builder.append(", ");
      }
    }
    builder.append("]");
    return builder.toString();
  }
  
  /**
   * @param pageNumber Page number to add or remove from this map
   * @param add True to add it, false to remove it
   */
  protected abstract void addOrRemovePageNumber(int pageNumber, boolean add) throws IOException;

  /**
   * Utility class to iterate over the pages in the UsageMap.
   */
  public abstract class PageIterator
  {
    /** the next set page number bit */
    protected int _nextSetBit;
    /** the previous set page number bit */
    protected int _prevSetBit;
    /** the last read modification count on the UsageMap.  we track this so
        that the iterator can detect updates to the usage map while iterating
        and act accordingly */
    protected int _lastModCount;

    protected PageIterator() {
    }

    /**
     * @return {@code true} if there is another valid page, {@code false}
     *         otherwise.
     */
    public boolean hasNextPage() {
      if((_nextSetBit < 0) &&
         (_lastModCount != _modCount)) {
        // recheck the last page, in case more showed up
        if(_prevSetBit < 0) {
          // we were at the beginning
          reset();
        } else {
          _nextSetBit = _prevSetBit;
          getNextPage();
        }
      }
      return(_nextSetBit >= 0);
    }      
    
    /**
     * @return valid page number if there was another page to read,
     *         {@link PageChannel#INVALID_PAGE_NUMBER} otherwise
     */
    public abstract int getNextPage();

    /**
     * After calling this method, getNextPage will return the first page in the
     * map
     */
    public abstract void reset();
  }
  
  /**
   * Utility class to iterate forward over the pages in the UsageMap.
   */
  public class ForwardPageIterator extends PageIterator
  {
    private ForwardPageIterator() {
      reset();
    }
    
    @Override
    public int getNextPage() {
      if (hasNextPage()) {
        _lastModCount = _modCount;
        _prevSetBit = _nextSetBit;
        _nextSetBit = _pageNumbers.nextSetBit(_nextSetBit + 1);
        return _prevSetBit + _startPage;
      } else {
        return PageChannel.INVALID_PAGE_NUMBER;
      }
    }

    @Override
    public final void reset() {
      _lastModCount = _modCount;
      _prevSetBit = -1;
      _nextSetBit = _pageNumbers.nextSetBit(0);
    }
  }
  
  /**
   * Utility class to iterate backward over the pages in the UsageMap.
   */
  public class ReversePageIterator extends PageIterator
  {
    private ReversePageIterator() {
      reset();
    }
    
    @Override
    public int getNextPage() {
      if(hasNextPage()) {
        _lastModCount = _modCount;
        _prevSetBit = _nextSetBit;
        --_nextSetBit;
        while(hasNextPage() && !_pageNumbers.get(_nextSetBit)) {
          --_nextSetBit;
        }
        return _prevSetBit + _startPage;
      } else {
        return PageChannel.INVALID_PAGE_NUMBER;
      }
    }

    @Override
    public final void reset() {
      _lastModCount = _modCount;
      _prevSetBit = -1;
      _nextSetBit = _pageNumbers.length() - 1;
    }
  }

  
}
