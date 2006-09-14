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
  
  /** Index of the current page, incremented after calling getNextPage */
  private int _currentPageIndex = 0;
  /** Page number of the map declaration */
  private int _dataPageNum;
  /** Offset of the data page at which the usage map data starts */
  private int _startOffset;
  /** Offset of the data page at which the usage map declaration starts */
  private short _rowStart;
  /** Format of the database that contains this usage map */
  private JetFormat _format;
  /** List of page numbers used */
  private List<Integer> _pageNumbers = new ArrayList<Integer>();
  /** Buffer that contains the usage map declaration page */
  private ByteBuffer _dataBuffer;
  /** Used to read in pages */
  private PageChannel _pageChannel;
  
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
  
  protected short getRowStart() {
    return _rowStart;
  }
  
  public List<Integer> getPageNumbers() {
    return _pageNumbers;
  }

  public Integer getCurrentPageNumber() {
    return _pageNumbers.get(_currentPageIndex - 1);
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

  /**
   * After calling this method, getNextPage will return the first page in the
   * map
   */
  public void reset() {
    _currentPageIndex = 0;
  }
  
  /**
   * @return non-<code>null</code> if there was another page to read,
   *         <code>null</code> otherwise
   */
  public Integer getNextPage() {
    if (_pageNumbers.size() > _currentPageIndex) {
      return _pageNumbers.get(_currentPageIndex++);
    } else {
      return null;
    }
  }
  
  /**
   * Read in the page numbers in this inline map
   */
  protected void processMap(ByteBuffer buffer, int pageIndex, int startPage) {
    int byteCount = 0;
    while (buffer.hasRemaining()) {
      byte b = buffer.get();
      for (int i = 0; i < 8; i++) {
        if ((b & (1 << i)) != 0) {
          Integer pageNumber = new Integer((startPage + byteCount * 8 + i) +
              (pageIndex * _format.PAGES_PER_USAGE_MAP_PAGE));
          _pageNumbers.add(pageNumber);
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
    if (LOG.isDebugEnabled() && _pageNumbers.contains(new Integer(pageNumber))) {
      throw new IOException("Page number " + pageNumber + " already in usage map");
    }
    addOrRemovePageNumber(pageNumber, true);
  }
  
  /**
   * Remove a page number from this usage map
   */
  public void removePageNumber(int pageNumber) throws IOException {
    addOrRemovePageNumber(pageNumber, false);
  }
  
  protected void updateMap(int absolutePageNumber, int relativePageNumber,
      int bitmask, ByteBuffer buffer, boolean add)
  {
    //Find the byte to apply the bitmask to
    int offset = relativePageNumber / 8;
    byte b = buffer.get(_startOffset + offset);
    //Apply the bitmask
    if (add) {
      b |= bitmask;
      _pageNumbers.add(new Integer(absolutePageNumber));
    } else {
      b &= ~bitmask;
    }
    buffer.put(_startOffset + offset, b);
  }
  
  public String toString() {
    return "page numbers: " + _pageNumbers;
  }
  
  /**
   * @param pageNumber Page number to add or remove from this map
   * @param add True to add it, false to remove it
   */
  protected abstract void addOrRemovePageNumber(int pageNumber, boolean add) throws IOException;
  
}
