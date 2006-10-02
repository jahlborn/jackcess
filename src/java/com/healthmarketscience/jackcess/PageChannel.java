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
import java.nio.ByteOrder;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Reads and writes individual pages in a database file
 * @author Tim McCune
 */
public class PageChannel implements Channel {
  
  private static final Log LOG = LogFactory.getLog(PageChannel.class);
  
  static final int INVALID_PAGE_NUMBER = -1;

  /** dummy buffer used when allocating new pages */
  private static final ByteBuffer FORCE_BYTES = ByteBuffer.allocate(1);
  
  /** Global usage map always lives on page 1 */
  private static final int PAGE_GLOBAL_USAGE_MAP = 1;
  
  /** Channel containing the database */
  private FileChannel _channel;
  /** Format of the database in the channel */
  private JetFormat _format;
  /** Tracks free pages in the database. */
  private UsageMap _globalUsageMap;
  
  /**
   * @param channel Channel containing the database
   * @param format Format of the database in the channel
   */
  public PageChannel(FileChannel channel, JetFormat format) throws IOException {
    _channel = channel;
    _format = format;
    //Null check only exists for unit tests.  Channel should never normally be null.
    if (channel != null) {
      _globalUsageMap = UsageMap.read(this, PAGE_GLOBAL_USAGE_MAP, (byte) 0, format);
    }
  }
  
  /**
   * @param buffer Buffer to read the page into
   * @param pageNumber Number of the page to read in (starting at 0)
   * @return True if the page was successfully read into the buffer, false if
   *    that page doesn't exist.
   */
  public boolean readPage(ByteBuffer buffer, int pageNumber)
    throws IOException
  {
    if(pageNumber == INVALID_PAGE_NUMBER) {
      throw new IllegalStateException("invalid page number");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reading in page " + Integer.toHexString(pageNumber));
    }
    buffer.clear();
    boolean rtn = _channel.read(buffer, (long) pageNumber * (long) _format.PAGE_SIZE) != -1;
    buffer.flip();
    return rtn;
  }
  
  /**
   * Write a page to disk
   * @param page Page to write
   * @param pageNumber Page number to write the page to
   */
  public void writePage(ByteBuffer page, int pageNumber) throws IOException {
    page.rewind();
    _channel.write(page, (long) pageNumber * (long) _format.PAGE_SIZE);
    _channel.force(true);
  }
  
  /**
   * Write a page to disk as a new page, appending it to the database
   * @param page Page to write
   * @return Page number at which the page was written
   */
  public int writeNewPage(ByteBuffer page) throws IOException {
    long size = _channel.size();
    page.rewind();
    _channel.write(page, size);
    int pageNumber = (int) (size / _format.PAGE_SIZE);
    _globalUsageMap.removePageNumber(pageNumber);  //force is done here
    return pageNumber;
  }

  /**
   * Allocates a new page in the database.  Data in the page is undefined
   * until it is written in a call to {@link #writePage}.
   */
  public int allocateNewPage() throws IOException {
    long size = _channel.size();
    FORCE_BYTES.rewind();
    long offset = size + _format.PAGE_SIZE - FORCE_BYTES.remaining();
    // this will force the file to be extended with mostly undefined bytes
    _channel.write(FORCE_BYTES, offset);
    int pageNumber = (int) (size / _format.PAGE_SIZE);
    _globalUsageMap.removePageNumber(pageNumber);  //force is done here
    return pageNumber;
  }
  
  /**
   * @return Number of pages in the database
   */
  public int getPageCount() throws IOException {
    return (int) (_channel.size() / _format.PAGE_SIZE);
  }
  
  /**
   * @return A newly-allocated buffer that can be passed to readPage
   */
  public ByteBuffer createPageBuffer() {
    ByteBuffer rtn = ByteBuffer.allocate(_format.PAGE_SIZE);
    rtn.order(ByteOrder.LITTLE_ENDIAN);
    return rtn;
  }
  
  public void close() throws IOException {
    _channel.force(true);
    _channel.close();
  }
  
  public boolean isOpen() {
    return _channel.isOpen();
  }

  /**
   * @return a duplicate of the current buffer narrowed to the given position
   *         and limit.  mark will be set at the current position.
   */
  public static ByteBuffer narrowBuffer(ByteBuffer buffer, int position,
                                        int limit)
  {
    return (ByteBuffer)buffer.duplicate()
      .order(buffer.order())
      .clear()
      .limit(limit)
      .position(position)
      .mark();
  }
  
}
