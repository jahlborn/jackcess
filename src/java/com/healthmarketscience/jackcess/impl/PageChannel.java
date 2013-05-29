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

package com.healthmarketscience.jackcess.impl;

import java.io.Flushable;
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
public class PageChannel implements Channel, Flushable {
  
  private static final Log LOG = LogFactory.getLog(PageChannel.class);
  
  static final int INVALID_PAGE_NUMBER = -1;

  static final ByteOrder DEFAULT_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
  
  /** invalid page header, used when deallocating old pages.  data pages
      generally have 4 interesting bytes at the beginning which we want to
      reset. */
  private static final byte[] INVALID_PAGE_BYTE_HEADER =
    new byte[]{PageTypes.INVALID, (byte)0, (byte)0, (byte)0};
  
  /** Global usage map always lives on page 1 */
  static final int PAGE_GLOBAL_USAGE_MAP = 1;
  /** Global usage map always lives at row 0 */
  static final int ROW_GLOBAL_USAGE_MAP = 0;
  
  /** Channel containing the database */
  private final FileChannel _channel;
  /** whether or not the _channel should be closed by this class */
  private final boolean _closeChannel;
  /** Format of the database in the channel */
  private final JetFormat _format;
  /** whether or not to force all writes to disk immediately */
  private final  boolean _autoSync;
  /** buffer used when deallocating old pages.  data pages generally have 4
      interesting bytes at the beginning which we want to reset. */
  private final ByteBuffer _invalidPageBytes =
    ByteBuffer.wrap(INVALID_PAGE_BYTE_HEADER);
  /** dummy buffer used when allocating new pages */
  private final ByteBuffer _forceBytes = ByteBuffer.allocate(1);
  /** Tracks free pages in the database. */
  private UsageMap _globalUsageMap;
  /** handler for the current database encoding type */
  private CodecHandler _codecHandler = DefaultCodecProvider.DUMMY_HANDLER;
  /** temp page buffer used when pages cannot be partially encoded */
  private final TempPageHolder _fullPageEncodeBufferH =
    TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);
  
  /**
   * Only used by unit tests
   */
  protected PageChannel(boolean testing) {
    if(!testing) {
      throw new IllegalArgumentException();
    }
    _channel = null;
    _closeChannel = false;
    _format = JetFormat.VERSION_4;
    _autoSync = false;
  }

  /**
   * @param channel Channel containing the database
   * @param format Format of the database in the channel
   */
  public PageChannel(FileChannel channel, boolean closeChannel,
                     JetFormat format, boolean autoSync)
    throws IOException
  {
    _channel = channel;
    _closeChannel = closeChannel;
    _format = format;
    _autoSync = autoSync;
  }

  /**
   * Does second-stage initialization, must be called after construction.
   */
  public void initialize(DatabaseImpl database, CodecProvider codecProvider)
    throws IOException
  {
    // initialize page en/decoding support
    _codecHandler = codecProvider.createHandler(this, database.getCharset());

    // note the global usage map is a special map where any page outside of
    // the current range is assumed to be "on"
    _globalUsageMap = UsageMap.read(database, PAGE_GLOBAL_USAGE_MAP,
                                    ROW_GLOBAL_USAGE_MAP, true);
  }
  
  public JetFormat getFormat() {
    return _format;
  }

  public boolean isAutoSync() {
    return _autoSync;
  }

  /**
   * Returns the next page number based on the given file size.
   */
  private int getNextPageNumber(long size) {
    return (int)(size / getFormat().PAGE_SIZE);
  }

  /**
   * Returns the offset for a page within the file.
   */
  private long getPageOffset(int pageNumber) {
    return((long) pageNumber * (long) getFormat().PAGE_SIZE);
  }
  
  /**
   * Validates that the given pageNumber is valid for this database.
   */
  private void validatePageNumber(int pageNumber)
    throws IOException
  {
    int nextPageNumber = getNextPageNumber(_channel.size());
    if((pageNumber <= INVALID_PAGE_NUMBER) || (pageNumber >= nextPageNumber)) {
      throw new IllegalStateException("invalid page number " + pageNumber);
    }
  }
  
  /**
   * @param buffer Buffer to read the page into
   * @param pageNumber Number of the page to read in (starting at 0)
   */
  public void readPage(ByteBuffer buffer, int pageNumber)
    throws IOException
  {
    validatePageNumber(pageNumber);
    buffer.clear();
    int bytesRead = _channel.read(
        buffer, (long) pageNumber * (long) getFormat().PAGE_SIZE);
    buffer.flip();
    if(bytesRead != getFormat().PAGE_SIZE) {
      throw new IOException("Failed attempting to read " +
                            getFormat().PAGE_SIZE + " bytes from page " +
                            pageNumber + ", only read " + bytesRead);
    }

    if(pageNumber == 0) {
      // de-mask header (note, page 0 never has additional encoding)
      applyHeaderMask(buffer);
    } else {
      _codecHandler.decodePage(buffer, pageNumber);
    }
  }
  
  /**
   * Write a page to disk
   * @param page Page to write
   * @param pageNumber Page number to write the page to
   */
  public void writePage(ByteBuffer page, int pageNumber) throws IOException {
    writePage(page, pageNumber, 0);
  }
  
  /**
   * Write a page (or part of a page) to disk
   * @param page Page to write
   * @param pageNumber Page number to write the page to
   * @param pageOffset offset within the page at which to start writing the
   *                   page data
   */
  public void writePage(ByteBuffer page, int pageNumber, int pageOffset)
    throws IOException
  {
    validatePageNumber(pageNumber);
    
    page.rewind().position(pageOffset);

    int writeLen = page.remaining();
    if((writeLen + pageOffset) > getFormat().PAGE_SIZE) {
      throw new IllegalArgumentException(
          "Page buffer is too large, size " + (writeLen + pageOffset));
    }
    
    ByteBuffer encodedPage = page;
    if(pageNumber == 0) {
      // re-mask header
      applyHeaderMask(page);
    } else {

      if(!_codecHandler.canEncodePartialPage()) {
        if((pageOffset > 0) && (writeLen < getFormat().PAGE_SIZE)) {

          // current codec handler cannot encode part of a page, so need to
          // copy the modified part into the current page contents in a temp
          // buffer so that we can encode the entire page
          ByteBuffer fullPage = _fullPageEncodeBufferH.setPage(
              this, pageNumber);

          // copy the modified part to the full page
          fullPage.position(pageOffset);
          fullPage.put(page);
          fullPage.rewind();

          // reset so we can write the whole page
          page = fullPage;
          pageOffset = 0;
          writeLen = getFormat().PAGE_SIZE;

        } else {

          _fullPageEncodeBufferH.possiblyInvalidate(pageNumber, null);
        }
      }

      // re-encode page
      encodedPage = _codecHandler.encodePage(page, pageNumber, pageOffset);

      // reset position/limit in case they were affected by encoding
      encodedPage.position(pageOffset).limit(pageOffset + writeLen);
    }

    try {
      _channel.write(encodedPage, (getPageOffset(pageNumber) + pageOffset));
      if(_autoSync) {
        flush();
      }
    } finally {
      if(pageNumber == 0) {
        // de-mask header
        applyHeaderMask(page);
      }
    }
  }
  
  /**
   * Allocates a new page in the database.  Data in the page is undefined
   * until it is written in a call to {@link #writePage(ByteBuffer,int)}.
   */
  public int allocateNewPage() throws IOException {
    // this will force the file to be extended with mostly undefined bytes
    long size = _channel.size();
    if(size >= getFormat().MAX_DATABASE_SIZE) {
      throw new IOException("Database is at maximum size " +
                            getFormat().MAX_DATABASE_SIZE);
    }
    if((size % getFormat().PAGE_SIZE) != 0L) {
      throw new IOException("Database corrupted, file size " + size +
                            " is not multiple of page size " +
                            getFormat().PAGE_SIZE);
    }
    
    _forceBytes.rewind();
    
    // push the buffer to the end of the page, so that a full page's worth of
    // data is written
    int pageOffset = (getFormat().PAGE_SIZE - _forceBytes.remaining());
    long offset = size + pageOffset;
    int pageNumber = getNextPageNumber(size);

    // since we are just allocating page space at this point and not writing
    // meaningful data, we do _not_ encode the page.
    _channel.write(_forceBytes, offset);

    // note, we "force" page removal because we know that this is an unused
    // page (since we just added it to the file)
    _globalUsageMap.removePageNumber(pageNumber, true);
    return pageNumber;
  }

  /**
   * Deallocate a previously used page in the database.
   */
  public void deallocatePage(int pageNumber) throws IOException {
    validatePageNumber(pageNumber);
    
    // don't write the whole page, just wipe out the header (which should be
    // enough to let us know if we accidentally try to use an invalid page)
    _invalidPageBytes.rewind();
    _channel.write(_invalidPageBytes, getPageOffset(pageNumber));
    
    _globalUsageMap.addPageNumber(pageNumber);  //force is done here
  }
  
  /**
   * @return A newly-allocated buffer that can be passed to readPage
   */
  public ByteBuffer createPageBuffer() {
    return createBuffer(getFormat().PAGE_SIZE);
  }

  /**
   * @return A newly-allocated buffer of the given size and DEFAULT_BYTE_ORDER
   *         byte order
   */
  public ByteBuffer createBuffer(int size) {
    return createBuffer(size, DEFAULT_BYTE_ORDER);
  }
  
  /**
   * @return A newly-allocated buffer of the given size and byte order
   */
  public ByteBuffer createBuffer(int size, ByteOrder order) {
    return ByteBuffer.allocate(size).order(order);
  }
  
  public void flush() throws IOException {
    _channel.force(true);
  }
  
  public void close() throws IOException {
    flush();
    if(_closeChannel) {
      _channel.close();
    }
  }
  
  public boolean isOpen() {
    return _channel.isOpen();
  }

  /**
   * Applies the XOR mask to the database header in the given buffer.
   */
  private void applyHeaderMask(ByteBuffer buffer) {
      // de/re-obfuscate the header
      byte[] headerMask = _format.HEADER_MASK;
      for(int idx = 0; idx < headerMask.length; ++idx) {
        int pos = idx + _format.OFFSET_MASKED_HEADER;
        byte b = (byte)(buffer.get(pos) ^ headerMask[idx]);
        buffer.put(pos, b);
      }
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
