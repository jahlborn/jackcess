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

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Manages a reference to a page buffer.
 *
 * @author James Ahlborn
 */
public final class TempPageHolder {

  private int _pageNumber = PageChannel.INVALID_PAGE_NUMBER;
  private final TempBufferHolder _buffer;
  /** the last "modification" count of the buffer that this holder observed.
      this is tracked so that the page data can be re-read if the underlying
      buffer has been discarded since the last page read */
  private int _bufferModCount;
  
  private TempPageHolder(TempBufferHolder.Type type) {
    _buffer = TempBufferHolder.newHolder(type, false);
    _bufferModCount = _buffer.getModCount();
  }

  /**
   * Creates a new TempPageHolder.
   * @param type the type of reference desired for any create page buffers
   */
  public static TempPageHolder newHolder(TempBufferHolder.Type type) {
    return new TempPageHolder(type);
  }

  /**
   * @return the currently set page number
   */
  public int getPageNumber() {
    return _pageNumber;
  }

  /**
   * @return the page for the current page number, reading as necessary,
   *         position and limit are unchanged
   */
  public ByteBuffer getPage(PageChannel pageChannel)
    throws IOException
  {
    return setPage(pageChannel, _pageNumber, false);
  }
  
  /**
   * Sets the current page number and returns that page
   * @return the page for the new page number, reading as necessary, resets
   *         position
   */
  public ByteBuffer setPage(PageChannel pageChannel, int pageNumber)
    throws IOException
  {
    return setPage(pageChannel, pageNumber, true);
  }

  private ByteBuffer setPage(PageChannel pageChannel, int pageNumber,
                             boolean rewind)
    throws IOException
  {
    ByteBuffer buffer = _buffer.getPageBuffer(pageChannel);
    int modCount = _buffer.getModCount();
    if((pageNumber != _pageNumber) || (_bufferModCount != modCount)) {
      _pageNumber = pageNumber;
      _bufferModCount = modCount;
      pageChannel.readPage(buffer, _pageNumber);
    } else if(rewind) {
      buffer.rewind();
    }
    
    return buffer;
  }

  /**
   * Allocates a new buffer in the database (with undefined data) and returns
   * a new empty buffer.
   */
  public ByteBuffer setNewPage(PageChannel pageChannel)
    throws IOException
  {
    // ditch any current data
    clear();
    // allocate a new page in the database
    _pageNumber = pageChannel.allocateNewPage();
    // return a new buffer
    return _buffer.getPageBuffer(pageChannel);
  }

  /**
   * Forces any current page data to be disregarded (any
   * <code>getPage</code>/<code>setPage</code> call must reload page data).
   * Does not necessarily release any memory.
   */
  public void invalidate() {
    possiblyInvalidate(_pageNumber, null);
  }
  
  /**
   * Forces any current page data to be disregarded if it matches the given
   * page number (any <code>getPage</code>/<code>setPage</code> call must
   * reload page data) and is not the given buffer.  Does not necessarily
   * release any memory.
   */
  public void possiblyInvalidate(int modifiedPageNumber,
                                 ByteBuffer modifiedBuffer) {
    if(modifiedBuffer == _buffer.getExistingBuffer()) {
      // no worries, our buffer was the one modified (or is null, either way
      // we'll need to reload)
      return;
    }
    if(modifiedPageNumber == _pageNumber) {
      _pageNumber = PageChannel.INVALID_PAGE_NUMBER;
    }
  }

  /**
   * Forces any current page data to be disregarded (any
   * <code>getPage</code>/<code>setPage</code> call must reload page data) and
   * releases any referenced memory.
   */
  public void clear() {
    invalidate();
    _buffer.clear();
  }

}
