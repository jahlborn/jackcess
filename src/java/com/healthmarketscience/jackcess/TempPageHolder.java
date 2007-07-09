// Copyright (c) 2006 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

/**
 * Manages a reference to a page buffer.
 *
 * @author James Ahlborn
 */
public abstract class TempPageHolder {

  private static final SoftReference<ByteBuffer> EMPTY_BUFFER_REF =
    new SoftReference<ByteBuffer>(null);
  
  protected int _pageNumber = PageChannel.INVALID_PAGE_NUMBER;
  
  protected TempPageHolder() {
  }

  /**
   * Creates a new TempPageHolder.
   * @param hard iff true, the TempPageHolder will maintain a hard reference
   *             to the current page buffer, otherwise will maintain a
   *             SoftReference.
   */
  public static TempPageHolder newHolder(boolean hard) {
    if(hard) {
      return new HardTempPageHolder();
    }
    return new SoftTempPageHolder();
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
    ByteBuffer buffer = getBuffer(pageChannel);
    if(pageNumber != _pageNumber) {
      _pageNumber = pageNumber;
      pageChannel.readPage(buffer, _pageNumber);
    } else if(rewind) {
      buffer.rewind();
    }
    
    return buffer;
  }

  /**
   * Sets the current page number to INVALID_PAGE_NUMBER and returns a new
   * empty buffer.  Once the new page has been written to the page channel,
   * {@link #finishNewPage} must be called with the new page number.
   */
  public ByteBuffer startNewPage(PageChannel pageChannel)
    throws IOException
  {
    clear();
    return getBuffer(pageChannel);
  }

  /**
   * Sets the new page number for the current buffer of data.  Must be called
   * after the buffer returned from {@link #startNewPage} has been written to
   * the PageChannel as a new page and assigned a number.
   */
  public void finishNewPage(int pageNumber)
  {
    if(_pageNumber != PageChannel.INVALID_PAGE_NUMBER) {
      throw new IllegalStateException("page number should not be set");
    }
    _pageNumber = pageNumber;
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
    if(modifiedBuffer == getExistingBuffer()) {
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
  }

  protected abstract ByteBuffer getExistingBuffer();
  
  protected abstract ByteBuffer getBuffer(PageChannel pageChannel);

  /**
   * TempPageHolder which has a hard reference to the page buffer.
   */
  private static class HardTempPageHolder extends TempPageHolder
  {
    private ByteBuffer _buffer;

    @Override
    protected ByteBuffer getExistingBuffer() {
      return _buffer;
    }
    
    @Override
    protected ByteBuffer getBuffer(PageChannel pageChannel) {
      if(_buffer == null) {
        _buffer = pageChannel.createPageBuffer();
      }
      return _buffer;
    }

    @Override
    public void clear() {
      super.clear();
      _buffer = null;
    }
  }
  
  /**
   * TempPageHolder which has a soft reference to the page buffer.
   */
  private static class SoftTempPageHolder extends TempPageHolder
  {
    private SoftReference<ByteBuffer> _buffer = EMPTY_BUFFER_REF;

    @Override
    protected ByteBuffer getExistingBuffer() {
      return _buffer.get();
    }
    
    @Override
    protected ByteBuffer getBuffer(PageChannel pageChannel) {
      ByteBuffer buffer = _buffer.get();
      if(buffer == null) {
        buffer = pageChannel.createPageBuffer();
        _buffer = new SoftReference<ByteBuffer>(buffer);
      }
      return buffer;
    }

    @Override
    public void clear() {
      super.clear();
      _buffer.clear();
    }
  }
  
  
}
