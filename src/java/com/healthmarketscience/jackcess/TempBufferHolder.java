// Copyright (c) 2008 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Manages a reference to a ByteBuffer.
 *
 * @author James Ahlborn
 */
public abstract class TempBufferHolder {

  private static final Reference<ByteBuffer> EMPTY_BUFFER_REF =
    new SoftReference<ByteBuffer>(null);

  /** whether or not every get automatically rewinds the buffer */
  private final boolean _autoRewind;
  /** ByteOrder for all allocated buffers */
  private final ByteOrder _order;
  /** the mod count of the current buffer (changes on every realloc) */
  private int _modCount;
  
  protected TempBufferHolder(boolean autoRewind, ByteOrder order) {
    _autoRewind = autoRewind;
    _order = order;
  }

  /**
   * @return the modification count of the current buffer (this count is
   *         changed every time the buffer is reallocated)
   */
  public int getModCount() {
    return _modCount;
  }
  
  /**
   * Creates a new TempBufferHolder.
   * @param hard iff true, the TempBufferHolder will maintain a hard reference
   *             to the current buffer, otherwise will maintain a
   *             SoftReference.
   * @param autoRewind whether or not every get automatically rewinds the
   *                   buffer
   */
  public static TempBufferHolder newHolder(boolean hard, boolean autoRewind) {
    return newHolder(hard, autoRewind, PageChannel.DEFAULT_BYTE_ORDER);
  }
  
  /**
   * Creates a new TempBufferHolder.
   * @param hard iff true, the TempBufferHolder will maintain a hard reference
   *             to the current buffer, otherwise will maintain a
   *             SoftReference.
   * @param autoRewind whether or not every get automatically rewinds the
   *                   buffer
   * @param order byte order for all allocated buffers
   */
  public static TempBufferHolder newHolder(boolean hard, boolean autoRewind,
                                           ByteOrder order)
  {
    if(hard) {
      return new HardTempBufferHolder(autoRewind, order);
    }
    return new SoftTempBufferHolder(autoRewind, order);
  }

  /**
   * Returns a ByteBuffer of at least the defined page size, with the limit
   * set to the page size, and the predefined byteOrder.  Will be rewound iff
   * autoRewind is enabled for this buffer.
   */
  public final ByteBuffer getPageBuffer(PageChannel pageChannel) {
    return getBuffer(pageChannel, pageChannel.getFormat().PAGE_SIZE);
  }

  /**
   * Returns a ByteBuffer of at least the given size, with the limit set to
   * the given size, and the predefined byteOrder.  Will be rewound iff
   * autoRewind is enabled for this buffer.
   */
  public final ByteBuffer getBuffer(PageChannel pageChannel, int size) {
    ByteBuffer buffer = getExistingBuffer();
    if((buffer == null) || (buffer.capacity() < size)) {
      buffer = pageChannel.createBuffer(size, _order);
      ++_modCount;
      setNewBuffer(buffer);
    } else {
      buffer.limit(size);
    }
    if(_autoRewind) {
      buffer.rewind();
    }
    return buffer;
  }

  /**
   * @returns the currently referenced buffer, {@code null} if none
   */
  public abstract ByteBuffer getExistingBuffer();
  
  /**
   * Releases any referenced memory.
   */
  public abstract void clear();

  /**
   * Sets a new buffer for this holder.
   */
  protected abstract void setNewBuffer(ByteBuffer newBuffer);
  
  /**
   * TempBufferHolder which has a hard reference to the buffer buffer.
   */
  private static final class HardTempBufferHolder extends TempBufferHolder
  {
    private ByteBuffer _buffer;

    private HardTempBufferHolder(boolean autoRewind, ByteOrder order) {
      super(autoRewind, order);
    }
    
    @Override
    public ByteBuffer getExistingBuffer() {
      return _buffer;
    }
    
    @Override
    protected void setNewBuffer(ByteBuffer newBuffer) {
      _buffer = newBuffer;
    }

    @Override
    public void clear() {
      _buffer = null;
    }
  }
  
  /**
   * TempBufferHolder which has a soft reference to the buffer buffer.
   */
  private static final class SoftTempBufferHolder extends TempBufferHolder
  {
    private Reference<ByteBuffer> _buffer = EMPTY_BUFFER_REF;

    private SoftTempBufferHolder(boolean autoRewind, ByteOrder order) {
      super(autoRewind, order);
    }
    
    @Override
    public ByteBuffer getExistingBuffer() {
      return _buffer.get();
    }
    
    @Override
    protected void setNewBuffer(ByteBuffer newBuffer) {
      _buffer.clear();
      _buffer = new SoftReference<ByteBuffer>(newBuffer);
//       // FIXME, enable for testing (make this automatic)
//       _buffer = new PhantomReference<ByteBuffer>(newBuffer, null);
    }

    @Override
    public void clear() {
      _buffer.clear();
    }
  }
  
  
}
