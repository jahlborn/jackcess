/*
Copyright (c) 2008 Health Market Science, Inc.

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

  /**
   * The caching type for the buffer holder.
   */
  public enum Type {
    /** a hard reference is maintained to the created buffer */
    HARD,
    /** a soft reference is maintained to the created buffer (may be garbage
        collected if memory gets tight) */
    SOFT,
    /** no reference is maintained to a created buffer (new buffer every
        time) */
    NONE;
  }
  
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
   * @param type the type of reference desired for any created buffer
   * @param autoRewind whether or not every get automatically rewinds the
   *                   buffer
   */
  public static TempBufferHolder newHolder(Type type, boolean autoRewind) {
    return newHolder(type, autoRewind, PageChannel.DEFAULT_BYTE_ORDER);
  }
  
  /**
   * Creates a new TempBufferHolder.
   * @param type the type of reference desired for any created buffer
   * @param autoRewind whether or not every get automatically rewinds the
   *                   buffer
   * @param order byte order for all allocated buffers
   */
  public static TempBufferHolder newHolder(Type type, boolean autoRewind,
                                           ByteOrder order)
  {
    switch(type) {
    case HARD:
      return new HardTempBufferHolder(autoRewind, order);
    case SOFT:
      return new SoftTempBufferHolder(autoRewind, order);
    case NONE:
      return new NoneTempBufferHolder(autoRewind, order);
    default:
      throw new IllegalStateException("Unknown type " + type);
    }
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
      buffer = PageChannel.createBuffer(size, _order);
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
   * @return the currently referenced buffer, {@code null} if none
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
   * TempBufferHolder which has a hard reference to the buffer.
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
   * TempBufferHolder which has a soft reference to the buffer.
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
    }

    @Override
    public void clear() {
      _buffer.clear();
    }
  }
  
  /**
   * TempBufferHolder which has a no reference to the buffer.
   */
  private static final class NoneTempBufferHolder extends TempBufferHolder
  {
    private NoneTempBufferHolder(boolean autoRewind, ByteOrder order) {
      super(autoRewind, order);
    }
    
    @Override
    public ByteBuffer getExistingBuffer() {
      return null;
    }
    
    @Override
    protected void setNewBuffer(ByteBuffer newBuffer) {
      // nothing to do
    }

    @Override
    public void clear() {
      // nothing to do
    }
  }
  
}
