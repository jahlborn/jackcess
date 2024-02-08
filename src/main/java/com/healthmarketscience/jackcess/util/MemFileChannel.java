/*
Copyright (c) 2012 James Ahlborn

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

package com.healthmarketscience.jackcess.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.impl.ByteUtil;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;

/**
 * FileChannel implementation which maintains the entire "file" in memory.
 * This enables working with a Database entirely in memory (for situations
 * where disk usage may not be possible or desirable).  Obviously, this
 * requires enough jvm heap space to fit the file data.  Use one of the
 * {@code newChannel()} methods to construct an instance of this class.
 * <p>
 * In order to use this class with a Database, you <i>must</i> use the {@link
 * DatabaseBuilder} to open/create the Database instance, passing an instance
 * of this class to the {@link DatabaseBuilder#setChannel} method.
 * <p>
 * Implementation note: this class is optimized for use with {@link Database}.
 * Therefore not all methods may be implemented and individual read/write
 * operations are only supported within page boundaries.
 *
 * @author James Ahlborn
 * @usage _advanced_class_
 */
public class MemFileChannel extends FileChannel
{
  /** read-only channel access mode */
  public static final String RO_CHANNEL_MODE = "r";
  /** read/write channel access mode */
  public static final String RW_CHANNEL_MODE = "rw";

  private static final byte[][] EMPTY_DATA = new byte[0][];

  // use largest possible Jet "page size" to ensure that reads/writes will
  // always be within a single chunk
  private static final int CHUNK_SIZE = 4096;
  // this ensures that an "empty" mdb will fit in the initial chunk table
  private static final int INIT_CHUNKS = 128;

  /** current read/write position */
  private long _position;
  /** current amount of actual data in the file */
  private long _size;
  /** chunks containing the file data.  the length of the chunk array is
      always a power of 2 and the chunks are always CHUNK_SIZE. */
  private byte[][] _data;

  private MemFileChannel()
  {
    this(0L, 0L, EMPTY_DATA);
  }

  private MemFileChannel(long position, long size, byte[][] data) {
    _position = position;
    _size = size;
    _data = data;
  }

  /**
   * Creates a new read/write, empty MemFileChannel.
   */
  public static MemFileChannel newChannel() {
    return new MemFileChannel();
  }

  /**
   * Creates a new read/write MemFileChannel containing the contents of the
   * given File.  Note, modifications to the returned channel will <i>not</i>
   * affect the original File source.
   */
  public static MemFileChannel newChannel(File file) throws IOException {
    return newChannel(file, RW_CHANNEL_MODE);
  }

  /**
   * Creates a new MemFileChannel containing the contents of the
   * given File with the given mode (for mode details see
   * {@link RandomAccessFile#RandomAccessFile(File,String)}).  Note,
   * modifications to the returned channel will <i>not</i> affect the original
   * File source.
   */
  public static MemFileChannel newChannel(File file, String mode)
    throws IOException
  {
    RandomAccessFile raf = null;
    FileChannel in = null;
    try {
      raf = new RandomAccessFile(file, RO_CHANNEL_MODE);
      return newChannel(in = raf.getChannel(), mode);
    } finally {
      ByteUtil.closeQuietly(in);
      ByteUtil.closeQuietly(raf);
    }
  }

  /**
   * Creates a new MemFileChannel containing the contents of the
   * given Path with the given mode (for mode details see
   * {@link RandomAccessFile#RandomAccessFile(File,String)}).  Note,
   * modifications to the returned channel will <i>not</i> affect the original
   * File source.
   */
  public static MemFileChannel newChannel(Path file, OpenOption... opts)
    throws IOException
  {
    FileChannel in = null;
    try {
      String mode = RO_CHANNEL_MODE;
      if(opts != null) {
        for(OpenOption opt : opts) {
          if(opt == StandardOpenOption.WRITE) {
            mode = RW_CHANNEL_MODE;
            break;
          }
        }
      }
      return newChannel(in = FileChannel.open(file, StandardOpenOption.READ),
                        mode);
    } finally {
      ByteUtil.closeQuietly(in);
    }
  }

  /**
   * Creates a new read/write MemFileChannel containing the contents of the
   * given Path.  Note, modifications to the returned channel will <i>not</i>
   * affect the original File source.
   */
  public static MemFileChannel newChannel(Path file) throws IOException {
    return newChannel(file, DatabaseImpl.RW_CHANNEL_OPTS);
  }

  /**
   * Creates a new read/write MemFileChannel containing the contents of the
   * given InputStream.
   */
  public static MemFileChannel newChannel(InputStream in) throws IOException {
    return newChannel(in, RW_CHANNEL_MODE);
  }

  /**
   * Creates a new MemFileChannel containing the contents of the
   * given InputStream with the given mode (for mode details see
   * {@link RandomAccessFile#RandomAccessFile(File,String)}).
   */
  public static MemFileChannel newChannel(InputStream in, String mode)
    throws IOException
  {
    return newChannel(Channels.newChannel(in), mode);
  }

  /**
   * Creates a new read/write MemFileChannel containing the contents of the
   * given ReadableByteChannel.
   */
  public static MemFileChannel newChannel(ReadableByteChannel in)
    throws IOException
  {
    return newChannel(in, RW_CHANNEL_MODE);
  }

  /**
   * Creates a new MemFileChannel containing the contents of the
   * given ReadableByteChannel with the given mode (for mode details see
   * {@link RandomAccessFile#RandomAccessFile(File,String)}).
   */
  public static MemFileChannel newChannel(ReadableByteChannel in, String mode)
    throws IOException
  {
    MemFileChannel channel = new MemFileChannel();
    channel.transferFrom(in, 0L, Long.MAX_VALUE);
    if(!mode.contains("w")) {
      channel = new ReadOnlyChannel(channel);
    }
    return channel;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int bytesRead = read(dst, _position);
    if(bytesRead > 0) {
      _position += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    if(position >= _size) {
      return -1;
    }

    int numBytes = (int)Math.min(dst.remaining(), _size - position);
    int rem = numBytes;

    while(rem > 0) {
      byte[] chunk = _data[getChunkIndex(position)];
      int chunkOffset = getChunkOffset(position);
      int bytesRead = Math.min(rem, CHUNK_SIZE - chunkOffset);
      dst.put(chunk, chunkOffset, bytesRead);
      rem -= bytesRead;
      position += bytesRead;
    }

    return numBytes;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int bytesWritten = write(src, _position);
    _position += bytesWritten;
    return bytesWritten;
  }

  @Override
  public int write(ByteBuffer src, long position) throws IOException {
    int numBytes = src.remaining();
    long newSize = position + numBytes;
    ensureCapacity(newSize);

    int rem = numBytes;
    while(rem > 0) {
      byte[] chunk = _data[getChunkIndex(position)];
      int chunkOffset = getChunkOffset(position);
      int bytesWritten = Math.min(rem, CHUNK_SIZE - chunkOffset);
      src.get(chunk, chunkOffset, bytesWritten);
      rem -= bytesWritten;
      position += bytesWritten;
    }

    if(newSize > _size) {
      _size = newSize;
    }

    return numBytes;
  }

  @Override
  public long position() throws IOException {
    return _position;
  }

  @Override
  public FileChannel position(long newPosition) throws IOException {
    if(newPosition < 0L) {
      throw new IllegalArgumentException("negative position");
    }
    _position = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    return _size;
  }

  @Override
  public FileChannel truncate(long newSize) throws IOException {
    if(newSize < 0L) {
      throw new IllegalArgumentException("negative size");
    }
    if(newSize < _size) {
      // we'll optimize for memory over speed and aggressively free unused
      // chunks
      for(int i = getNumChunks(newSize); i < getNumChunks(_size); ++i) {
        _data[i] = null;
      }
      _size = newSize;
    }
    _position = Math.min(newSize, _position);
    return this;
  }

  @Override
  public void force(boolean metaData) throws IOException {
    // nothing to do
  }

  /**
   * Convenience method for writing the entire contents of this channel to the
   * given destination channel.
   * @see #transferTo(long,long,WritableByteChannel)
   */
  public long transferTo(WritableByteChannel dst)
    throws IOException
  {
    return transferTo(0L, _size, dst);
  }

  @Override
  public long transferTo(long position, long count, WritableByteChannel dst)
    throws IOException
  {
    if(position >= _size) {
      return 0L;
    }

    count = Math.min(count, _size - position);

    int chunkIndex = getChunkIndex(position);
    int chunkOffset = getChunkOffset(position);

    long numBytes = 0L;
    while(count > 0L) {

      int chunkBytes = (int)Math.min(count, CHUNK_SIZE - chunkOffset);
      ByteBuffer src = ByteBuffer.wrap(_data[chunkIndex], chunkOffset,
                                       chunkBytes);

      do {
        int bytesWritten = dst.write(src);
        if(bytesWritten == 0L) {
          // dst full
          return numBytes;
        }
        numBytes += bytesWritten;
        count -= bytesWritten;
      } while(src.hasRemaining());

      ++chunkIndex;
      chunkOffset = 0;
    }

    return numBytes;
  }

  /**
   * Convenience method for writing the entire contents of this channel to the
   * given destination stream.
   * @see #transferTo(long,long,WritableByteChannel)
   */
  public long transferTo(OutputStream dst)
    throws IOException
  {
    return transferTo(0L, _size, dst);
  }

  /**
   * Convenience method for writing the selected portion of this channel to
   * the given destination stream.
   * @see #transferTo(long,long,WritableByteChannel)
   */
  public long transferTo(long position, long count, OutputStream dst)
    throws IOException
  {
    return transferTo(position, count, Channels.newChannel(dst));
  }

  @Override
  public long transferFrom(ReadableByteChannel src,
                           long position, long count)
    throws IOException
  {
    int chunkIndex = getChunkIndex(position);
    int chunkOffset = getChunkOffset(position);

    long numBytes = 0L;
    while(count > 0L) {

      ensureCapacity(position + numBytes + 1);

      int chunkBytes = (int)Math.min(count, CHUNK_SIZE - chunkOffset);
      ByteBuffer dst = ByteBuffer.wrap(_data[chunkIndex], chunkOffset,
                                       chunkBytes);
      do {
        int bytesRead = src.read(dst);
        if(bytesRead <= 0) {
          // src empty
          return numBytes;
        }
        numBytes += bytesRead;
        count -= bytesRead;
        _size = Math.max(_size, position + numBytes);
      } while(dst.hasRemaining());

      ++chunkIndex;
      chunkOffset = 0;
    }

    return numBytes;
  }

  @Override
  protected void implCloseChannel() throws IOException {
    // release data
    _data = EMPTY_DATA;
    _size = _position = 0L;
  }

  private void ensureCapacity(long newSize)
  {
    if(newSize <= _size) {
      // nothing to do
      return;
    }

    int newNumChunks = getNumChunks(newSize);
    int numChunks = getNumChunks(_size);

    if(newNumChunks > _data.length) {

      // need to extend chunk array (use powers of 2)
      int newDataLen = Math.max(_data.length, INIT_CHUNKS);
      while(newDataLen < newNumChunks) {
        newDataLen <<= 1;
      }

      byte[][] newData = new byte[newDataLen][];

      // copy existing chunks
      System.arraycopy(_data, 0, newData, 0, numChunks);

      _data = newData;
    }

    // allocate new chunks
    for(int i = numChunks; i < newNumChunks; ++i) {
      _data[i] = new byte[CHUNK_SIZE];
    }
  }

  private static int getChunkIndex(long pos) {
    return (int)(pos / CHUNK_SIZE);
  }

  private static int getChunkOffset(long pos) {
    return (int)(pos % CHUNK_SIZE);
  }

  private static int getNumChunks(long size) {
    return getChunkIndex(size + CHUNK_SIZE - 1);
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length)
    throws IOException
  {
    long numBytes = 0L;
    for(int i = offset; i < offset + length; ++i) {
      numBytes += write(srcs[i]);
    }
    return numBytes;
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length)
    throws IOException
  {
    long numBytes = 0L;
    for(int i = offset; i < offset + length; ++i) {
      if(_position >= _size) {
        return ((numBytes > 0L) ? numBytes : -1L);
      }
      numBytes += read(dsts[i]);
    }
    return numBytes;
  }

  @Override
  public MappedByteBuffer map(MapMode mode, long position, long size)
    throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileLock lock(long position, long size, boolean shared)
    throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileLock tryLock(long position, long size, boolean shared)
    throws IOException
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Subclass of MemFileChannel which is read-only.
   */
  private static final class ReadOnlyChannel extends MemFileChannel
  {
    private ReadOnlyChannel(MemFileChannel channel)
    {
      super(channel._position, channel._size, channel._data);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
      throw new NonWritableChannelException();
    }

    @Override
    public FileChannel truncate(long newSize) throws IOException {
      throw new NonWritableChannelException();
    }

    @Override
    public long transferFrom(ReadableByteChannel src,
                             long position, long count)
      throws IOException
    {
      throw new NonWritableChannelException();
    }
  }
}
