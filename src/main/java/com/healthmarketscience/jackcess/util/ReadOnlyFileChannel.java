/*
Copyright (c) 2016 James Ahlborn

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import com.healthmarketscience.jackcess.Database;

/**
 * Wrapper for existing FileChannel which is read-only.
 * <p/>
 * Implementation note: this class is optimized for use with {@link Database}.
 * Therefore not all methods may be implemented.
 * 
 * @author James Ahlborn
 * @usage _advanced_class_
 */
public class ReadOnlyFileChannel extends FileChannel
{
  private final FileChannel _delegate;
  
  public ReadOnlyFileChannel(FileChannel delegate) {
    _delegate = delegate;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return _delegate.read(dst);
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    return _delegate.read(dsts, offset, length);
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    return _delegate.read(dst, position);
  }

  @Override
  public long position() throws IOException {
    return _delegate.position();
  }

  @Override
  public FileChannel position(long newPosition) throws IOException {
    _delegate.position(newPosition);
    return this;
  }

  @Override
  public long size() throws IOException {
    return _delegate.size();
  }

  @Override
  public FileChannel truncate(long size) throws IOException {
    throw new NonWritableChannelException();
  }

  @Override
  public void force(boolean metaData) throws IOException {
    // do nothing
  }

  @Override
  public long transferTo(long position, long count, WritableByteChannel target) 
    throws IOException 
  {
    return _delegate.transferTo(position, count, target);
  }

  @Override
  public long transferFrom(ReadableByteChannel src, long position, long count) 
    throws IOException 
  {
    throw new NonWritableChannelException();
  }

  @Override
  public int write(ByteBuffer src, long position) throws IOException {
    throw new NonWritableChannelException();
  }


  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new NonWritableChannelException();
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length) 
    throws IOException 
  {
    throw new NonWritableChannelException();
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

  @Override
  protected void implCloseChannel() throws IOException {
    _delegate.close();
  }
}
