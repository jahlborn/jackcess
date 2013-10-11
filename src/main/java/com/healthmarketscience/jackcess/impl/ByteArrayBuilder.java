/*
Copyright (c) 2013 James Ahlborn

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
*/

package com.healthmarketscience.jackcess.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Utility class for constructing {@code byte[]s} where the final size of the
 * data is not known beforehand.  The API is similar to {@code ByteBuffer} but
 * the data is not actually written to a {@code byte[]} until {@link
 * #toBuffer} or {@link #toArray} is called.
 *
 * @author James Ahlborn
 */
public class ByteArrayBuilder 
{
  private int _pos;
  private final List<Data> _data = new ArrayList<Data>();

  public ByteArrayBuilder() {
  }

  public int position() {
    return _pos;
  }

  public ByteArrayBuilder reserveInt() {
    return reserve(4);
  }

  public ByteArrayBuilder reserveShort() {
    return reserve(2);
  }

  public ByteArrayBuilder reserve(int bytes) {
    _pos += bytes;
    return this;
  }

  public ByteArrayBuilder put(byte val) {
    return put(new ByteData(_pos, val));
  }

  public ByteArrayBuilder putInt(int val) {
    return putInt(_pos, val);
  }

  public ByteArrayBuilder putInt(int pos, int val) {
    return put(new IntData(pos, val));
  }

  public ByteArrayBuilder putShort(short val) {
    return putShort(_pos, val);
  }

  public ByteArrayBuilder putShort(int pos, short val) {
    return put(new ShortData(pos, val));
  }

  public ByteArrayBuilder put(byte[] val) {
    return put(new BytesData(_pos, val));
  }

  public ByteArrayBuilder put(ByteBuffer val) {
    return put(new BufData(_pos, val));
  }

  private ByteArrayBuilder put(Data data) {
    _data.add(data);
    int endPos = data.getEndPos();
    if(endPos > _pos) {
      _pos = endPos;
    }
    return this;
  }

  public ByteBuffer toBuffer() {
    return toBuffer(PageChannel.wrap(new byte[_pos]));
  }

  public ByteBuffer toBuffer(ByteBuffer buf) {
    for(Data data : _data) {
      data.write(buf);
    }
    buf.rewind();
    return buf;
  }

  public byte[] toArray() {
    return toBuffer().array();
  }

  private static abstract class Data
  {
    private int _pos;
    
    protected Data(int pos) {
      _pos = pos;
    }

    public int getPos() {
      return _pos;
    }

    public int getEndPos() {
      return getPos() + size();
    }

    public abstract int size();

    public abstract void write(ByteBuffer buf);
  }

  private static final class IntData extends Data
  {
    private final int _val;

    private IntData(int pos, int val) {
      super(pos);
      _val = val;
    }

    @Override
    public int size() {
      return 4;
    }

    @Override
    public void write(ByteBuffer buf) {
      buf.putInt(getPos(), _val);
    }
  }

  private static final class ShortData extends Data
  {
    private final short _val;

    private ShortData(int pos, short val) {
      super(pos);
      _val = val;
    }

    @Override
    public int size() {
      return 2;
    }

    @Override
    public void write(ByteBuffer buf) {
      buf.putShort(getPos(), _val);
    }
  }

  private static final class ByteData extends Data
  {
    private final byte _val;

    private ByteData(int pos, byte val) {
      super(pos);
      _val = val;
    }

    @Override
    public int size() {
      return 1;
    }

    @Override
    public void write(ByteBuffer buf) {
      buf.put(getPos(), _val);
    }
  }

  private static final class BytesData extends Data
  {
    private final byte[] _val;

    private BytesData(int pos, byte[] val) {
      super(pos);
      _val = val;
    }

    @Override
    public int size() {
      return _val.length;
    }

    @Override
    public void write(ByteBuffer buf) {
      buf.position(getPos());
      buf.put(_val);
    }
  }

  private static final class BufData extends Data
  {
    private final ByteBuffer _val;

    private BufData(int pos, ByteBuffer val) {
      super(pos);
      _val = val;
    }

    @Override
    public int size() {
      return _val.remaining();
    }

    @Override
    public void write(ByteBuffer buf) {
      buf.position(getPos());
      buf.put(_val);
    }
  }
}
