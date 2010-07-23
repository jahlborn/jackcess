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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Byte manipulation and display utilities
 * @author Tim McCune
 */
public final class ByteUtil {
  
  private static final String[] HEX_CHARS = new String[] {
      "0", "1", "2", "3", "4", "5", "6", "7",
      "8", "9", "A", "B", "C", "D", "E", "F"};

  private static final int NUM_BYTES_PER_BLOCK = 4;
  private static final int NUM_BYTES_PER_LINE = 24;
      
  private ByteUtil() {}

  /**
   * Put an integer into the given buffer at the given offset as a 3-byte
   * integer.
   * @param buffer buffer into which to insert the int
   * @param val Int to convert
   */    
  public static void put3ByteInt(ByteBuffer buffer, int val)
  {
    put3ByteInt(buffer, val, buffer.order());
  }
  
  /**
   * Put an integer into the given buffer at the given offset as a 3-byte
   * integer.
   * @param buffer buffer into which to insert the int
   * @param val Int to convert
   * @param order  the order to insert the bytes of the int
   */    
  public static void put3ByteInt(ByteBuffer buffer, int val, ByteOrder order)
  {
    int pos = buffer.position();
    put3ByteInt(buffer, val, pos, order);
    buffer.position(pos + 3);
  }
  
  /**
   * Put an integer into the given buffer at the given offset as a 3-byte
   * integer.
   * @param buffer buffer into which to insert the int
   * @param val Int to convert
   * @param offset offset at which to insert the int
   * @param order  the order to insert the bytes of the int
   */    
  public static void put3ByteInt(ByteBuffer buffer, int val, int offset,
                                 ByteOrder order) {

    int offInc = 1;
    if(order == ByteOrder.BIG_ENDIAN) {
      offInc = -1;
      offset += 2;
    }

    buffer.put(offset, (byte) (val & 0xFF));
    buffer.put(offset + (1 * offInc), (byte) ((val >>> 8) & 0xFF));
    buffer.put(offset + (2 * offInc), (byte) ((val >>> 16) & 0xFF));
  }

  /**
   * Read a 3 byte int from a buffer
   * @param buffer Buffer containing the bytes
   * @return The int
   */
  public static int get3ByteInt(ByteBuffer buffer) {
    return get3ByteInt(buffer, buffer.order());
  }

  /**
   * Read a 3 byte int from a buffer
   * @param buffer Buffer containing the bytes
   * @param order  the order of the bytes of the int
   * @return The int
   */
  public static int get3ByteInt(ByteBuffer buffer, ByteOrder order) {  
    int pos = buffer.position();
    int rtn = get3ByteInt(buffer, pos, order);
    buffer.position(pos + 3);
    return rtn;
  }

  /**
   * Read a 3 byte int from a buffer
   * @param buffer Buffer containing the bytes
   * @param offset Offset at which to start reading the int
   * @return The int
   */
  public static int get3ByteInt(ByteBuffer buffer, int offset) {
    return get3ByteInt(buffer, offset, buffer.order());
  }
  
  /**
   * Read a 3 byte int from a buffer
   * @param buffer Buffer containing the bytes
   * @param offset Offset at which to start reading the int
   * @param order  the order of the bytes of the int
   * @return The int
   */
  public static int get3ByteInt(ByteBuffer buffer, int offset,
                                ByteOrder order) {

    int offInc = 1;
    if(order == ByteOrder.BIG_ENDIAN) {
      offInc = -1;
      offset += 2;
    }
    
    int rtn = getUnsignedByte(buffer, offset);
    rtn += (getUnsignedByte(buffer, offset + (1 * offInc)) << 8);
    rtn += (getUnsignedByte(buffer, offset + (2 * offInc)) << 16);
    return rtn;
  }

  /**
   * Read an unsigned byte from a buffer
   * @param buffer Buffer containing the bytes
   * @return The unsigned byte as an int
   */
  public static int getUnsignedByte(ByteBuffer buffer) {
    int pos = buffer.position();
    int rtn = getUnsignedByte(buffer, pos);
    buffer.position(pos + 1);
    return rtn;
  }

  /**
   * Read an unsigned byte from a buffer
   * @param buffer Buffer containing the bytes
   * @param offset Offset at which to read the byte
   * @return The unsigned byte as an int
   */
  public static int getUnsignedByte(ByteBuffer buffer, int offset) {  
    return asUnsignedByte(buffer.get(offset));
  }
  
  /**
   * Read an unsigned short from a buffer
   * @param buffer Buffer containing the short
   * @return The unsigned short as an int
   */
  public static int getUnsignedShort(ByteBuffer buffer) {
    int pos = buffer.position();
    int rtn = getUnsignedShort(buffer, pos);
    buffer.position(pos + 2);
    return rtn;
  }

  /**
   * Read an unsigned short from a buffer
   * @param buffer Buffer containing the short
   * @param offset Offset at which to read the short
   * @return The unsigned short as an int
   */
  public static int getUnsignedShort(ByteBuffer buffer, int offset) {  
    return asUnsignedShort(buffer.getShort(offset));
  }

  
  /**
   * @param buffer Buffer containing the bytes
   * @param order  the order of the bytes of the int
   * @return an int from the current position in the given buffer, read using
   *         the given ByteOrder
   */
  public static int getInt(ByteBuffer buffer, ByteOrder order) {
    int offset = buffer.position();
    int rtn = getInt(buffer, offset, order);
    buffer.position(offset + 4);
    return rtn;
  }
  
  /**
   * @param buffer Buffer containing the bytes
   * @param offset Offset at which to start reading the int
   * @param order  the order of the bytes of the int
   * @return an int from the given position in the given buffer, read using
   *         the given ByteOrder
   */
  public static int getInt(ByteBuffer buffer, int offset, ByteOrder order) {
    ByteOrder origOrder = buffer.order();
    try {
      return buffer.order(order).getInt(offset);
    } finally {
      buffer.order(origOrder);
    }
  }
  
  /**
   * Writes an int at the current position in the given buffer, using the
   * given ByteOrder
   * @param buffer buffer into which to insert the int
   * @param val Int to insert
   * @param order the order to insert the bytes of the int
   */
  public static void putInt(ByteBuffer buffer, int val, ByteOrder order) {
    int offset = buffer.position();
    putInt(buffer, val, offset, order);
    buffer.position(offset + 4);
  }
  
  /**
   * Writes an int at the given position in the given buffer, using the
   * given ByteOrder
   * @param buffer buffer into which to insert the int
   * @param val Int to insert
   * @param offset offset at which to insert the int
   * @param order the order to insert the bytes of the int
   */
  public static void putInt(ByteBuffer buffer, int val, int offset,
                            ByteOrder order)
  {
    ByteOrder origOrder = buffer.order();
    try {
      buffer.order(order).putInt(offset, val);
    } finally {
      buffer.order(origOrder);
    }
  }

  /**
   * Read an unsigned variable length int from a buffer
   * @param buffer Buffer containing the variable length int
   * @return The unsigned int
   */
  public static int getUnsignedVarInt(ByteBuffer buffer, int numBytes) {
    int pos = buffer.position();
    int rtn = getUnsignedVarInt(buffer, pos, numBytes);
    buffer.position(pos + numBytes);
    return rtn;
  }

  /**
   * Read an unsigned variable length int from a buffer
   * @param buffer Buffer containing the variable length int
   * @param offset Offset at which to read the value
   * @return The unsigned int
   */
  public static int getUnsignedVarInt(ByteBuffer buffer, int offset, 
                                      int numBytes) {  
    switch(numBytes) {
    case 1:
      return getUnsignedByte(buffer, offset);
    case 2:
      return getUnsignedShort(buffer, offset);
    case 3:
      return get3ByteInt(buffer, offset);
    case 4:
      return buffer.getInt(offset);
    default:
      throw new IllegalArgumentException("Invalid num bytes " + numBytes);
    }
  }

  
  /**
   * Sets all bits in the given remaining byte range to 0.
   */
  public static void clearRemaining(ByteBuffer buffer)
  {
    if(!buffer.hasRemaining()) {
      return;
    }
    int pos = buffer.position();
    clearRange(buffer, pos, pos + buffer.remaining());
  }

  /**
   * Sets all bits in the given byte range to 0.
   */
  public static void clearRange(ByteBuffer buffer, int start,
                                int end)
  {
    putRange(buffer, start, end, (byte)0x00);
  }

  /**
   * Sets all bits in the given byte range to 1.
   */
  public static void fillRange(ByteBuffer buffer, int start,
                               int end)
  {
    putRange(buffer, start, end, (byte)0xff);
  }
  
  /**
   * Sets all bytes in the given byte range to the given byte value.
   */
  public static void putRange(ByteBuffer buffer, int start,
                              int end, byte b)
  {
    for(int i = start; i < end; ++i) {
      buffer.put(i, b);
    }
  }

  /**
   * Matches a pattern of bytes against the given buffer starting at the given
   * offset.
   */
  public static boolean matchesRange(ByteBuffer buffer, int start,
                                     byte[] pattern)
  {
    for(int i = 0; i < pattern.length; ++i) {
      if(pattern[i] != buffer.get(start + i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Searches for a pattern of bytes in the given buffer starting at the
   * given offset.
   * @return the offset of the pattern if a match is found, -1 otherwise
   */
  public static int findRange(ByteBuffer buffer, int start, byte[] pattern)
  {
    byte firstByte = pattern[0];
    int limit = buffer.limit() - pattern.length;
    for(int i = start; i < limit; ++i) {
      if((firstByte == buffer.get(i)) && matchesRange(buffer, i, pattern)) {
        return i;
      }
    }
    return -1;
  }
  
  /**
   * Convert a byte buffer to a hexadecimal string for display
   * @param buffer Buffer to display, starting at offset 0
   * @param size Number of bytes to read from the buffer
   * @return The display String
   */
  public static String toHexString(ByteBuffer buffer, int size) {
    return toHexString(buffer, 0, size);
  }
  
  /**
   * Convert a byte array to a hexadecimal string for display
   * @param array byte array to display, starting at offset 0
   * @return The display String
   */
  public static String toHexString(byte[] array) {
    return toHexString(ByteBuffer.wrap(array), 0, array.length);
  }
  
  /**
   * Convert a byte buffer to a hexadecimal string for display
   * @param buffer Buffer to display, starting at offset 0
   * @param offset Offset at which to start reading the buffer
   * @param size Number of bytes to read from the buffer
   * @return The display String
   */
  public static String toHexString(ByteBuffer buffer, int offset, int size) {
    return toHexString(buffer, offset, size, true);
  }    

  /**
   * Convert a byte buffer to a hexadecimal string for display
   * @param buffer Buffer to display, starting at offset 0
   * @param offset Offset at which to start reading the buffer
   * @param size Number of bytes to read from the buffer
   * @param formatted flag indicating if formatting is required
   * @return The display String
   */
  public static String toHexString(ByteBuffer buffer,
                                   int offset, int size, boolean formatted) {
    
    StringBuilder rtn = new StringBuilder();
    int position = buffer.position();
    buffer.position(offset);

    for (int i = 0; i < size; i++) {
      byte b = buffer.get();
      byte h = (byte) (b & 0xF0);
      h = (byte) (h >>> 4);
      h = (byte) (h & 0x0F);
      rtn.append(HEX_CHARS[h]);
      h = (byte) (b & 0x0F);
      rtn.append(HEX_CHARS[h]);

      int next = (i + 1);
      if(formatted && (next < size))
      {
        if((next % NUM_BYTES_PER_LINE) == 0) {

          rtn.append("\n");

        } else {
          
          rtn.append(" ");

          if ((next % NUM_BYTES_PER_BLOCK) == 0) {
            rtn.append(" ");
          }
        }
      }
    }

    buffer.position(position);
    return rtn.toString();
  }

  /**
   * Writes a sequence of hexidecimal values into the given buffer, where
   * every two characters represent one byte value.
   */
  public static void writeHexString(ByteBuffer buffer,
                                    String hexStr)
    throws IOException
  {
    char[] hexChars = hexStr.toCharArray();
    if((hexChars.length % 2) != 0) {
      throw new IOException("Hex string length must be even");
    }
    for(int i = 0; i < hexChars.length; i += 2) {
      String tmpStr = new String(hexChars, i, 2);
      buffer.put((byte)Long.parseLong(tmpStr, 16));
    }
  }

  /**
   * Writes a chunk of data to a file in pretty printed hexidecimal.
   */
  public static void toHexFile(
      String fileName,
      ByteBuffer buffer, 
      int offset, int size)
    throws IOException
  {
    PrintWriter writer = new PrintWriter(
        new FileWriter(fileName));
    try {
      writer.println(toHexString(buffer, offset, size));
    } finally {
      writer.close();
    }
  }

  /**
   * @return the byte value converted to an unsigned int value
   */
  public static int asUnsignedByte(byte b) { 
    return b & 0xFF;
  }
  
  /**
   * @return the short value converted to an unsigned int value
   */
  public static int asUnsignedShort(short s) { 
    return s & 0xFFFF;
  }

  /**
   * Swaps the 4 bytes (changes endianness) of the bytes at the given offset.
   *
   * @param bytes buffer containing bytes to swap
   * @param offset offset of the first byte of the bytes to swap
   */
  public static void swap4Bytes(byte[] bytes, int offset)
  {
    byte b = bytes[offset + 0];
    bytes[offset + 0] = bytes[offset + 3];
    bytes[offset + 3] = b;
    b = bytes[offset + 1];
    bytes[offset + 1] = bytes[offset + 2];
    bytes[offset + 2] = b;
  }

  /**
   * Swaps the 2 bytes (changes endianness) of the bytes at the given offset.
   *
   * @param bytes buffer containing bytes to swap
   * @param offset offset of the first byte of the bytes to swap
   */
  public static void swap2Bytes(byte[] bytes, int offset)
  {
    byte b = bytes[offset + 0];
    bytes[offset + 0] = bytes[offset + 1];
    bytes[offset + 1] = b;
  }

  /**
   * Moves the position of the given buffer the given count from the current
   * position.
   * @return the new buffer position
   */
  public static int forward(ByteBuffer buffer, int count)
  {
    int newPos = buffer.position() + count;
    buffer.position(newPos);
    return newPos;
  }

  /**
   * Utility byte stream similar to ByteArrayOutputStream but with extended
   * accessibility to the bytes.
   */
  public static class ByteStream
  {
    private byte[] _bytes;
    private int _length;
    private int _lastLength;


    public ByteStream() {
      this(32);
    }

    public ByteStream(int capacity) {
      _bytes = new byte[capacity];
    }

    public int getLength() {
      return _length;
    }

    public byte[] getBytes() {
      return _bytes;
    }

    protected void ensureNewCapacity(int numBytes) {
      int newLength = _length + numBytes;
      if(newLength > _bytes.length) {
        byte[] temp = new byte[newLength * 2];
        System.arraycopy(_bytes, 0, temp, 0, _length);
        _bytes = temp;
      }
    }

    public void write(int b) {
      ensureNewCapacity(1);
      _bytes[_length++] = (byte)b;
    }

    public void write(byte[] b) {
      write(b, 0, b.length);
    }

    public void write(byte[] b, int offset, int length) {
      ensureNewCapacity(length);
      System.arraycopy(b, offset, _bytes, _length, length);
      _length += length;
    }

    public byte get(int offset) {
      return _bytes[offset];
    }

    public void set(int offset, byte b) {
      _bytes[offset] = b;
    }

    public void writeFill(int length, byte b) {
      ensureNewCapacity(length);
      int oldLength = _length;
      _length += length;
      Arrays.fill(_bytes, oldLength, _length, b);
    }

    public void writeTo(ByteStream out) {
      out.write(_bytes, 0, _length);
    }

    public byte[] toByteArray() {

      byte[] result = null;
      if(_length == _bytes.length) {
        result = _bytes;
        _bytes = null;
      } else {
        result = new byte[_length];
        System.arraycopy(_bytes, 0, result, 0, _length);
        if(_lastLength == _length) {
          // if we get the same result length bytes twice in a row, clear the
          // _bytes so that the next _bytes will be _lastLength
          _bytes = null;
        }
      }

      // save result length so we can potentially get the right length of the
      // next byte[] in reset()
      _lastLength = _length;

      return result;
    }

    public void reset() {
      _length = 0;
      if(_bytes == null) {
        _bytes = new byte[_lastLength];
      }
    }

    public void trimTrailing(byte minTrimCode, byte maxTrimCode)
    {
      int minTrim = ByteUtil.asUnsignedByte(minTrimCode);
      int maxTrim = ByteUtil.asUnsignedByte(maxTrimCode);

      int idx = _length - 1;
      while(idx >= 0) {
        int val = asUnsignedByte(get(idx));
        if((val >= minTrim) && (val <= maxTrim)) {
          --idx;
        } else {
          break;
        }
      }

      _length = idx + 1;
    }
  }

}
