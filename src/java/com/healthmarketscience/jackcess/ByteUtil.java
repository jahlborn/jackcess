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

/**
 * Byte manipulation and display utilities
 * @author Tim McCune
 */
public final class ByteUtil {
  
  private static final String[] HEX_CHARS = new String[] {
      "0", "1", "2", "3", "4", "5", "6", "7",
      "8", "9", "A", "B", "C", "D", "E", "F"};
      
  private ByteUtil() {}
  
  /**
   * Convert an int from 4 bytes to 3
   * @param i Int to convert
   * @return Array of 3 bytes in little-endian order
   */    
  public static byte[] to3ByteInt(int i) {
    byte[] rtn = new byte[3];
    rtn[0] = (byte) (i & 0xFF);
    rtn[1] = (byte) ((i >>> 8) & 0xFF);
    rtn[2] = (byte) ((i >>> 16) & 0xFF);
    return rtn;
  }
  
  /**
   * Read a 3 byte int from a buffer in little-endian order
   * @param buffer Buffer containing the bytes
   * @param offset Offset at which to start reading the int
   * @return The int
   */
  public static int get3ByteInt(ByteBuffer buffer, int offset) {
    int rtn = buffer.get(offset) & 0xff;
    rtn += ((((int) buffer.get(offset + 1)) & 0xFF) << 8);
    rtn += ((((int) buffer.get(offset + 2)) & 0xFF) << 16);
    rtn &= 16777215;  //2 ^ (8 * 3) - 1
    return rtn;
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
      rtn.append(HEX_CHARS[(int) h]);
      h = (byte) (b & 0x0F);
      rtn.append(HEX_CHARS[(int) h]);

      if (formatted == true)
      {
        rtn.append(" ");

        if ((i + 1) % 4 == 0) {
          rtn.append(" ");
        }
        if ((i + 1) % 24 == 0) {
          rtn.append("\n");
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

  
}
