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

import java.nio.ByteBuffer;

/**
 * Bitmask that indicates whether or not each column in a row is null.  Also
 * holds values of boolean columns.
 * @author Tim McCune
 */
public class NullMask {
  
  /** The actual bitmask */
  private byte[] _mask;
  
  /**
   * @param columnCount Number of columns in the row that this mask will be
   *    used for
   */
  public NullMask(int columnCount) {
    _mask = new byte[(columnCount + 7) / 8];
    for (int i = 0; i < _mask.length; i++) {
      _mask[i] = (byte) 0xff;
    }
    for (int i = columnCount; i < _mask.length * 8; i++) {
      markNull(i);
    }
  }
  
  /**
   * Read a mask in from a buffer
   */
  public void read(ByteBuffer buffer) {
    buffer.get(_mask);
  }
  
  public ByteBuffer wrap() {
    return ByteBuffer.wrap(_mask);
  }
  
  /**
   * @param columnNumber 0-based column number in this mask's row
   * @return Whether or not the value for that column is null.  For boolean
   *    columns, returns the actual value of the column.
   */
  public boolean isNull(int columnNumber) {
    return (_mask[columnNumber / 8] & (byte) (1 << (columnNumber % 8))) == 0;
  }
  
  public void markNull(int columnNumber) {
    int maskIndex = columnNumber / 8;
    _mask[maskIndex] = (byte) (_mask[maskIndex] & (byte) ~(1 << (columnNumber % 8)));
  }
  
  /**
   * @return Size in bytes of this mask
   */
  public int byteSize() {
    return _mask.length;
  }
  
}
