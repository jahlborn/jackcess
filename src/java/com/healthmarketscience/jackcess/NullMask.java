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
    // we leave everything initially marked as null so that we don't need to
    // do anything for deleted columns (we only need to mark as non-null
    // valid columns for which we actually have values).
    _mask = new byte[(columnCount + 7) / 8];
  }
  
  /**
   * Read a mask in from a buffer
   */
  public void read(ByteBuffer buffer) {
    buffer.get(_mask);
  }

  /**
   * Write a mask to a buffer
   */
  public void write(ByteBuffer buffer) {
    buffer.put(_mask);
  }

  /**
   * @param column column to test for {@code null}
   * @return Whether or not the value for that column is null.  For boolean
   *    columns, returns the actual value of the column (where
   *    non-{@code null} == {@code true})
   */
  public boolean isNull(Column column) {
    int columnNumber = column.getColumnNumber();
    int maskIndex = columnNumber / 8;
    // if new columns were added to the table, old null masks may not include
    // them (meaning the field is null)
    if(maskIndex >= _mask.length) {
      // it's null
      return true;
    }
    return (_mask[maskIndex] & (byte) (1 << (columnNumber % 8))) == 0;
  }

  /**
   * Indicate that the column with the given number is not {@code null} (or a
   * boolean value is {@code true}).
   * @param column column to be marked non-{@code null}
   */
  public void markNotNull(Column column) {
    int columnNumber = column.getColumnNumber();
    int maskIndex = columnNumber / 8;
    _mask[maskIndex] = (byte) (_mask[maskIndex] | (byte) (1 << (columnNumber % 8)));
  }
  
  /**
   * @return Size in bytes of this mask
   */
  public int byteSize() {
    return _mask.length;
  }
  
}
