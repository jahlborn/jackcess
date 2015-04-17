/*
Copyright (c) 2005 Health Market Science, Inc.

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

import java.nio.ByteBuffer;

/**
 * Bitmask that indicates whether or not each column in a row is null.  Also
 * holds values of boolean columns.
 * @author Tim McCune
 */
public class NullMask {
  
  /** num row columns */
  private final int _columnCount;
  /** The actual bitmask */
  private final byte[] _mask;
  
  /**
   * @param columnCount Number of columns in the row that this mask will be
   *    used for
   */
  public NullMask(int columnCount) {
    _columnCount = columnCount;
    // we leave everything initially marked as null so that we don't need to
    // do anything for deleted columns (we only need to mark as non-null
    // valid columns for which we actually have values).
    _mask = new byte[(_columnCount + 7) / 8];
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
  public boolean isNull(ColumnImpl column) {
    int columnNumber = column.getColumnNumber();
    // if new columns were added to the table, old null masks may not include
    // them (meaning the field is null)
    if(columnNumber >= _columnCount) {
      // it's null
      return true;
    }
    return (_mask[byteIndex(columnNumber)] & bitMask(columnNumber)) == 0;
  }

  /**
   * Indicate that the column with the given number is not {@code null} (or a
   * boolean value is {@code true}).
   * @param column column to be marked non-{@code null}
   */
  public void markNotNull(ColumnImpl column) {
    int columnNumber = column.getColumnNumber();
    int maskIndex = byteIndex(columnNumber);
    _mask[maskIndex] = (byte) (_mask[maskIndex] | bitMask(columnNumber));
  }
  
  /**
   * @return Size in bytes of this mask
   */
  public int byteSize() {
    return _mask.length;
  }

  private static int byteIndex(int columnNumber) {
    return columnNumber / 8;
  }

  private static byte bitMask(int columnNumber) {
    return (byte) (1 << (columnNumber % 8));
  }
}
