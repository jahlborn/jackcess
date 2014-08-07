/*
Copyright (c) 2014 James Ahlborn

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

import java.io.IOException;
import com.healthmarketscience.jackcess.DataType;
import java.nio.ByteBuffer;

/**
 * ColumnImpl subclass which is used for Text data types.
 *
 * @author James Ahlborn
 * @usage _advanced_class_
 */
class TextColumnImpl extends ColumnImpl 
{
  /** whether or not they are compressed */ 
  private final boolean _compressedUnicode;
  /** the collating sort order for a text field */
  private final SortOrder _sortOrder;
  /** the code page for a text field (for certain db versions) */
  private final short _codePage;

  TextColumnImpl(TableImpl table, ByteBuffer buffer, int offset, 
                 int displayIndex, DataType type, byte flags)
    throws IOException
  {
    super(table, buffer, offset, displayIndex, type, flags);

      // co-located w/ precision/scale
      _sortOrder = readSortOrder(
          buffer, offset + getFormat().OFFSET_COLUMN_SORT_ORDER, getFormat());
      _codePage = readCodePage(buffer, offset, getFormat());

      _compressedUnicode = ((buffer.get(offset +
        getFormat().OFFSET_COLUMN_COMPRESSED_UNICODE) & 1) == 1);
  }

  @Override
  public boolean isCompressedUnicode() {
    return _compressedUnicode;
  }
  
  @Override
  public short getTextCodePage() {
    return _codePage;
  }
  
  @Override
  public SortOrder getTextSortOrder() {
    return _sortOrder;
  }
}
