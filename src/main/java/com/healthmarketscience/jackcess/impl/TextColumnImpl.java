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

  TextColumnImpl(InitArgs args) throws IOException
  {
    super(args);

      // co-located w/ precision/scale
      _sortOrder = readSortOrder(
          args.buffer, args.offset + getFormat().OFFSET_COLUMN_SORT_ORDER,
          getFormat());
      _codePage = readCodePage(args.buffer, args.offset, getFormat());

      _compressedUnicode = 
        ((args.extFlags & COMPRESSED_UNICODE_EXT_FLAG_MASK) != 0);
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
