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
import java.nio.ByteOrder;


/**
 * ColumnImpl subclass which is used for unknown/unsupported data types.
 * 
 * @author James Ahlborn
 * @usage _advanced_class_
 */
class UnsupportedColumnImpl extends ColumnImpl 
{
  private final byte _originalType;
  
  UnsupportedColumnImpl(InitArgs args) throws IOException
  {
    super(args);
    _originalType = args.colType;
  }

  @Override
  byte getOriginalDataType() {
    return _originalType;
  }

  @Override
  public Object read(byte[] data, ByteOrder order) throws IOException {
    return rawDataWrapper(data);
  }
}
