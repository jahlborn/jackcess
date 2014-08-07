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
import java.nio.ByteBuffer;

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.complex.ComplexColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.impl.complex.ComplexColumnInfoImpl;

/**
 * ColumnImpl subclass which is used for complex data types.
 * 
 * @author James Ahlborn
 * @usage _advanced_class_
 */
class ComplexColumnImpl extends ColumnImpl 
{
  /** additional information specific to complex columns */
  private final ComplexColumnInfo<? extends ComplexValue> _complexInfo;

  ComplexColumnImpl(TableImpl table, ByteBuffer buffer, int offset, 
                    int displayIndex, DataType type, byte flags)
    throws IOException
  {
    super(table, buffer, offset, displayIndex, type, flags);
    _complexInfo = ComplexColumnSupport.create(this, buffer, offset);
  }

  @Override
  void postTableLoadInit() throws IOException {
    if(_complexInfo != null) {
      ((ComplexColumnInfoImpl<? extends ComplexValue>)_complexInfo)
        .postTableLoadInit();
    }
    super.postTableLoadInit();
  }

  @Override
  public ComplexColumnInfo<? extends ComplexValue> getComplexInfo() {
    return _complexInfo;
  }
}
