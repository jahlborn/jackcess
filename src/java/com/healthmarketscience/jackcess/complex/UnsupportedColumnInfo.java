/*
Copyright (c) 2011 James Ahlborn

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

package com.healthmarketscience.jackcess.complex;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Table;

/**
 *
 * @author James Ahlborn
 */
public class UnsupportedColumnInfo extends ComplexColumnInfo<ComplexValue>
{

  public UnsupportedColumnInfo(Column column, int complexId, Table typeObjTable,
                               Table flatTable)
    throws IOException
  {
    super(column, complexId, typeObjTable, flatTable);
  }

  @Override
  public ComplexDataType getType()
  {
    return ComplexDataType.UNSUPPORTED;
  }

  @Override
  protected List<ComplexValue> toValues(ComplexValueForeignKey complexValueFk,
                                        List<Map<String,Object>> rawValues)
    throws IOException
  {
    // FIXME
    return null;
  }

  public ComplexValue newValue() {
    // FIXME
    return null;
  }
  
}
