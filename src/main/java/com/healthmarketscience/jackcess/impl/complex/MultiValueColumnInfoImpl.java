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

package com.healthmarketscience.jackcess.impl.complex;

import java.io.IOException;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.complex.MultiValueColumnInfo;
import com.healthmarketscience.jackcess.complex.SingleValue;

/**
 * Complex column info for a column holding multiple simple values per row.
 *
 * @author James Ahlborn
 */
public class MultiValueColumnInfoImpl extends ComplexColumnInfoImpl<SingleValue>
  implements MultiValueColumnInfo
{
  private final Column _valueCol;
  
  public MultiValueColumnInfoImpl(Column column, int complexId,
                                  Table typeObjTable, Table flatTable) 
    throws IOException
  {
    super(column, complexId, typeObjTable, flatTable);

    _valueCol = getTypeColumns().get(0);
  }

  @Override
  public ComplexDataType getType()
  {
    return ComplexDataType.MULTI_VALUE;
  }

  public Column getValueColumn() {
    return _valueCol;
  }

  @Override
  protected SingleValueImpl toValue(
      ComplexValueForeignKey complexValueFk,
      Row rawValue)
  {
    ComplexValue.Id id = getValueId(rawValue);
    Object value = getValueColumn().getRowValue(rawValue);

    return new SingleValueImpl(id, complexValueFk, value);
  }

  @Override
  protected Object[] asRow(Object[] row, SingleValue value) throws IOException {
    super.asRow(row, value);
    getValueColumn().setRowValue(row, value.get());
    return row;
  }
  
  public static SingleValue newSingleValue(Object value) {
    return newSingleValue(INVALID_FK, value);
  }

  public static SingleValue newSingleValue(
      ComplexValueForeignKey complexValueFk, Object value) {
    return new SingleValueImpl(INVALID_ID, complexValueFk, value);
  }


  private static class SingleValueImpl extends ComplexValueImpl
    implements SingleValue
  {
    private Object _value;

    private SingleValueImpl(Id id, ComplexValueForeignKey complexValueFk,
                            Object value)
    {
      super(id, complexValueFk);
      _value = value;
    }
    
    public Object get() {
      return _value;
    }

    public void set(Object value) {
      _value = value;
    }

    public void update() throws IOException {
      getComplexValueForeignKey().updateMultiValue(this);
    }
    
    public void delete() throws IOException {
      getComplexValueForeignKey().deleteMultiValue(this);
    }
    
    @Override
    public String toString()
    {
      return "SingleValue(" + getComplexValueForeignKey() + "," + getId() +
        ") " + get();
    } 
  }
}
