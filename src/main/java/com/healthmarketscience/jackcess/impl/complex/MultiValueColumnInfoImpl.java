/*
Copyright (c) 2011 James Ahlborn

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
