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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Table;

/**
 * Complex column info for a column holding multiple values per row.
 *
 * @author James Ahlborn
 */
public class MultiValueColumnInfo extends ComplexColumnInfo<SingleValue>
{
  private static final Set<DataType> VALUE_TYPES = EnumSet.of(
      DataType.BYTE, DataType.INT, DataType.LONG, DataType.FLOAT,
      DataType.DOUBLE, DataType.GUID, DataType.NUMERIC, DataType.TEXT);

  private final Column _valueCol;
  
  public MultiValueColumnInfo(Column column, int complexId,
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
      Map<String,Object> rawValue)
  {
    int id = (Integer)getPrimaryKeyColumn().getRowValue(rawValue);
    Object value = getValueColumn().getRowValue(rawValue);

    return new SingleValueImpl(id, complexValueFk, value);
  }

  @Override
  protected Object[] asRow(Object[] row, SingleValue value) {
    super.asRow(row, value);
    getValueColumn().setRowValue(row, value.get());
    return row;
  }
  
  public static SingleValue newSingleValue(Object value) {
    return newSingleValue(INVALID_COMPLEX_VALUE_ID, value);
  }

  public static SingleValue newSingleValue(
      ComplexValueForeignKey complexValueFk, Object value) {
    return new SingleValueImpl(INVALID_ID, complexValueFk, value);
  }

  public static boolean isMultiValueColumn(Table typeObjTable) {
    // if we found a single value of a "simple" type, then we are dealing with
    // a multi-value column
    List<Column> typeCols = typeObjTable.getColumns();
    return ((typeCols.size() == 1) &&
            VALUE_TYPES.contains(typeCols.get(0).getType()));
  }

  private static class SingleValueImpl extends ComplexValueImpl
    implements SingleValue
  {
    private Object _value;

    private SingleValueImpl(int id, ComplexValueForeignKey complexValueFk,
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
