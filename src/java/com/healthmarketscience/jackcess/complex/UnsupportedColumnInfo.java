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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Table;

/**
 * Complex column info for an unsupported complex type.
 *
 * @author James Ahlborn
 */
public class UnsupportedColumnInfo extends ComplexColumnInfo<UnsupportedValue>
{

  public UnsupportedColumnInfo(Column column, int complexId, Table typeObjTable,
                               Table flatTable)
    throws IOException
  {
    super(column, complexId, typeObjTable, flatTable);
  }

  public List<Column> getValueColumns() {
    return getTypeColumns();
  }

  @Override
  public ComplexDataType getType()
  {
    return ComplexDataType.UNSUPPORTED;
  }

  @Override
  protected UnsupportedValueImpl toValue(
      ComplexValueForeignKey complexValueFk,
      Map<String,Object> rawValue)
  {
    int id = (Integer)getPrimaryKeyColumn().getRowValue(rawValue);

    Map<String,Object> values = new LinkedHashMap<String,Object>();
    for(Column col : getValueColumns()) {
      col.setRowValue(values, col.getRowValue(rawValue));
    }

    return new UnsupportedValueImpl(id, complexValueFk, values);
  }

  @Override
  protected Object[] asRow(Object[] row, UnsupportedValue value) {
    super.asRow(row, value);

    Map<String,Object> values = value.getValues();
    for(Column col : getValueColumns()) {
      col.setRowValue(row, col.getRowValue(values));
    }

    return row;
  }

  public static UnsupportedValue newValue(Map<String,?> values) {
    return newValue(INVALID_COMPLEX_VALUE_ID, values);
  }

  public static UnsupportedValue newValue(
      ComplexValueForeignKey complexValueFk, Map<String,?> values) {
    return new UnsupportedValueImpl(INVALID_ID, complexValueFk, 
                                    new LinkedHashMap<String,Object>(values));
  }
  
  private static class UnsupportedValueImpl extends ComplexValueImpl
    implements UnsupportedValue
  {
    private Map<String,Object> _values;

    private UnsupportedValueImpl(int id, ComplexValueForeignKey complexValueFk,
                                 Map<String,Object> values)
    {
      super(id, complexValueFk);
      _values = values;
    }

    public Map<String,Object> getValues() {
      return _values;
    }
    
    public Object get(String columnName) {
      return getValues().get(columnName);
    }

    public void set(String columnName, Object value) {
      getValues().put(columnName, value);
    }

    public void update() throws IOException {
      getComplexValueForeignKey().updateUnsupportedValue(this);
    }
    
    public void delete() throws IOException {
      getComplexValueForeignKey().deleteUnsupportedValue(this);
    }
    
    @Override
    public String toString()
    {
      return "UnsupportedValue(" + getComplexValueForeignKey() + "," + getId() +
        ") " + getValues();
    } 
  }
}
