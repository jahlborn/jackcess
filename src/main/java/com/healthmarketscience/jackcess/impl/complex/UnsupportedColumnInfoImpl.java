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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.complex.UnsupportedColumnInfo;
import com.healthmarketscience.jackcess.complex.UnsupportedValue;

/**
 * Complex column info for an unsupported complex type.
 *
 * @author James Ahlborn
 */
public class UnsupportedColumnInfoImpl
  extends ComplexColumnInfoImpl<UnsupportedValue>
  implements UnsupportedColumnInfo
{

  public UnsupportedColumnInfoImpl(Column column, int complexId,
                                   Table typeObjTable, Table flatTable)
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
      Row rawValue)
  {
    ComplexValue.Id id = getValueId(rawValue);

    Map<String,Object> values = new LinkedHashMap<String,Object>();
    for(Column col : getValueColumns()) {
      col.setRowValue(values, col.getRowValue(rawValue));
    }

    return new UnsupportedValueImpl(id, complexValueFk, values);
  }

  @Override
  protected Object[] asRow(Object[] row, UnsupportedValue value)
    throws IOException
  {
    super.asRow(row, value);

    Map<String,Object> values = value.getValues();
    for(Column col : getValueColumns()) {
      col.setRowValue(row, col.getRowValue(values));
    }

    return row;
  }

  public static UnsupportedValue newValue(Map<String,?> values) {
    return newValue(INVALID_FK, values);
  }

  public static UnsupportedValue newValue(
      ComplexValueForeignKey complexValueFk, Map<String,?> values) {
    return new UnsupportedValueImpl(INVALID_ID, complexValueFk,
                                    new LinkedHashMap<String,Object>(values));
  }

  private static class UnsupportedValueImpl extends ComplexValueImpl
    implements UnsupportedValue
  {
    private final Map<String,Object> _values;

    private UnsupportedValueImpl(Id id, ComplexValueForeignKey complexValueFk,
                                 Map<String,Object> values)
    {
      super(id, complexValueFk);
      _values = values;
    }

    @Override
    public Map<String,Object> getValues() {
      return _values;
    }

    @Override
    public Object get(String columnName) {
      return getValues().get(columnName);
    }

    @Override
    public void set(String columnName, Object value) {
      getValues().put(columnName, value);
    }

    @Override
    public void update() throws IOException {
      getComplexValueForeignKey().updateUnsupportedValue(this);
    }

    @Override
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
