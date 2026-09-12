/*
Copyright (c) 2013 James Ahlborn

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

package com.healthmarketscience.jackcess.complex;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Row;

/**
 * Base class for the additional information tracked for complex columns.
 *
 * @author James Ahlborn
 */
public interface ComplexColumnInfo<V extends ComplexValue>
{
  public ComplexDataType getType();

  public int countValues(int complexValueFk) throws IOException;

  public List<Row> getRawValues(int complexValueFk)
    throws IOException;

  public List<Row> getRawValues(int complexValueFk,
                                Collection<String> columnNames)
    throws IOException;

  public List<V> getValues(ComplexValueForeignKey complexValueFk)
    throws IOException;

  public ComplexValue.Id addRawValue(Map<String,?> rawValue)
    throws IOException;

  public ComplexValue.Id addValue(V value) throws IOException;

  public void addValues(Collection<? extends V> values) throws IOException;

  public ComplexValue.Id updateRawValue(Row rawValue) throws IOException;

  public ComplexValue.Id updateValue(V value) throws IOException;

  public void updateValues(Collection<? extends V> values) throws IOException;

  public void deleteRawValue(Row rawValue) throws IOException;

  public void deleteValue(V value) throws IOException;

  public void deleteValues(Collection<? extends V> values) throws IOException;

  public void deleteAllValues(int complexValueFk) throws IOException;

  public void deleteAllValues(ComplexValueForeignKey complexValueFk)
    throws IOException;

}
