/*
Copyright (c) 2013 James Ahlborn

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
