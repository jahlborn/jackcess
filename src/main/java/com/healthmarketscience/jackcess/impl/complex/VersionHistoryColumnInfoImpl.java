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
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.complex.Version;
import com.healthmarketscience.jackcess.complex.VersionHistoryColumnInfo;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

/**
 * Complex column info for a column which tracking the version history of an
 * "append only" memo column.
 * <p>
 * Note, the strongly typed update/delete methods are <i>not</i> supported for
 * version history columns (the data is supposed to be immutable).  That said,
 * the "raw" update/delete methods are supported for those that <i>really</i>
 * want to muck with the version history data.
 *
 * @author James Ahlborn
 */
public class VersionHistoryColumnInfoImpl extends ComplexColumnInfoImpl<Version> 
  implements VersionHistoryColumnInfo
{
  private final Column _valueCol;
  private final Column _modifiedCol;
  
  public VersionHistoryColumnInfoImpl(Column column, int complexId,
                                      Table typeObjTable, Table flatTable) 
    throws IOException
  {
    super(column, complexId, typeObjTable, flatTable);

    Column valueCol = null;
    Column modifiedCol = null;
    for(Column col : getTypeColumns()) {
      switch(col.getType()) {
      case SHORT_DATE_TIME:
        modifiedCol = col;
        break;
      case MEMO:
        valueCol = col;
        break;
      default:
        // ignore
      }
    }

    _valueCol = valueCol;
    _modifiedCol = modifiedCol;
  }

  @Override
  public void postTableLoadInit() throws IOException {
    super.postTableLoadInit();

    // link up with the actual versioned column.  it should have the same name
    // as the "value" column in the type table.
    Column versionedCol = getColumn().getTable().getColumn(
        getValueColumn().getName());
    ((ColumnImpl)versionedCol).setVersionHistoryColumn((ColumnImpl)getColumn());
  }
    
  public Column getValueColumn() {
    return _valueCol;
  }

  public Column getModifiedDateColumn() {
    return _modifiedCol;
  }
  
  @Override
  public ComplexDataType getType() {
    return ComplexDataType.VERSION_HISTORY;
  }

  @Override
  public ComplexValue.Id updateValue(Version value) throws IOException {
    throw new UnsupportedOperationException(
        "This column does not support value updates");
  }

  @Override
  public void deleteValue(Version value) throws IOException {
    throw new UnsupportedOperationException(
        "This column does not support value deletes");
  }

  @Override
  public void deleteAllValues(int complexValueFk) throws IOException {
    throw new UnsupportedOperationException(
        "This column does not support value deletes");
  }

  @Override
  protected List<Version> toValues(ComplexValueForeignKey complexValueFk,
                                   List<Row> rawValues)
    throws IOException
  {
    List<Version> versions = super.toValues(complexValueFk, rawValues);

    // order versions newest to oldest
    Collections.sort(versions);
    
    return versions;
  }

  @Override
  protected VersionImpl toValue(ComplexValueForeignKey complexValueFk,
                                Row rawValue) {
    ComplexValue.Id id = getValueId(rawValue);
    String value = (String)getValueColumn().getRowValue(rawValue);
    Date modifiedDate = (Date)getModifiedDateColumn().getRowValue(rawValue);

    return new VersionImpl(id, complexValueFk, value, modifiedDate);
  }

  @Override
  protected Object[] asRow(Object[] row, Version version) throws IOException {
    super.asRow(row, version);
    getValueColumn().setRowValue(row, version.getValue());
    getModifiedDateColumn().setRowValue(row, version.getModifiedDate());
    return row;
  }
  
  public static Version newVersion(String value, Date modifiedDate) {
    return newVersion(INVALID_FK, value, modifiedDate);
  }
  
  public static Version newVersion(ComplexValueForeignKey complexValueFk,
                                   String value, Date modifiedDate) {
    return new VersionImpl(INVALID_ID, complexValueFk, value, modifiedDate);
  }


  private static class VersionImpl extends ComplexValueImpl implements Version
  {
    private final String _value;
    private final Date _modifiedDate;

    private VersionImpl(Id id, ComplexValueForeignKey complexValueFk,
                        String value, Date modifiedDate)
    {
      super(id, complexValueFk);
      _value = value;
      _modifiedDate = modifiedDate;
    }
    
    public String getValue() {
      return _value;
    }

    public Date getModifiedDate() {
      return _modifiedDate;
    }    
    
    public int compareTo(Version o) {
      Date d1 = getModifiedDate();
      Date d2 = o.getModifiedDate();

      // sort by descending date (newest/greatest first)
      int cmp = d2.compareTo(d1);
      if(cmp != 0) {
        return cmp;
      }

      // use id, then complexValueFk to break ties (although we really
      // shouldn't be comparing across different columns)
      int id1 = getId().get();
      int id2 = o.getId().get();
      if(id1 != id2) {
        return ((id1 > id2) ? -1 : 1);
      }
      id1 = getComplexValueForeignKey().get();
      id2 = o.getComplexValueForeignKey().get();
      return ((id1 > id2) ? -1 :
              ((id1 < id2) ? 1 : 0));
    }

    public void update() throws IOException {
      throw new UnsupportedOperationException(
          "This column does not support value updates");
    }
    
    public void delete() throws IOException {
      throw new UnsupportedOperationException(
          "This column does not support value deletes");
    }

    @Override
    public String toString()
    {
      return "Version(" + getComplexValueForeignKey() + "," + getId() + ") " +
        getModifiedDate() + ", " + getValue();
    } 
  }
  
}
