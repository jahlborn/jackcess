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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Table;

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
public class VersionHistoryColumnInfo extends ComplexColumnInfo<Version> 
{
  private final Column _valueCol;
  private final Column _modifiedCol;
  
  public VersionHistoryColumnInfo(Column column, int complexId,
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
    versionedCol.setVersionHistoryColumn(getColumn());
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
  public int updateValue(Version value) throws IOException {
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
                                   List<Map<String,Object>> rawValues)
    throws IOException
  {
    List<Version> versions = super.toValues(complexValueFk, rawValues);

    // order versions newest to oldest
    Collections.sort(versions);
    
    return versions;
  }

  @Override
  protected VersionImpl toValue(ComplexValueForeignKey complexValueFk,
                                Map<String,Object> rawValue) {
    int id = (Integer)getPrimaryKeyColumn().getRowValue(rawValue);
    String value = (String)getValueColumn().getRowValue(rawValue);
    Date modifiedDate = (Date)getModifiedDateColumn().getRowValue(rawValue);

    return new VersionImpl(id, complexValueFk, value, modifiedDate);
  }

  @Override
  protected Object[] asRow(Object[] row, Version version) {
    super.asRow(row, version);
    getValueColumn().setRowValue(row, version.getValue());
    getModifiedDateColumn().setRowValue(row, version.getModifiedDate());
    return row;
  }
  
  public static Version newVersion(String value, Date modifiedDate) {
    return newVersion(INVALID_COMPLEX_VALUE_ID, value, modifiedDate);
  }
  
  public static Version newVersion(ComplexValueForeignKey complexValueFk,
                                   String value, Date modifiedDate) {
    return new VersionImpl(INVALID_ID, complexValueFk, value, modifiedDate);
  }

  public static boolean isVersionHistoryColumn(Table typeObjTable) {
    // version history data has these columns <value>(MEMO),
    // <modified>(SHORT_DATE_TIME)
    List<Column> typeCols = typeObjTable.getColumns();
    if(typeCols.size() < 2) {
      return false;
    }

    int numMemo = 0;
    int numDate = 0;
    
    for(Column col : typeCols) {
      switch(col.getType()) {
      case SHORT_DATE_TIME:
        ++numDate;
        break;
      case MEMO:
        ++numMemo;
        break;
      default:
        // ignore
      }
    }

    // be flexible, allow for extra columns...
    return((numMemo >= 1) && (numDate >= 1));
  }

  private static class VersionImpl extends ComplexValueImpl implements Version
  {
    private final String _value;
    private final Date _modifiedDate;

    private VersionImpl(int id, ComplexValueForeignKey complexValueFk,
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
      int id1 = getId();
      int id2 = o.getId();
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
