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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.RuntimeIOException;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.CustomToStringStyle;
import com.healthmarketscience.jackcess.impl.TableImpl;

/**
 * Base class for the additional information tracked for complex columns.
 *
 * @author James Ahlborn
 */
public abstract class ComplexColumnInfoImpl<V extends ComplexValue> 
  implements ComplexColumnInfo<V>
{
  private static final int INVALID_ID_VALUE = -1;
  public static final ComplexValue.Id INVALID_ID = new ComplexValueIdImpl(
      INVALID_ID_VALUE, null);
  public static final ComplexValueForeignKey INVALID_FK =
    new ComplexValueForeignKeyImpl(null, INVALID_ID_VALUE);

  private final Column _column;
  private final int _complexTypeId;
  private final Table _flatTable;
  private final List<Column> _typeCols;
  private final Column _pkCol;
  private final Column _complexValFkCol;
  private IndexCursor _complexValIdCursor;
  
  protected ComplexColumnInfoImpl(Column column, int complexTypeId,
                                  Table typeObjTable, Table flatTable)
    throws IOException
  {
    _column = column;
    _complexTypeId = complexTypeId;
    _flatTable = flatTable;
    
    // the flat table has all the "value" columns and 2 extra columns, a
    // primary key for each row, and a LONG value which is essentially a
    // foreign key to the main table.
    List<Column> typeCols = new ArrayList<Column>();
    List<Column> otherCols = new ArrayList<Column>();
    diffFlatColumns(typeObjTable, flatTable, typeCols, otherCols);

    _typeCols = Collections.unmodifiableList(typeCols);

    Column pkCol = null;
    Column complexValFkCol = null;
    for(Column col : otherCols) {
      if(col.isAutoNumber()) {
        pkCol = col;
      } else if(col.getType() == DataType.LONG) {
        complexValFkCol = col;
      }
    }

    if((pkCol == null) || (complexValFkCol == null)) {
      throw new IOException("Could not find expected columns in flat table " +
                            flatTable.getName() + " for complex column with id "
                            + complexTypeId);
    }
    _pkCol = pkCol;
    _complexValFkCol = complexValFkCol;
  }

  public void postTableLoadInit() throws IOException {
    // nothing to do in base class
  }
  
  public Column getColumn() {
    return _column;
  }

  public Database getDatabase() {
    return getColumn().getDatabase();
  }

  public Column getPrimaryKeyColumn() {
    return _pkCol;
  }

  public Column getComplexValueForeignKeyColumn() {
    return _complexValFkCol;
  }

  protected List<Column> getTypeColumns() {
    return _typeCols;
  }
  
  public int countValues(int complexValueFk) throws IOException {
    return getRawValues(complexValueFk,
                        Collections.singleton(_complexValFkCol.getName()))
      .size();
  }
  
  public List<Row> getRawValues(int complexValueFk)
    throws IOException
  {
    return getRawValues(complexValueFk, null);
  }

  private Iterator<Row> getComplexValFkIter(
      int complexValueFk, Collection<String> columnNames)
    throws IOException
  {
    if(_complexValIdCursor == null) {
      _complexValIdCursor = _flatTable.newCursor()
        .setIndexByColumns(_complexValFkCol)
        .toIndexCursor();
    }

    return _complexValIdCursor.newEntryIterable(complexValueFk)
      .setColumnNames(columnNames).iterator();
  }
  
  public List<Row> getRawValues(int complexValueFk,
                                Collection<String> columnNames)
    throws IOException
  {
    Iterator<Row> entryIter =
      getComplexValFkIter(complexValueFk, columnNames);
    if(!entryIter.hasNext()) {
      return Collections.emptyList();
    }

    List<Row> values = new ArrayList<Row>();
    while(entryIter.hasNext()) {
      values.add(entryIter.next());
    }
    
    return values;
  }

  public List<V> getValues(ComplexValueForeignKey complexValueFk)
    throws IOException
  {
    List<Row> rawValues = getRawValues(complexValueFk.get());
    if(rawValues.isEmpty()) {
      return Collections.emptyList();
    }

    return toValues(complexValueFk, rawValues);
  }
  
  protected List<V> toValues(ComplexValueForeignKey complexValueFk,
                             List<Row> rawValues)
    throws IOException
  {
    List<V> values = new ArrayList<V>();
    for(Row rawValue : rawValues) {
      values.add(toValue(complexValueFk, rawValue));
    }

    return values;
  }

  public ComplexValue.Id addRawValue(Map<String,?> rawValue)
    throws IOException 
  {
    Object[] row = ((TableImpl)_flatTable).asRowWithRowId(rawValue);
    _flatTable.addRow(row);
    return getValueId(row);
  }

  public ComplexValue.Id addValue(V value) throws IOException {
    Object[] row = asRow(newRowArray(), value);
    _flatTable.addRow(row);
    ComplexValue.Id id = getValueId(row);
    value.setId(id);
    return id;
  }

  public void addValues(Collection<? extends V> values) throws IOException {
    for(V value : values) {
      addValue(value);
    }
  }

  public ComplexValue.Id updateRawValue(Row rawValue) throws IOException {
    _flatTable.updateRow(rawValue);
    return getValueId(rawValue);
  }
  
  public ComplexValue.Id updateValue(V value) throws IOException {
    ComplexValue.Id id = value.getId();
    updateRow(id, asRow(newRowArray(), value));
    return id;
  }

  public void updateValues(Collection<? extends V> values) throws IOException {
    for(V value : values) {
      updateValue(value);
    }
  }

  public void deleteRawValue(Row rawValue) throws IOException {
    deleteRow(rawValue.getId());
  }
  
  public void deleteValue(V value) throws IOException {
    deleteRow(value.getId().getRowId());
  }

  public void deleteValues(Collection<? extends V> values) throws IOException {
    for(V value : values) {
      deleteValue(value);
    }
  }

  public void deleteAllValues(int complexValueFk) throws IOException {
    Iterator<Row> entryIter =
      getComplexValFkIter(complexValueFk, Collections.<String>emptySet());
    try {
      while(entryIter.hasNext()) {
        entryIter.next();
        entryIter.remove();
      }
    } catch(RuntimeIOException e) {
      throw (IOException)e.getCause();
    }
  }

  public void deleteAllValues(ComplexValueForeignKey complexValueFk)
    throws IOException
  {
    deleteAllValues(complexValueFk.get());
  }

  private void updateRow(ComplexValue.Id id, Object[] row) throws IOException {
    ((TableImpl)_flatTable).updateRow(id.getRowId(), row);
  }
  
  private void deleteRow(RowId rowId) throws IOException {
    ((TableImpl)_flatTable).deleteRow(rowId);
  }
  
  protected ComplexValueIdImpl getValueId(Row row) {
    int idVal = (Integer)getPrimaryKeyColumn().getRowValue(row);
    return new ComplexValueIdImpl(idVal, row.getId());
  }

  protected ComplexValueIdImpl getValueId(Object[] row) {
    int idVal = (Integer)getPrimaryKeyColumn().getRowValue(row);
    return new ComplexValueIdImpl(idVal, 
                                  ((TableImpl)_flatTable).getRowId(row));
  }

  protected Object[] asRow(Object[] row, V value) 
    throws IOException
  {
  ComplexValue.Id id = value.getId();
    _pkCol.setRowValue(
        row, ((id != INVALID_ID) ? id : Column.AUTO_NUMBER));
    ComplexValueForeignKey cFk = value.getComplexValueForeignKey();
    _complexValFkCol.setRowValue(
        row, ((cFk != INVALID_FK) ? cFk : Column.AUTO_NUMBER));
    return row;
  }

  private Object[] newRowArray() {
    Object[] row = new Object[_flatTable.getColumnCount() + 1];
    row[row.length - 1] = ColumnImpl.RETURN_ROW_ID;
    return row;
  }
  
  @Override
  public String toString() {
    return CustomToStringStyle.valueBuilder(this)
      .append("complexType", getType())
      .append("complexTypeId", _complexTypeId)
      .toString();
  }

  protected static void diffFlatColumns(Table typeObjTable, 
                                        Table flatTable,
                                        List<Column> typeCols,
                                        List<Column> otherCols)
  {
    // each "flat"" table has the columns from the "type" table, plus some
    // others.  separate the "flat" columns into these 2 buckets
    for(Column col : flatTable.getColumns()) {
      if(((TableImpl)typeObjTable).hasColumn(col.getName())) {
        typeCols.add(col);
      } else {
        otherCols.add(col);
      }  
    } 
  }
  
  public abstract ComplexDataType getType();

  protected abstract V toValue(
      ComplexValueForeignKey complexValueFk,
      Row rawValues)
    throws IOException;
  
  protected static abstract class ComplexValueImpl implements ComplexValue
  {
    private Id _id;
    private ComplexValueForeignKey _complexValueFk;

    protected ComplexValueImpl(Id id, ComplexValueForeignKey complexValueFk) {
      _id = id;
      _complexValueFk = complexValueFk;
    }

    public Id getId() {
      return _id;
    }

    public void setId(Id id) {
      if(_id == id) {
        // harmless, ignore
        return;
      }
      if(_id != INVALID_ID) {
        throw new IllegalStateException("id may not be reset");
      }
      _id = id;
    }
    
    public ComplexValueForeignKey getComplexValueForeignKey() {
      return _complexValueFk;
    }

    public void setComplexValueForeignKey(ComplexValueForeignKey complexValueFk)
    {
      if(_complexValueFk == complexValueFk) {
        // harmless, ignore
        return;
      }
      if(_complexValueFk != INVALID_FK) {
        throw new IllegalStateException("complexValueFk may not be reset");
      }
      _complexValueFk = complexValueFk;
    }

    public Column getColumn() {
      return _complexValueFk.getColumn();
    }
    
    @Override
    public int hashCode() {
      return ((_id.get() * 37) ^ _complexValueFk.hashCode());
    }

    @Override
    public boolean equals(Object o) {
      return ((this == o) ||
              ((o != null) && (getClass() == o.getClass()) &&
               (_id == ((ComplexValueImpl)o)._id) &&
               _complexValueFk.equals(((ComplexValueImpl)o)._complexValueFk)));
    }
  }

  /**
   * Implementation of ComplexValue.Id.
   */
  private static final class ComplexValueIdImpl extends ComplexValue.Id
  {
    private static final long serialVersionUID = 20130318L;    

    private final int _value;
    private final RowId _rowId;

    protected ComplexValueIdImpl(int value, RowId rowId) {
      _value = value;
      _rowId = rowId;
    }
    
    @Override
    public int get() {
      return _value;
    }

    @Override
    public RowId getRowId() {
      return _rowId;
    }
  }
  
}
