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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.JetFormat;
import com.healthmarketscience.jackcess.PageChannel;
import com.healthmarketscience.jackcess.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Base class for the additional information tracked for complex columns.
 *
 * @author James Ahlborn
 */
public abstract class ComplexColumnInfo<V extends ComplexValue>
{
  private static final Log LOG = LogFactory.getLog(Column.class);

  public static final int INVALID_ID = -1;
  public static final ComplexValueForeignKey INVALID_COMPLEX_VALUE_ID =
    new ComplexValueForeignKey(null, INVALID_ID);
  
  private static final String COL_COMPLEX_TYPE_OBJECT_ID = "ComplexTypeObjectID";
  private static final String COL_TABLE_ID = "ConceptualTableID";
  private static final String COL_FLAT_TABLE_ID = "FlatTableID";

  private final Column _column;
  private final int _complexTypeId;
  private final Table _flatTable;
  private final List<Column> _typeCols;
  private final Column _pkCol;
  private final Column _complexValFkCol;
  private IndexCursor _pkCursor;
  private IndexCursor _complexValIdCursor;
  
  protected ComplexColumnInfo(Column column, int complexTypeId,
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

  public static ComplexColumnInfo<? extends ComplexValue> create(
      Column column, ByteBuffer buffer, int offset)
    throws IOException
  {
    int complexTypeId = buffer.getInt(
        offset + column.getFormat().OFFSET_COLUMN_COMPLEX_ID);

    Database db = column.getDatabase();
    Table complexColumns = db.getSystemComplexColumns();
    IndexCursor cursor = IndexCursor.createCursor(
        complexColumns, complexColumns.getPrimaryKeyIndex());
    if(!cursor.findFirstRowByEntry(complexTypeId)) {
      throw new IOException(
          "Could not find complex column info for complex column with id " +
          complexTypeId);
    }
    Map<String,Object> cColRow = cursor.getCurrentRow();
    int tableId = (Integer)cColRow.get(COL_TABLE_ID);
    if(tableId != column.getTable().getTableDefPageNumber()) {
      throw new IOException(
          "Found complex column for table " + tableId + " but expected table " +
          column.getTable().getTableDefPageNumber());
    }
    int flatTableId = (Integer)cColRow.get(COL_FLAT_TABLE_ID);
    int typeObjId = (Integer)cColRow.get(COL_COMPLEX_TYPE_OBJECT_ID);

    Table typeObjTable = db.getTable(typeObjId);
    Table flatTable = db.getTable(flatTableId);

    if((typeObjTable == null) || (flatTable == null)) {
      throw new IOException(
          "Could not find supporting tables (" + typeObjId + ", " + flatTableId
          + ") for complex column with id " + complexTypeId);
    }
    
    // we inspect the structore of the "type table" to determine what kind of
    // complex info we are dealing with
    if(MultiValueColumnInfo.isMultiValueColumn(typeObjTable)) {
      return new MultiValueColumnInfo(column, complexTypeId, typeObjTable,
                                      flatTable);
    } else if(AttachmentColumnInfo.isAttachmentColumn(typeObjTable)) {
      return new AttachmentColumnInfo(column, complexTypeId, typeObjTable,
                                      flatTable);
    } else if(VersionHistoryColumnInfo.isVersionHistoryColumn(typeObjTable)) {
      return new VersionHistoryColumnInfo(column, complexTypeId, typeObjTable,
                                          flatTable);
    }
    
    LOG.warn("Unsupported complex column type " + typeObjTable.getName());
    return new UnsupportedColumnInfo(column, complexTypeId, typeObjTable,
                                     flatTable);
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
  
  public JetFormat getFormat() {
    return getDatabase().getFormat();
  }

  public PageChannel getPageChannel() {
    return getDatabase().getPageChannel();
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
  
  public List<Map<String,Object>> getRawValues(int complexValueFk)
    throws IOException
  {
    return getRawValues(complexValueFk, null);
  }

  private Iterator<Map<String,Object>> getComplexValFkIter(
      int complexValueFk, Collection<String> columnNames)
    throws IOException
  {
    if(_complexValIdCursor == null) {
      _complexValIdCursor = new CursorBuilder(_flatTable)
        .setIndexByColumns(_complexValFkCol)
        .toIndexCursor();
    }

    return _complexValIdCursor.entryIterator(columnNames, complexValueFk);
  }
  
  public List<Map<String,Object>> getRawValues(int complexValueFk,
                                               Collection<String> columnNames)
    throws IOException
  {
    Iterator<Map<String,Object>> entryIter =
      getComplexValFkIter(complexValueFk, columnNames);
    if(!entryIter.hasNext()) {
      return Collections.emptyList();
    }

    List<Map<String,Object>> values = new ArrayList<Map<String,Object>>();
    while(entryIter.hasNext()) {
      values.add(entryIter.next());
    }
    
    return values;
  }

  public List<V> getValues(ComplexValueForeignKey complexValueFk)
    throws IOException
  {
    List<Map<String,Object>> rawValues = getRawValues(complexValueFk.get());
    if(rawValues.isEmpty()) {
      return Collections.emptyList();
    }

    return toValues(complexValueFk, rawValues);
  }
  
  protected List<V> toValues(ComplexValueForeignKey complexValueFk,
                             List<Map<String,Object>> rawValues)
    throws IOException
  {
    List<V> values = new ArrayList<V>();
    for(Map<String,Object> rawValue : rawValues) {
      values.add(toValue(complexValueFk, rawValue));
    }

    return values;
  }

  public int addRawValue(Map<String,Object> rawValue) throws IOException {
    Object[] row = _flatTable.asRow(rawValue);
    _flatTable.addRow(row);
    return (Integer)_pkCol.getRowValue(row);
  }

  public int addValue(V value) throws IOException {
    Object[] row = asRow(newRowArray(), value);
    _flatTable.addRow(row);
    int id = (Integer)_pkCol.getRowValue(row);
    value.setId(id);
    return id;
  }

  public void addValues(Collection<? extends V> values) throws IOException {
    for(V value : values) {
      addValue(value);
    }
  }

  public int updateRawValue(Map<String,Object> rawValue) throws IOException {
    Integer id = (Integer)_pkCol.getRowValue(rawValue);
    updateRow(id, _flatTable.asUpdateRow(rawValue));
    return id;
  }
  
  public int updateValue(V value) throws IOException {
    int id = value.getId();
    updateRow(id, asRow(newRowArray(), value));
    return id;
  }

  public void updateValues(Collection<? extends V> values) throws IOException {
    for(V value : values) {
      updateValue(value);
    }
  }

  public void deleteRawValue(Map<String,Object> rawValue) throws IOException {
    deleteRow((Integer)_pkCol.getRowValue(rawValue));
  }
  
  public void deleteValue(V value) throws IOException {
    deleteRow(value.getId());
  }

  public void deleteValues(Collection<? extends V> values) throws IOException {
    for(V value : values) {
      deleteValue(value);
    }
  }

  public void deleteAllValues(int complexValueFk) throws IOException {
    Iterator<Map<String,Object>> entryIter =
      getComplexValFkIter(complexValueFk, Collections.<String>emptySet());
    try {
      while(entryIter.hasNext()) {
        entryIter.next();
        entryIter.remove();
      }
    } catch(RuntimeException e) {
      if(e.getCause() instanceof IOException) {
        throw (IOException)e.getCause();
      }
      throw e;
    }
  }

  public void deleteAllValues(ComplexValueForeignKey complexValueFk)
    throws IOException
  {
    deleteAllValues(complexValueFk.get());
  }

  private void moveToRow(Integer id) throws IOException {
    if(_pkCursor == null) {
      _pkCursor = new CursorBuilder(_flatTable)
        .setIndexByColumns(_pkCol)
        .toIndexCursor();
    }

    if(!_pkCursor.findFirstRowByEntry(id)) {
      throw new IllegalArgumentException("Row with id " + id +
                                         " does not exist");
    }    
  }

  private void updateRow(Integer id, Object[] row) throws IOException {
    moveToRow(id);
    _pkCursor.updateCurrentRow(row);
  }
  
  private void deleteRow(Integer id) throws IOException {
    moveToRow(id);
    _pkCursor.deleteCurrentRow();
  }
  
  protected Object[] asRow(Object[] row, V value) {
    int id = value.getId();
    _pkCol.setRowValue(row, ((id != INVALID_ID) ? id : Column.AUTO_NUMBER));
    int cId = value.getComplexValueForeignKey().get();
    _complexValFkCol.setRowValue(
        row, ((cId != INVALID_ID) ? cId : Column.AUTO_NUMBER));
    return row;
  }

  private Object[] newRowArray() {
    return new Object[_flatTable.getColumnCount()];
  }
  
  @Override
  public String toString() {
    StringBuilder rtn = new StringBuilder();
    rtn.append("\n\t\tComplexType: " + getType());
    rtn.append("\n\t\tComplexTypeId: " + _complexTypeId);
    return rtn.toString();
  }

  protected static void diffFlatColumns(Table typeObjTable, Table flatTable,
                                        List<Column> typeCols,
                                        List<Column> otherCols)
  {
    // each "flat"" table has the columns from the "type" table, plus some
    // others.  separate the "flat" columns into these 2 buckets
    for(Column col : flatTable.getColumns()) {
      boolean found = false;
      try {
        typeObjTable.getColumn(col.getName());
        found = true;
      } catch(IllegalArgumentException e) {
        // FIXME better way to test this?
      }
      if(found) {
        typeCols.add(col);
      } else {
        otherCols.add(col);
      }  
    } 
  }
  
  public abstract ComplexDataType getType();

  protected abstract V toValue(
      ComplexValueForeignKey complexValueFk,
      Map<String,Object> rawValues)
    throws IOException;
  
  protected static abstract class ComplexValueImpl implements ComplexValue
  {
    private int _id;
    private ComplexValueForeignKey _complexValueFk;

    protected ComplexValueImpl(int id, ComplexValueForeignKey complexValueFk) {
      _id = id;
      _complexValueFk = complexValueFk;
    }

    public int getId() {
      return _id;
    }

    public void setId(int id) {
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
      if(_complexValueFk != INVALID_COMPLEX_VALUE_ID) {
        throw new IllegalStateException("complexValueFk may not be reset");
      }
      _complexValueFk = complexValueFk;
    }

    public Column getColumn() {
      return _complexValueFk.getColumn();
    }
    
    @Override
    public int hashCode() {
      return ((_id * 37) ^ _complexValueFk.hashCode());
    }

    @Override
    public boolean equals(Object o) {
      return ((this == o) ||
              ((o != null) && (getClass() == o.getClass()) &&
               (_id == ((ComplexValueImpl)o)._id) &&
               _complexValueFk.equals(((ComplexValueImpl)o)._complexValueFk)));
    }
  }
  
}
