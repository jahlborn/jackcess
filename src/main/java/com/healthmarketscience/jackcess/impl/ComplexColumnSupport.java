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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.impl.complex.AttachmentColumnInfoImpl;
import com.healthmarketscience.jackcess.impl.complex.MultiValueColumnInfoImpl;
import com.healthmarketscience.jackcess.impl.complex.UnsupportedColumnInfoImpl;
import com.healthmarketscience.jackcess.impl.complex.VersionHistoryColumnInfoImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Utility code for loading complex columns.
 *
 * @author James Ahlborn
 */
public class ComplexColumnSupport 
{
  private static final Log LOG = LogFactory.getLog(ComplexColumnSupport.class);

  private static final String COL_COMPLEX_TYPE_OBJECT_ID = "ComplexTypeObjectID";
  private static final String COL_TABLE_ID = "ConceptualTableID";
  private static final String COL_FLAT_TABLE_ID = "FlatTableID";

  private static final Set<DataType> MULTI_VALUE_TYPES = EnumSet.of(
      DataType.BYTE, DataType.INT, DataType.LONG, DataType.FLOAT,
      DataType.DOUBLE, DataType.GUID, DataType.NUMERIC, DataType.TEXT,
      DataType.BIG_INT);


  /**
   * Creates a ComplexColumnInfo for a complex column.
   */
  public static ComplexColumnInfo<? extends ComplexValue> create(
      ColumnImpl column, ByteBuffer buffer, int offset)
    throws IOException
  {
    int complexTypeId = buffer.getInt(
        offset + column.getFormat().OFFSET_COLUMN_COMPLEX_ID);

    DatabaseImpl db = column.getDatabase();
    TableImpl complexColumns = db.getSystemComplexColumns();
    IndexCursor cursor = CursorBuilder.createCursor(
        complexColumns.getPrimaryKeyIndex());
    if(!cursor.findFirstRowByEntry(complexTypeId)) {
      throw new IOException(column.withErrorContext(
          "Could not find complex column info for complex column with id " +
          complexTypeId));
    }
    Row cColRow = cursor.getCurrentRow();
    int tableId = cColRow.getInt(COL_TABLE_ID);
    if(tableId != column.getTable().getTableDefPageNumber()) {
      throw new IOException(column.withErrorContext(
          "Found complex column for table " + tableId + " but expected table " +
          column.getTable().getTableDefPageNumber()));
    }
    int flatTableId = cColRow.getInt(COL_FLAT_TABLE_ID);
    int typeObjId = cColRow.getInt(COL_COMPLEX_TYPE_OBJECT_ID);

    TableImpl typeObjTable = db.getTable(typeObjId);
    TableImpl flatTable = db.getTable(flatTableId);

    if((typeObjTable == null) || (flatTable == null)) {
      throw new IOException(column.withErrorContext(
          "Could not find supporting tables (" + typeObjId + ", " + flatTableId
          + ") for complex column with id " + complexTypeId));
    }
    
    // we inspect the structore of the "type table" to determine what kind of
    // complex info we are dealing with
    if(isMultiValueColumn(typeObjTable)) {
      return new MultiValueColumnInfoImpl(column, complexTypeId, typeObjTable,
                                      flatTable);
    } else if(isAttachmentColumn(typeObjTable)) {
      return new AttachmentColumnInfoImpl(column, complexTypeId, typeObjTable,
                                      flatTable);
    } else if(isVersionHistoryColumn(typeObjTable)) {
      return new VersionHistoryColumnInfoImpl(column, complexTypeId, typeObjTable,
                                          flatTable);
    }
    
    LOG.warn(column.withErrorContext(
                 "Unsupported complex column type " + typeObjTable.getName()));
    return new UnsupportedColumnInfoImpl(column, complexTypeId, typeObjTable,
                                         flatTable);
  }


  public static boolean isMultiValueColumn(Table typeObjTable) {
    // if we found a single value of a "simple" type, then we are dealing with
    // a multi-value column
    List<? extends Column> typeCols = typeObjTable.getColumns();
    return ((typeCols.size() == 1) &&
            MULTI_VALUE_TYPES.contains(typeCols.get(0).getType()));
  }

  public static boolean isAttachmentColumn(Table typeObjTable) {
    // attachment data has these columns FileURL(MEMO), FileName(TEXT),
    // FileType(TEXT), FileData(OLE), FileTimeStamp(SHORT_DATE_TIME),
    // FileFlags(LONG)
    List<? extends Column> typeCols = typeObjTable.getColumns();
    if(typeCols.size() < 6) {
      return false;
    }

    int numMemo = 0;
    int numText = 0;
    int numDate = 0;
    int numOle= 0;
    int numLong = 0;
    
    for(Column col : typeCols) {
      switch(col.getType()) {
      case TEXT:
        ++numText;
        break;
      case LONG:
        ++numLong;
        break;
      case SHORT_DATE_TIME:
        ++numDate;
        break;
      case OLE:
        ++numOle;
        break;
      case MEMO:
        ++numMemo;
        break;
      default:
        // ignore
      }
    }

    // be flexible, allow for extra columns...
    return((numMemo >= 1) && (numText >= 2) && (numOle >= 1) &&
           (numDate >= 1) && (numLong >= 1));
  }

  public static boolean isVersionHistoryColumn(Table typeObjTable) {
    // version history data has these columns <value>(MEMO),
    // <modified>(SHORT_DATE_TIME)
    List<? extends Column> typeCols = typeObjTable.getColumns();
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
}
