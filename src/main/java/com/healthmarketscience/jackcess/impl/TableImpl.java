/*
Copyright (c) 2005 Health Market Science, Inc.

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

You can contact Health Market Science at info@healthmarketscience.com
or at the following address:

Health Market Science
2700 Horizon Drive
Suite 200
King of Prussia, PA 19406
*/

package com.healthmarketscience.jackcess.impl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.BatchUpdateException;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.ConstraintViolationException;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.JackcessException;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.ErrorHandler;
import com.healthmarketscience.jackcess.util.ExportUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A single database table
 * <p>
 * Is not thread-safe.
 * 
 * @author Tim McCune
 * @usage _general_class_
 */
public class TableImpl implements Table
{  
  private static final Log LOG = LogFactory.getLog(TableImpl.class);

  private static final short OFFSET_MASK = (short)0x1FFF;

  private static final short DELETED_ROW_MASK = (short)0x8000;
  
  private static final short OVERFLOW_ROW_MASK = (short)0x4000;

  static final int MAGIC_TABLE_NUMBER = 1625;

  private static final int MAX_BYTE = 256;

  /**
   * Table type code for system tables
   * @usage _intermediate_class_
   */
  public static final byte TYPE_SYSTEM = 0x53;
  /**
   * Table type code for user tables
   * @usage _intermediate_class_
   */
  public static final byte TYPE_USER = 0x4e;

  /** comparator which sorts variable length columns based on their index into
      the variable length offset table */
  private static final Comparator<ColumnImpl> VAR_LEN_COLUMN_COMPARATOR =
    new Comparator<ColumnImpl>() {
      public int compare(ColumnImpl c1, ColumnImpl c2) {
        return ((c1.getVarLenTableIndex() < c2.getVarLenTableIndex()) ? -1 :
                ((c1.getVarLenTableIndex() > c2.getVarLenTableIndex()) ? 1 :
                 0));
      }
    };

  /** comparator which sorts columns based on their display index */
  private static final Comparator<ColumnImpl> DISPLAY_ORDER_COMPARATOR =
    new Comparator<ColumnImpl>() {
      public int compare(ColumnImpl c1, ColumnImpl c2) {
        return ((c1.getDisplayIndex() < c2.getDisplayIndex()) ? -1 :
                ((c1.getDisplayIndex() > c2.getDisplayIndex()) ? 1 :
                 0));
      }
    };

  /** owning database */
  private final DatabaseImpl _database;
  /** additional table flags from the catalog entry */
  private final int _flags;
  /** Type of the table (either TYPE_SYSTEM or TYPE_USER) */
  private final byte _tableType;
  /** Number of actual indexes on the table */
  private final int _indexCount;
  /** Number of logical indexes for the table */
  private final int _logicalIndexCount;
  /** page number of the definition of this table */
  private final int _tableDefPageNumber;
  /** max Number of columns in the table (includes previous deletions) */
  private final short _maxColumnCount;
  /** max Number of variable columns in the table */
  private final short _maxVarColumnCount;
  /** List of columns in this table, ordered by column number */
  private final List<ColumnImpl> _columns = new ArrayList<ColumnImpl>();
  /** List of variable length columns in this table, ordered by offset */
  private final List<ColumnImpl> _varColumns = new ArrayList<ColumnImpl>();
  /** List of autonumber columns in this table, ordered by column number */
  private final List<ColumnImpl> _autoNumColumns = new ArrayList<ColumnImpl>(1);
  /** List of indexes on this table (multiple logical indexes may be backed by
      the same index data) */
  private final List<IndexImpl> _indexes = new ArrayList<IndexImpl>();
  /** List of index datas on this table (the actual backing data for an
      index) */
  private final List<IndexData> _indexDatas = new ArrayList<IndexData>();
  /** List of columns in this table which are in one or more indexes */
  private final Set<ColumnImpl> _indexColumns = new LinkedHashSet<ColumnImpl>();
  /** Table name as stored in Database */
  private final String _name;
  /** Usage map of pages that this table owns */
  private final UsageMap _ownedPages;
  /** Usage map of pages that this table owns with free space on them */
  private final UsageMap _freeSpacePages;
  /** Number of rows in the table */
  private int _rowCount;
  /** last long auto number for the table */
  private int _lastLongAutoNumber;
  /** last complex type auto number for the table */
  private int _lastComplexTypeAutoNumber;
  /** modification count for the table, keeps row-states up-to-date */
  private int _modCount;
  /** page buffer used to update data pages when adding rows */
  private final TempPageHolder _addRowBufferH =
    TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);
  /** page buffer used to update the table def page */
  private final TempPageHolder _tableDefBufferH =
    TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);
  /** buffer used to writing rows of data */
  private final TempBufferHolder _writeRowBufferH =
    TempBufferHolder.newHolder(TempBufferHolder.Type.SOFT, true);
  /** page buffer used to write out-of-row "long value" data */
  private final TempPageHolder _longValueBufferH =
    TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);
  /** optional error handler to use when row errors are encountered */
  private ErrorHandler _tableErrorHandler;
  /** properties for this table */
  private PropertyMap _props;
  /** properties group for this table (and columns) */
  private PropertyMaps _propertyMaps;
  /** foreign-key enforcer for this table */
  private final FKEnforcer _fkEnforcer;

  /** default cursor for iterating through the table, kept here for basic
      table traversal */
  private CursorImpl _defaultCursor;
  
  /**
   * Only used by unit tests
   * @usage _advanced_method_
   */
  protected TableImpl(boolean testing, List<ColumnImpl> columns) 
    throws IOException 
  {
    if(!testing) {
      throw new IllegalArgumentException();
    }
    _database = null;
    _tableDefPageNumber = PageChannel.INVALID_PAGE_NUMBER;
    _name = null;

    _columns.addAll(columns);
    for(ColumnImpl col : _columns) {
      if(col.getType().isVariableLength()) {
        _varColumns.add(col);
      }
    }
    _maxColumnCount = (short)_columns.size();
    _maxVarColumnCount = (short)_varColumns.size();
    getAutoNumberColumns();

    _fkEnforcer = null;
    _flags = 0;
    _tableType = TYPE_USER;
    _indexCount = 0;
    _logicalIndexCount = 0;
    _ownedPages = null;
    _freeSpacePages = null;
  }
  
  /**
   * @param database database which owns this table
   * @param tableBuffer Buffer to read the table with
   * @param pageNumber Page number of the table definition
   * @param name Table name
   */
  protected TableImpl(DatabaseImpl database, ByteBuffer tableBuffer,
                  int pageNumber, String name, int flags)
    throws IOException
  {
    _database = database;
    _tableDefPageNumber = pageNumber;
    _name = name;
    _flags = flags;

    // read table definition
    tableBuffer = loadCompleteTableDefinitionBuffer(tableBuffer);
    _rowCount = tableBuffer.getInt(getFormat().OFFSET_NUM_ROWS);
    _lastLongAutoNumber = tableBuffer.getInt(getFormat().OFFSET_NEXT_AUTO_NUMBER);
    if(getFormat().OFFSET_NEXT_COMPLEX_AUTO_NUMBER >= 0) {
      _lastComplexTypeAutoNumber = tableBuffer.getInt(
          getFormat().OFFSET_NEXT_COMPLEX_AUTO_NUMBER);
    }
    _tableType = tableBuffer.get(getFormat().OFFSET_TABLE_TYPE);
    _maxColumnCount = tableBuffer.getShort(getFormat().OFFSET_MAX_COLS);
    _maxVarColumnCount = tableBuffer.getShort(getFormat().OFFSET_NUM_VAR_COLS);
    short columnCount = tableBuffer.getShort(getFormat().OFFSET_NUM_COLS);
    _logicalIndexCount = tableBuffer.getInt(getFormat().OFFSET_NUM_INDEX_SLOTS);
    _indexCount = tableBuffer.getInt(getFormat().OFFSET_NUM_INDEXES);

    tableBuffer.position(getFormat().OFFSET_OWNED_PAGES);
    _ownedPages = UsageMap.read(getDatabase(), tableBuffer, false);
    tableBuffer.position(getFormat().OFFSET_FREE_SPACE_PAGES);
    _freeSpacePages = UsageMap.read(getDatabase(), tableBuffer, false);
    
    for (int i = 0; i < _indexCount; i++) {
      _indexDatas.add(IndexData.create(this, tableBuffer, i, getFormat()));
    }
    
    readColumnDefinitions(tableBuffer, columnCount);

    readIndexDefinitions(tableBuffer);

    // read column usage map info
    while(tableBuffer.remaining() >= 2) {

      short umapColNum = tableBuffer.getShort();
      if(umapColNum == IndexData.COLUMN_UNUSED) {
        break;
      }
      
      int pos = tableBuffer.position();
      UsageMap colOwnedPages = null;
      UsageMap colFreeSpacePages = null;
      try {
        colOwnedPages = UsageMap.read(getDatabase(), tableBuffer, false);
        colFreeSpacePages = UsageMap.read(getDatabase(), tableBuffer, false);
      } catch(IllegalStateException e) {
        // ignore invalid usage map info
        colOwnedPages = null;
        colFreeSpacePages = null;
        tableBuffer.position(pos + 8);
        LOG.warn(withErrorContext("Invalid column " + umapColNum + 
                                  " usage map definition: " + e));
      }
      
      for(ColumnImpl col : _columns) {
        if(col.getColumnNumber() == umapColNum) {
          col.setUsageMaps(colOwnedPages, colFreeSpacePages);
          break;
        }
      }
    }

    // re-sort columns if necessary
    if(getDatabase().getColumnOrder() != ColumnOrder.DATA) {
      Collections.sort(_columns, DISPLAY_ORDER_COMPARATOR);
    }

    for(ColumnImpl col : _columns) {
      // some columns need to do extra work after the table is completely
      // loaded
      col.postTableLoadInit();
    }

    _fkEnforcer = new FKEnforcer(this);

    if(!isSystem()) {
      // after fully constructed, allow column validator to be configured (but
      // only for user tables)
      for(ColumnImpl col : _columns) {
        col.setColumnValidator(null);
      }
    }
  }

  public String getName() {
    return _name;
  }

  public boolean isHidden() {
    return((_flags & DatabaseImpl.HIDDEN_OBJECT_FLAG) != 0);
  }

  public boolean isSystem() {
    return(_tableType != TYPE_USER);
  }

  /**
   * @usage _advanced_method_
   */
  public int getMaxColumnCount() {
    return _maxColumnCount;
  }
  
  public int getColumnCount() {
    return _columns.size();
  }
  
  public DatabaseImpl getDatabase() {
    return _database;
  }
  
  /**
   * @usage _advanced_method_
   */
  public JetFormat getFormat() {
    return getDatabase().getFormat();
  }

  /**
   * @usage _advanced_method_
   */
  public PageChannel getPageChannel() {
    return getDatabase().getPageChannel();
  }

  public ErrorHandler getErrorHandler() {
    return((_tableErrorHandler != null) ? _tableErrorHandler :
           getDatabase().getErrorHandler());
  }

  public void setErrorHandler(ErrorHandler newErrorHandler) {
    _tableErrorHandler = newErrorHandler;
  }    

  public int getTableDefPageNumber() {
    return _tableDefPageNumber;
  }

  /**
   * @usage _advanced_method_
   */
  public RowState createRowState() {
    return new RowState(TempBufferHolder.Type.HARD);
  }

  /**
   * @usage _advanced_method_
   */
  public UsageMap.PageCursor getOwnedPagesCursor() {
    return _ownedPages.cursor();
  }

  /**
   * Returns the <i>approximate</i> number of database pages owned by this
   * table and all related indexes (this number does <i>not</i> take into
   * account pages used for large OLE/MEMO fields).
   * <p>
   * To calculate the approximate number of bytes owned by a table:
   * <code>
   * int approxTableBytes = (table.getApproximateOwnedPageCount() *
   *                         table.getFormat().PAGE_SIZE);
   * </code>
   * @usage _intermediate_method_
   */
  public int getApproximateOwnedPageCount() {

    // add a page for the table def (although that might actually be more than
    // one page)
    int count = _ownedPages.getPageCount() + 1;

    for(ColumnImpl col : _columns) {
      count += col.getOwnedPageCount();
    }

    // note, we count owned pages from _physical_ indexes, not logical indexes
    // (otherwise we could double count pages)
    for(IndexData indexData : _indexDatas) {
      count += indexData.getOwnedPageCount();
    }

    return count;
  }
  
  protected TempPageHolder getLongValueBuffer() {
    return _longValueBufferH;
  }

  public List<ColumnImpl> getColumns() {
    return Collections.unmodifiableList(_columns);
  }

  public ColumnImpl getColumn(String name) {
    for(ColumnImpl column : _columns) {
      if(column.getName().equalsIgnoreCase(name)) {
        return column;
      }
    }
    throw new IllegalArgumentException(withErrorContext(
            "Column with name " + name + " does not exist in this table"));
  }
  
  public boolean hasColumn(String name) {
    for(ColumnImpl column : _columns) {
      if(column.getName().equalsIgnoreCase(name)) {
        return true;
      }
    }
    return false;
  }

  public PropertyMap getProperties() throws IOException {
    if(_props == null) {
      _props = getPropertyMaps().getDefault();
    }
    return _props;
  }

  /**
   * @return all PropertyMaps for this table (and columns)
   * @usage _advanced_method_
   */
  public PropertyMaps getPropertyMaps() throws IOException {
    if(_propertyMaps == null) {
      _propertyMaps = getDatabase().getPropertiesForObject(
          _tableDefPageNumber);
    }
    return _propertyMaps;
  }
  
  public List<IndexImpl> getIndexes() {
    return Collections.unmodifiableList(_indexes);
  }

  public IndexImpl getIndex(String name) {
    for(IndexImpl index : _indexes) {
      if(index.getName().equalsIgnoreCase(name)) {
        return index;
      }
    }
    throw new IllegalArgumentException(withErrorContext(
            "Index with name " + name + " does not exist on this table"));
  }

  public IndexImpl getPrimaryKeyIndex() {
    for(IndexImpl index : _indexes) {
      if(index.isPrimaryKey()) {
        return index;
      }
    }
    throw new IllegalArgumentException(withErrorContext(
            "No primary key index found"));
  }
  
  public IndexImpl getForeignKeyIndex(Table otherTable) {
    for(IndexImpl index : _indexes) {
      if(index.isForeignKey() && (index.getReference() != null) &&
         (index.getReference().getOtherTablePageNumber() ==
          ((TableImpl)otherTable).getTableDefPageNumber())) {
        return index;
      }
    }
    throw new IllegalArgumentException(withErrorContext(
        "No foreign key reference to " +
        otherTable.getName() + " found"));
  }
  
  /**
   * @return All of the IndexData on this table (unmodifiable List)
   * @usage _advanced_method_
   */
  public List<IndexData> getIndexDatas() {
    return Collections.unmodifiableList(_indexDatas);
  }

  /**
   * Only called by unit tests
   * @usage _advanced_method_
   */
  public int getLogicalIndexCount() {
    return _logicalIndexCount;
  }

  public CursorImpl getDefaultCursor() {
    if(_defaultCursor == null) {
      _defaultCursor = CursorImpl.createCursor(this);
    }
    return _defaultCursor;
  }

  public CursorBuilder newCursor() {
    return new CursorBuilder(this);
  }
  
  public void reset() {
    getDefaultCursor().reset();
  }

  public Row deleteRow(Row row) throws IOException {
    deleteRow(row.getId());
    return row;
  }

  /**
   * Delete the row with the given id.  Provided RowId must have previously
   * been returned from this Table.
   * @return the given rowId
   * @throws IllegalStateException if the given row is not valid
   * @usage _intermediate_method_
   */
  public RowId deleteRow(RowId rowId) throws IOException {
    deleteRow(getDefaultCursor().getRowState(), (RowIdImpl)rowId);
    return rowId;
  }

  /**
   * Delete the row for the given rowId.
   * @usage _advanced_method_
   */
  public void deleteRow(RowState rowState, RowIdImpl rowId) 
    throws IOException 
  {
    requireValidRowId(rowId);
    
    getPageChannel().startWrite();
    try {
      
      // ensure that the relevant row state is up-to-date
      ByteBuffer rowBuffer = positionAtRowHeader(rowState, rowId);

      if(rowState.isDeleted()) {
        // don't care about duplicate deletion
        return;
      }
      requireNonDeletedRow(rowState, rowId);
    
      // delete flag always gets set in the "header" row (even if data is on
      // overflow row)
      int pageNumber = rowState.getHeaderRowId().getPageNumber();
      int rowNumber = rowState.getHeaderRowId().getRowNumber();

      // attempt to fill in index column values
      Object[] rowValues = null;
      if(!_indexDatas.isEmpty()) {

        // move to row data to get index values
        rowBuffer = positionAtRowData(rowState, rowId);

        for(ColumnImpl idxCol : _indexColumns) {
          getRowColumn(getFormat(), rowBuffer, idxCol, rowState, null);
        }

        // use any read rowValues to help update the indexes
        rowValues = rowState.getRowCacheValues();

        // check foreign keys before proceeding w/ deletion
        _fkEnforcer.deleteRow(rowValues);    

        // move back to the header
        rowBuffer = positionAtRowHeader(rowState, rowId);
      }

      // finally, pull the trigger
      int rowIndex = getRowStartOffset(rowNumber, getFormat());
      rowBuffer.putShort(rowIndex, (short)(rowBuffer.getShort(rowIndex)
                                           | DELETED_ROW_MASK | OVERFLOW_ROW_MASK));
      writeDataPage(rowBuffer, pageNumber);

      // update the indexes
      for(IndexData indexData : _indexDatas) {
        indexData.deleteRow(rowValues, rowId);
      }
    
      // make sure table def gets updated
      updateTableDefinition(-1);

    } finally {
      getPageChannel().finishWrite();
    }
  }
  
  public Row getNextRow() throws IOException {
    return getDefaultCursor().getNextRow();
  }
  
  /**
   * Reads a single column from the given row.
   * @usage _advanced_method_
   */
  public Object getRowValue(RowState rowState, RowIdImpl rowId,
                            ColumnImpl column)
    throws IOException
  {
    if(this != column.getTable()) {
      throw new IllegalArgumentException(withErrorContext(
          "Given column " + column + " is not from this table"));
    }
    requireValidRowId(rowId);
    
    // position at correct row
    ByteBuffer rowBuffer = positionAtRowData(rowState, rowId);
    requireNonDeletedRow(rowState, rowId);
    
    return getRowColumn(getFormat(), rowBuffer, column, rowState, null);
  }

  /**
   * Reads some columns from the given row.
   * @param columnNames Only column names in this collection will be returned
   * @usage _advanced_method_
   */
  public RowImpl getRow(
      RowState rowState, RowIdImpl rowId, Collection<String> columnNames)
    throws IOException
  {
    requireValidRowId(rowId);

    // position at correct row
    ByteBuffer rowBuffer = positionAtRowData(rowState, rowId);
    requireNonDeletedRow(rowState, rowId);

    return getRow(getFormat(), rowState, rowBuffer, _columns, columnNames);
  }

  /**
   * Reads the row data from the given row buffer.  Leaves limit unchanged.
   * Saves parsed row values to the given rowState.
   */
  private static RowImpl getRow(
      JetFormat format,
      RowState rowState,
      ByteBuffer rowBuffer,
      Collection<ColumnImpl> columns,
      Collection<String> columnNames)
    throws IOException
  {
    RowImpl rtn = new RowImpl(rowState.getHeaderRowId(), columns.size());
    for(ColumnImpl column : columns) {

      if((columnNames == null) || (columnNames.contains(column.getName()))) {
        // Add the value to the row data
        column.setRowValue(
            rtn, getRowColumn(format, rowBuffer, column, rowState, null));
      }
    }
    return rtn;
  }
  
  /**
   * Reads the column data from the given row buffer.  Leaves limit unchanged.
   * Caches the returned value in the rowState.
   */
  private static Object getRowColumn(JetFormat format,
                                     ByteBuffer rowBuffer,
                                     ColumnImpl column,
                                     RowState rowState,
                                     Map<ColumnImpl,byte[]> rawVarValues)
    throws IOException
  {
    byte[] columnData = null;
    try {

      NullMask nullMask = rowState.getNullMask(rowBuffer);
      boolean isNull = nullMask.isNull(column);
      if(column.storeInNullMask()) {
          // Boolean values are stored in the null mask.  see note about
          // caching below
        return rowState.setRowCacheValue(column.getColumnIndex(),
                                         column.readFromNullMask(isNull));
      } else if(isNull) {
        // well, that's easy! (no need to update cache w/ null)
        return null;
      }

      Object cachedValue = rowState.getRowCacheValue(column.getColumnIndex());
      if(cachedValue != null) {
        // we already have it, use it
        return cachedValue;
      }
      
      // reset position to row start
      rowBuffer.reset();
    
      // locate the column data bytes
      int rowStart = rowBuffer.position();
      int colDataPos = 0;
      int colDataLen = 0;
      if(!column.isVariableLength()) {

        // read fixed length value (non-boolean at this point)
        int dataStart = rowStart + format.OFFSET_COLUMN_FIXED_DATA_ROW_OFFSET;
        colDataPos = dataStart + column.getFixedDataOffset();
        colDataLen = column.getType().getFixedSize(column.getLength());
      
      } else {
        int varDataStart; 
        int varDataEnd;

        if(format.SIZE_ROW_VAR_COL_OFFSET == 2) {

          // read simple var length value
          int varColumnOffsetPos =
            (rowBuffer.limit() - nullMask.byteSize() - 4) -
            (column.getVarLenTableIndex() * 2);

          varDataStart = rowBuffer.getShort(varColumnOffsetPos);
          varDataEnd = rowBuffer.getShort(varColumnOffsetPos - 2);

        } else {

          // read jump-table based var length values
          short[] varColumnOffsets = readJumpTableVarColOffsets(
              rowState, rowBuffer, rowStart, nullMask);

          varDataStart = varColumnOffsets[column.getVarLenTableIndex()];
          varDataEnd = varColumnOffsets[column.getVarLenTableIndex() + 1];
        }

        colDataPos = rowStart + varDataStart;
        colDataLen = varDataEnd - varDataStart;
      }

      // grab the column data
      rowBuffer.position(colDataPos);
      columnData = ByteUtil.getBytes(rowBuffer, colDataLen);

      if((rawVarValues != null) && column.isVariableLength()) {
        // caller wants raw value as well
        rawVarValues.put(column, columnData);
      }

      // parse the column data.  we cache the row values in order to be able
      // to update the index on row deletion.  note, most of the returned
      // values are immutable, except for binary data (returned as byte[]),
      // but binary data shouldn't be indexed anyway.
      return rowState.setRowCacheValue(column.getColumnIndex(), 
                                       column.read(columnData));

    } catch(Exception e) {

      // cache "raw" row value.  see note about caching above
      rowState.setRowCacheValue(column.getColumnIndex(), 
                                ColumnImpl.rawDataWrapper(columnData));

      return rowState.handleRowError(column, columnData, e);
    }
  }

  private static short[] readJumpTableVarColOffsets(
      RowState rowState, ByteBuffer rowBuffer, int rowStart,
      NullMask nullMask) 
  {
    short[] varColOffsets = rowState.getVarColOffsets();
    if(varColOffsets != null) {
      return varColOffsets;
    }

    // calculate offsets using jump-table info
    int nullMaskSize = nullMask.byteSize();
    int rowEnd = rowStart + rowBuffer.remaining() - 1;
    int numVarCols = ByteUtil.getUnsignedByte(rowBuffer, 
                                              rowEnd - nullMaskSize);
    varColOffsets = new short[numVarCols + 1];
	  
    int rowLen = rowEnd - rowStart + 1;
    int numJumps = (rowLen - 1) / MAX_BYTE;
    int colOffset = rowEnd - nullMaskSize - numJumps - 1;
	  
    // If last jump is a dummy value, ignore it
    if(((colOffset - rowStart - numVarCols) / MAX_BYTE) < numJumps) {
      numJumps--;
    }

    int jumpsUsed = 0;
    for(int i = 0; i < numVarCols + 1; i++) {

      while((jumpsUsed < numJumps) && 
         (i == ByteUtil.getUnsignedByte(
              rowBuffer, rowEnd - nullMaskSize-jumpsUsed - 1))) {
        jumpsUsed++;
      }
		  
      varColOffsets[i] = (short)
        (ByteUtil.getUnsignedByte(rowBuffer, colOffset - i)
         + (jumpsUsed * MAX_BYTE));
    }
	  
    rowState.setVarColOffsets(varColOffsets);
    return varColOffsets;
  }

  /**
   * Reads the null mask from the given row buffer.  Leaves limit unchanged.
   */
  private NullMask getRowNullMask(ByteBuffer rowBuffer)
    throws IOException
  {
    // reset position to row start
    rowBuffer.reset();

    // Number of columns in this row
    int columnCount = ByteUtil.getUnsignedVarInt(
        rowBuffer, getFormat().SIZE_ROW_COLUMN_COUNT);
    
    // read null mask
    NullMask nullMask = new NullMask(columnCount);
    rowBuffer.position(rowBuffer.limit() - nullMask.byteSize());  //Null mask at end
    nullMask.read(rowBuffer);

    return nullMask;
  }

  /**
   * Sets a new buffer to the correct row header page using the given rowState
   * according to the given rowId.  Deleted state is
   * determined, but overflow row pointers are not followed.
   * 
   * @return a ByteBuffer of the relevant page, or null if row was invalid
   * @usage _advanced_method_
   */
  public static ByteBuffer positionAtRowHeader(RowState rowState, 
                                               RowIdImpl rowId)
    throws IOException
  {
    ByteBuffer rowBuffer = rowState.setHeaderRow(rowId);

    if(rowState.isAtHeaderRow()) {
      // this task has already been accomplished
      return rowBuffer;
    }
    
    if(!rowState.isValid()) {
      // this was an invalid page/row
      rowState.setStatus(RowStateStatus.AT_HEADER);
      return null;
    }

    // note, we don't use findRowStart here cause we need the unmasked value
    short rowStart = rowBuffer.getShort(
        getRowStartOffset(rowId.getRowNumber(),
                          rowState.getTable().getFormat()));

    // check the deleted, overflow flags for the row (the "real" flags are
    // always set on the header row)
    RowStatus rowStatus = RowStatus.NORMAL;
    if(isDeletedRow(rowStart)) {
      rowStatus = RowStatus.DELETED;
    } else if(isOverflowRow(rowStart)) {
      rowStatus = RowStatus.OVERFLOW;
    }

    rowState.setRowStatus(rowStatus);
    rowState.setStatus(RowStateStatus.AT_HEADER);
    return rowBuffer;
  }
  
  /**
   * Sets the position and limit in a new buffer using the given rowState
   * according to the given row number and row end, following overflow row
   * pointers as necessary.
   * 
   * @return a ByteBuffer narrowed to the actual row data, or null if row was
   *         invalid or deleted
   * @usage _advanced_method_
   */
  public static ByteBuffer positionAtRowData(RowState rowState, 
                                             RowIdImpl rowId)
    throws IOException
  {
    positionAtRowHeader(rowState, rowId);
    if(!rowState.isValid() || rowState.isDeleted()) {
      // row is invalid or deleted
      rowState.setStatus(RowStateStatus.AT_FINAL);
      return null;
    }

    ByteBuffer rowBuffer = rowState.getFinalPage();
    int rowNum = rowState.getFinalRowId().getRowNumber();
    JetFormat format = rowState.getTable().getFormat();
    
    if(rowState.isAtFinalRow()) {
      // we've already found the final row data
      return PageChannel.narrowBuffer(
          rowBuffer,
          findRowStart(rowBuffer, rowNum, format),
          findRowEnd(rowBuffer, rowNum, format));
    }
    
    while(true) {
      
      // note, we don't use findRowStart here cause we need the unmasked value
      short rowStart = rowBuffer.getShort(getRowStartOffset(rowNum, format));
      short rowEnd = findRowEnd(rowBuffer, rowNum, format);

      // note, at this point we know the row is not deleted, so ignore any
      // subsequent deleted flags (as overflow rows are always marked deleted
      // anyway)
      boolean overflowRow = isOverflowRow(rowStart);

      // now, strip flags from rowStart offset
      rowStart = (short)(rowStart & OFFSET_MASK);

      if (overflowRow) {

        if((rowEnd - rowStart) < 4) {
          throw new IOException(rowState.getTable().withErrorContext(
                                    "invalid overflow row info"));
        }
      
        // Overflow page.  the "row" data in the current page points to
        // another page/row
        int overflowRowNum = ByteUtil.getUnsignedByte(rowBuffer, rowStart);
        int overflowPageNum = ByteUtil.get3ByteInt(rowBuffer, rowStart + 1);
        rowBuffer = rowState.setOverflowRow(
            new RowIdImpl(overflowPageNum, overflowRowNum));
        rowNum = overflowRowNum;
      
      } else {

        rowState.setStatus(RowStateStatus.AT_FINAL);
        return PageChannel.narrowBuffer(rowBuffer, rowStart, rowEnd);
      }
    }
  }

  public Iterator<Row> iterator() {
    return getDefaultCursor().iterator();
  }

  /**
   * Writes a new table defined by the given TableCreator to the database.
   * @usage _advanced_method_
   */
  protected static void writeTableDefinition(TableCreator creator)
    throws IOException
  {
    // first, create the usage map page
    createUsageMapDefinitionBuffer(creator);

    // next, determine how big the table def will be (in case it will be more
    // than one page)
    JetFormat format = creator.getFormat();
    int idxDataLen = (creator.getIndexCount() * 
                      (format.SIZE_INDEX_DEFINITION + 
                       format.SIZE_INDEX_COLUMN_BLOCK)) + 
      (creator.getLogicalIndexCount() * format.SIZE_INDEX_INFO_BLOCK);
    int colUmapLen = creator.getLongValueColumns().size() * 10;
    int totalTableDefSize = format.SIZE_TDEF_HEADER +
      (format.SIZE_COLUMN_DEF_BLOCK * creator.getColumns().size()) + 
      idxDataLen + colUmapLen + format.SIZE_TDEF_TRAILER;

    // total up the amount of space used by the column and index names (2
    // bytes per char + 2 bytes for the length)
    for(ColumnBuilder col : creator.getColumns()) {
      int nameByteLen = (col.getName().length() *
                         JetFormat.TEXT_FIELD_UNIT_SIZE);
      totalTableDefSize += nameByteLen + 2;
    }
    
    for(IndexBuilder idx : creator.getIndexes()) {
      int nameByteLen = (idx.getName().length() *
                         JetFormat.TEXT_FIELD_UNIT_SIZE);
      totalTableDefSize += nameByteLen + 2;
    }
    

    // now, create the table definition
    PageChannel pageChannel = creator.getPageChannel();
    ByteBuffer buffer = PageChannel.createBuffer(Math.max(totalTableDefSize,
                                                          format.PAGE_SIZE));
    writeTableDefinitionHeader(creator, buffer, totalTableDefSize);

    if(creator.hasIndexes()) {
      // index row counts
      IndexData.writeRowCountDefinitions(creator, buffer);
    }

    // column definitions
    ColumnImpl.writeDefinitions(creator, buffer); 
    
    if(creator.hasIndexes()) {
      // index and index data definitions
      IndexData.writeDefinitions(creator, buffer);
      IndexImpl.writeDefinitions(creator, buffer);
    }

    // write long value column usage map references
    for(ColumnBuilder lvalCol : creator.getLongValueColumns()) {
      buffer.putShort(lvalCol.getColumnNumber());
      TableCreator.ColumnState colState = 
        creator.getColumnState(lvalCol);
      
      // owned pages umap (both are on same page)
      buffer.put(colState.getUmapOwnedRowNumber());
      ByteUtil.put3ByteInt(buffer, colState.getUmapPageNumber());
      // free space pages umap
      buffer.put(colState.getUmapFreeRowNumber());
      ByteUtil.put3ByteInt(buffer, colState.getUmapPageNumber());
    }

    //End of tabledef
    buffer.put((byte) 0xff);
    buffer.put((byte) 0xff);

    // write table buffer to database
    if(totalTableDefSize <= format.PAGE_SIZE) {
      
      // easy case, fits on one page
      buffer.putShort(format.OFFSET_FREE_SPACE,
                      (short)(buffer.remaining() - 8)); // overwrite page free space
      // Write the tdef page to disk.
      pageChannel.writePage(buffer, creator.getTdefPageNumber());
      
    } else {

      // need to split across multiple pages
      ByteBuffer partialTdef = pageChannel.createPageBuffer();
      buffer.rewind();
      int nextTdefPageNumber = PageChannel.INVALID_PAGE_NUMBER;
      while(buffer.hasRemaining()) {

        // reset for next write
        partialTdef.clear();
        
        if(nextTdefPageNumber == PageChannel.INVALID_PAGE_NUMBER) {
          
          // this is the first page.  note, the first page already has the
          // page header, so no need to write it here
          nextTdefPageNumber = creator.getTdefPageNumber();
          
        } else {

          // write page header
          writeTablePageHeader(partialTdef);
        }

        // copy the next page of tdef bytes
        int curTdefPageNumber = nextTdefPageNumber;
        int writeLen = Math.min(partialTdef.remaining(), buffer.remaining());
        partialTdef.put(buffer.array(), buffer.position(), writeLen);
        ByteUtil.forward(buffer, writeLen);

        if(buffer.hasRemaining()) {
          // need a next page
          nextTdefPageNumber = pageChannel.allocateNewPage();
          partialTdef.putInt(format.OFFSET_NEXT_TABLE_DEF_PAGE,
                             nextTdefPageNumber);
        }

        // update page free space
        partialTdef.putShort(format.OFFSET_FREE_SPACE,
                             (short)(partialTdef.remaining() - 8)); // overwrite page free space

        // write partial page to disk
        pageChannel.writePage(partialTdef, curTdefPageNumber);
      }
        
    }
  }

  /**
   * @param buffer Buffer to write to
   * @param columns List of Columns in the table
   */
  private static void writeTableDefinitionHeader(
      TableCreator creator, ByteBuffer buffer, int totalTableDefSize)
    throws IOException
  {
    List<ColumnBuilder> columns = creator.getColumns();

    //Start writing the tdef
    writeTablePageHeader(buffer);
    buffer.putInt(totalTableDefSize);  //Length of table def
    buffer.putInt(MAGIC_TABLE_NUMBER); // seemingly constant magic value
    buffer.putInt(0);  //Number of rows
    buffer.putInt(0); //Last Autonumber
    buffer.put((byte) 1); // this makes autonumbering work in access
    for (int i = 0; i < 15; i++) {  //Unknown
      buffer.put((byte) 0);
    }
    buffer.put(TYPE_USER); //Table type
    buffer.putShort((short) columns.size()); //Max columns a row will have
    buffer.putShort(ColumnImpl.countVariableLength(columns));  //Number of variable columns in table
    buffer.putShort((short) columns.size()); //Number of columns in table
    buffer.putInt(creator.getLogicalIndexCount());  //Number of logical indexes in table
    buffer.putInt(creator.getIndexCount());  //Number of indexes in table
    buffer.put((byte) 0); //Usage map row number
    ByteUtil.put3ByteInt(buffer, creator.getUmapPageNumber());  //Usage map page number
    buffer.put((byte) 1); //Free map row number
    ByteUtil.put3ByteInt(buffer, creator.getUmapPageNumber());  //Free map page number
  }

  /**
   * Writes the page header for a table definition page
   * @param buffer Buffer to write to
   */
  private static void writeTablePageHeader(ByteBuffer buffer)
  {
    buffer.put(PageTypes.TABLE_DEF);  //Page type
    buffer.put((byte) 0x01); //Unknown
    buffer.put((byte) 0); //Unknown
    buffer.put((byte) 0); //Unknown
    buffer.putInt(0);  //Next TDEF page pointer
  }
  
  /**
   * Writes the given name into the given buffer in the format as expected by
   * {@link #readName}.
   */
  static void writeName(ByteBuffer buffer, String name, Charset charset)
  {
      ByteBuffer encName = ColumnImpl.encodeUncompressedText(name, charset);
      buffer.putShort((short) encName.remaining());
      buffer.put(encName);
  }
  
  /**
   * Create the usage map definition page buffer.  The "used pages" map is in
   * row 0, the "pages with free space" map is in row 1.  Index usage maps are
   * in subsequent rows.
   */
  private static void createUsageMapDefinitionBuffer(TableCreator creator)
    throws IOException
  {
    List<ColumnBuilder> lvalCols = creator.getLongValueColumns();

    // 2 table usage maps plus 1 for each index and 2 for each lval col
    int indexUmapEnd = 2 + creator.getIndexCount();
    int umapNum = indexUmapEnd + (lvalCols.size() * 2);

    JetFormat format = creator.getFormat();
    int umapRowLength = format.OFFSET_USAGE_MAP_START +
      format.USAGE_MAP_TABLE_BYTE_LENGTH;
    int umapSpaceUsage = getRowSpaceUsage(umapRowLength, format);
    PageChannel pageChannel = creator.getPageChannel();
    int umapPageNumber = PageChannel.INVALID_PAGE_NUMBER;
    ByteBuffer umapBuf = null;
    int freeSpace = 0;
    int rowStart = 0;
    int umapRowNum = 0;
    
    for(int i = 0; i < umapNum; ++i) {

      if(umapBuf == null) {

        // need new page for usage maps
        if(umapPageNumber == PageChannel.INVALID_PAGE_NUMBER) {
          // first umap page has already been reserved
          umapPageNumber = creator.getUmapPageNumber();
        } else {
          // need another umap page
          umapPageNumber = creator.reservePageNumber();
    }

        freeSpace = format.DATA_PAGE_INITIAL_FREE_SPACE;

        umapBuf = pageChannel.createPageBuffer();
        umapBuf.put(PageTypes.DATA);
        umapBuf.put((byte) 0x1);  //Unknown
        umapBuf.putShort((short)freeSpace);  //Free space in page
        umapBuf.putInt(0); //Table definition
        umapBuf.putInt(0); //Unknown
        umapBuf.putShort((short)0); //Number of records on this page

        rowStart = findRowEnd(umapBuf, 0, format) - umapRowLength;
        umapRowNum = 0;
      }

      umapBuf.putShort(getRowStartOffset(umapRowNum, format), (short)rowStart);
      
      if(i == 0) {

        // table "owned pages" map definition
        umapBuf.put(rowStart, UsageMap.MAP_TYPE_REFERENCE);

      } else if(i == 1) {

        // table "free space pages" map definition
        umapBuf.put(rowStart, UsageMap.MAP_TYPE_INLINE);

      } else if(i < indexUmapEnd) {

        // index umap
        int indexIdx = i - 2;
        IndexBuilder idx = creator.getIndexes().get(indexIdx);
        
        // allocate root page for the index
        int rootPageNumber = pageChannel.allocateNewPage();

        // stash info for later use
        TableCreator.IndexState idxState = creator.getIndexState(idx);
        idxState.setRootPageNumber(rootPageNumber);
        idxState.setUmapRowNumber((byte)umapRowNum);
        idxState.setUmapPageNumber(umapPageNumber);

        // index map definition, including initial root page
        umapBuf.put(rowStart, UsageMap.MAP_TYPE_INLINE);
        umapBuf.putInt(rowStart + 1, rootPageNumber);
        umapBuf.put(rowStart + 5, (byte)1);

      } else {

        // long value column umaps
        int lvalColIdx = i - indexUmapEnd;
        int umapType = lvalColIdx % 2;
        lvalColIdx /= 2;

        ColumnBuilder lvalCol = lvalCols.get(lvalColIdx);
        TableCreator.ColumnState colState = 
          creator.getColumnState(lvalCol);

        umapBuf.put(rowStart, UsageMap.MAP_TYPE_INLINE);

        if((umapType == 1) && 
           (umapPageNumber != colState.getUmapPageNumber())) {
          // we want to force both usage maps for a column to be on the same
          // data page, so just discard the previous one we wrote
          --i;
          umapType = 0;
      }
        
        if(umapType == 0) {
          // lval column "owned pages" usage map
          colState.setUmapOwnedRowNumber((byte)umapRowNum);
          colState.setUmapPageNumber(umapPageNumber);
        } else {
          // lval column "free space pages" usage map (always on same page)
          colState.setUmapFreeRowNumber((byte)umapRowNum);
    }
      }

      rowStart -= umapRowLength;
      freeSpace -= umapSpaceUsage;
      ++umapRowNum;

      if((freeSpace <= umapSpaceUsage) || (i == (umapNum - 1))) {
        // finish current page
        umapBuf.putShort(format.OFFSET_FREE_SPACE, (short)freeSpace);
        umapBuf.putShort(format.OFFSET_NUM_ROWS_ON_DATA_PAGE, 
                         (short)umapRowNum);
        pageChannel.writePage(umapBuf, umapPageNumber);
        umapBuf = null;
      }
    }
  }

  /**
   * Returns a single ByteBuffer which contains the entire table definition
   * (which may span multiple database pages).
   */
  private ByteBuffer loadCompleteTableDefinitionBuffer(ByteBuffer tableBuffer)
    throws IOException
  {
    int nextPage = tableBuffer.getInt(getFormat().OFFSET_NEXT_TABLE_DEF_PAGE);
    ByteBuffer nextPageBuffer = null;
    while (nextPage != 0) {
      if (nextPageBuffer == null) {
        nextPageBuffer = getPageChannel().createPageBuffer();
      }
      getPageChannel().readPage(nextPageBuffer, nextPage);
      nextPage = nextPageBuffer.getInt(getFormat().OFFSET_NEXT_TABLE_DEF_PAGE);
      ByteBuffer newBuffer = PageChannel.createBuffer(
          tableBuffer.capacity() + getFormat().PAGE_SIZE - 8);
      newBuffer.put(tableBuffer);
      newBuffer.put(nextPageBuffer.array(), 8, getFormat().PAGE_SIZE - 8);
      tableBuffer = newBuffer;
      tableBuffer.flip();
    }
    return tableBuffer;
  }
    
  private void readColumnDefinitions(ByteBuffer tableBuffer, short columnCount)
    throws IOException
  {
    int colOffset = getFormat().OFFSET_INDEX_DEF_BLOCK +
        _indexCount * getFormat().SIZE_INDEX_DEFINITION;

    tableBuffer.position(colOffset +
                         (columnCount * getFormat().SIZE_COLUMN_HEADER));
    List<String> colNames = new ArrayList<String>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      colNames.add(readName(tableBuffer));
    }    
    
    int dispIndex = 0;
    for (int i = 0; i < columnCount; i++) {
      ColumnImpl column = ColumnImpl.create(this, tableBuffer,
          colOffset + (i * getFormat().SIZE_COLUMN_HEADER), colNames.get(i),
          dispIndex++);
      _columns.add(column);
      if(column.isVariableLength()) {
        // also shove it in the variable columns list, which is ordered
        // differently from the _columns list
        _varColumns.add(column);
      }
    }

    Collections.sort(_columns);
    getAutoNumberColumns();

    // setup the data index for the columns
    int colIdx = 0;
    for(ColumnImpl col : _columns) {
      col.setColumnIndex(colIdx++);
    }

    // sort variable length columns based on their index into the variable
    // length offset table, because we will write the columns in this order
    Collections.sort(_varColumns, VAR_LEN_COLUMN_COMPARATOR);
  }

  private void readIndexDefinitions(ByteBuffer tableBuffer) throws IOException
  {
    // read index column information
    for (int i = 0; i < _indexCount; i++) {
      IndexData idxData = _indexDatas.get(i);
      idxData.read(tableBuffer, _columns);
      // keep track of all columns involved in indexes
      for(IndexData.ColumnDescriptor iCol : idxData.getColumns()) {
        _indexColumns.add(iCol.getColumn());
      }
    }

    // read logical index info (may be more logical indexes than index datas)
    for (int i = 0; i < _logicalIndexCount; i++) {
      _indexes.add(new IndexImpl(tableBuffer, _indexDatas, getFormat()));
    }

    // read logical index names
    for (int i = 0; i < _logicalIndexCount; i++) {
      _indexes.get(i).setName(readName(tableBuffer));
    }
    
    Collections.sort(_indexes);
  }
  
  /**
   * Writes the given page data to the given page number, clears any other
   * relevant buffers.
   */
  private void writeDataPage(ByteBuffer pageBuffer, int pageNumber)
    throws IOException
  {
    // write the page data
    getPageChannel().writePage(pageBuffer, pageNumber);

    // possibly invalidate the add row buffer if a different data buffer is
    // being written (e.g. this happens during deleteRow)
    _addRowBufferH.possiblyInvalidate(pageNumber, pageBuffer);
    
    // update modification count so any active RowStates can keep themselves
    // up-to-date
    ++_modCount;
  }

  /**
   * Returns a name read from the buffer at the current position. The
   * expected name format is the name length followed by the name 
   * encoded using the {@link JetFormat#CHARSET}
   */
  private String readName(ByteBuffer buffer) { 
    int nameLength = readNameLength(buffer);
    byte[] nameBytes = ByteUtil.getBytes(buffer, nameLength);
    return ColumnImpl.decodeUncompressedText(nameBytes, 
                                         getDatabase().getCharset());
  }
  
  /**
   * Returns a name length read from the buffer at the current position.
   */
  private int readNameLength(ByteBuffer buffer) { 
    return ByteUtil.getUnsignedVarInt(buffer, getFormat().SIZE_NAME_LENGTH);
  }
  
  public Object[] asRow(Map<String,?> rowMap) {
    return asRow(rowMap, null, false);
  }
  
  /**
   * Converts a map of columnName -> columnValue to an array of row values
   * appropriate for a call to {@link #addRow(Object...)}, where the generated
   * RowId will be an extra value at the end of the array.
   * @see ColumnImpl#RETURN_ROW_ID
   * @usage _intermediate_method_
   */
  public Object[] asRowWithRowId(Map<String,?> rowMap) {
    return asRow(rowMap, null, true);
  }
  
  public Object[] asUpdateRow(Map<String,?> rowMap) {
    return asRow(rowMap, Column.KEEP_VALUE, false);
  }

  /**
   * @return the generated RowId added to a row of values created via {@link
   *         #asRowWithRowId}
   * @usage _intermediate_method_
   */
  public RowId getRowId(Object[] row) {
    return (RowId)row[_columns.size()];
  }

  /**
   * Converts a map of columnName -> columnValue to an array of row values.
   */
  private Object[] asRow(Map<String,?> rowMap, Object defaultValue, 
                         boolean returnRowId)
  {
    int len = _columns.size();
    if(returnRowId) {
      ++len;
    }
    Object[] row = new Object[len];
    if(defaultValue != null) {
      Arrays.fill(row, defaultValue);
    }
    if(returnRowId) {
      row[len - 1] = ColumnImpl.RETURN_ROW_ID;
    }
    if(rowMap == null) {
      return row;
    }
    for(ColumnImpl col : _columns) {
      if(rowMap.containsKey(col.getName())) {
        col.setRowValue(row, col.getRowValue(rowMap));
      }
    }
    return row;
  }
  
  public Object[] addRow(Object... row) throws IOException {
    return addRows(Collections.singletonList(row), false).get(0);
  }

  public <M extends Map<String,Object>> M addRowFromMap(M row) 
    throws IOException 
  {
    Object[] rowValues = asRow(row);

    addRow(rowValues);

    returnRowValues(row, rowValues, _autoNumColumns);
    return row;
  }
    
  public List<? extends Object[]> addRows(List<? extends Object[]> rows) 
    throws IOException 
  {
    return addRows(rows, true);
  }
  
  public <M extends Map<String,Object>> List<M> addRowsFromMaps(List<M> rows) 
    throws IOException 
  {
    List<Object[]> rowValuesList = new ArrayList<Object[]>(rows.size());
    for(Map<String,Object> row : rows) {
      rowValuesList.add(asRow(row));
    } 

    addRows(rowValuesList);

    if(!_autoNumColumns.isEmpty()) {
      for(int i = 0; i < rowValuesList.size(); ++i) {
        Map<String,Object> row = rows.get(i);
        Object[] rowValues = rowValuesList.get(i);
        returnRowValues(row, rowValues, _autoNumColumns);
      }
    }
    return rows;
  }

  private static void returnRowValues(Map<String,Object> row, Object[] rowValues,
                                      List<ColumnImpl> cols)
  {
    for(ColumnImpl col : cols) {
      col.setRowValue(row, col.getRowValue(rowValues));
    }
  }

  /**
   * Add multiple rows to this table, only writing to disk after all
   * rows have been written, and every time a data page is filled.
   * @param inRows List of Object[] row values
   */
  private List<? extends Object[]> addRows(List<? extends Object[]> rows,
                                           final boolean isBatchWrite)
    throws IOException
  {
    if(rows.isEmpty()) {
      return rows;
    }

    getPageChannel().startWrite();
    try {
    
      ByteBuffer dataPage = null;
      int pageNumber = PageChannel.INVALID_PAGE_NUMBER;
      int updateCount = 0;
      int autoNumAssignCount = 0;
      try {

        List<Object[]> dupeRows = null;
        final int numCols = _columns.size();
        for (int i = 0; i < rows.size(); i++) {

          // we need to make sure the row is the right length and is an
          // Object[] (fill with null if too short).  note, if the row is
          // copied the caller will not be able to access any generated
          // auto-number value, but if they need that info they should use a
          // row array of the right size/type!
          Object[] row = rows.get(i);
          if((row.length < numCols) || (row.getClass() != Object[].class)) {
            row = dupeRow(row, numCols);
            // copy the input rows to a modifiable list so we can update the
            // elements
            if(dupeRows == null) {
              dupeRows = new ArrayList<Object[]>(rows);
              rows = dupeRows;
            }
            // we copied the row, so put the copy back into the rows list
            dupeRows.set(i, row);
          }

          // handle various value massaging activities
          for(ColumnImpl column : _columns) {
            if(!column.isAutoNumber()) {              
              // pass input value through column validator
              column.setRowValue(row, column.validate(column.getRowValue(row)));
            }
          }

          // fill in autonumbers
          handleAutoNumbersForAdd(row);
          ++autoNumAssignCount;
      
          // write the row of data to a temporary buffer
          ByteBuffer rowData = createRow(
              row, _writeRowBufferH.getPageBuffer(getPageChannel()));
      
          int rowSize = rowData.remaining();
          if (rowSize > getFormat().MAX_ROW_SIZE) {
            throw new IOException(withErrorContext(
                                      "Row size " + rowSize + " is too large"));
          }

          // get page with space
          dataPage = findFreeRowSpace(rowSize, dataPage, pageNumber);
          pageNumber = _addRowBufferH.getPageNumber();

          // determine where this row will end up on the page
          int rowNum = getRowsOnDataPage(dataPage, getFormat());

          RowIdImpl rowId = new RowIdImpl(pageNumber, rowNum);

          // before we actually write the row data, we verify all the database
          // constraints.
          if(!_indexDatas.isEmpty()) {

            IndexData.PendingChange idxChange = null;
            try {

              // handle foreign keys before adding to table
              _fkEnforcer.addRow(row);

              // prepare index updates
              for(IndexData indexData : _indexDatas) {
                idxChange = indexData.prepareAddRow(row, rowId, idxChange);
              }

              // complete index updates
              IndexData.commitAll(idxChange);

            } catch(ConstraintViolationException ce) {
              IndexData.rollbackAll(idxChange);
              throw ce;
            }
          }

          // we have satisfied all the constraints, write the row
          addDataPageRow(dataPage, rowSize, getFormat(), 0);
          dataPage.put(rowData);

          // return rowTd if desired
          if((row.length > numCols) && 
             (row[numCols] == ColumnImpl.RETURN_ROW_ID)) {
            row[numCols] = rowId;
          }

          ++updateCount;
        }

        writeDataPage(dataPage, pageNumber);
    
        // Update tdef page
        updateTableDefinition(rows.size());

      } catch(Exception rowWriteFailure) {

        boolean isWriteFailure = isWriteFailure(rowWriteFailure);

        if(!isWriteFailure && (autoNumAssignCount > updateCount)) {
          // we assigned some autonumbers which won't get written.  attempt to
          // recover them so we don't get ugly "holes"
          restoreAutoNumbersFromAdd(rows.get(autoNumAssignCount - 1));
        }
        
        if(!isBatchWrite) {
          // just re-throw the original exception
          if(rowWriteFailure instanceof IOException) {
            throw (IOException)rowWriteFailure;
          } 
          throw (RuntimeException)rowWriteFailure;
        }

        // attempt to resolve a partial batch write
        if(isWriteFailure) {

          // we don't really know the status of any of the rows, so clear the
          // update count
          updateCount = 0;

        } else if(updateCount > 0) {
          
          // attempt to flush the rows already written to disk
          try {

            writeDataPage(dataPage, pageNumber);
    
            // Update tdef page
            updateTableDefinition(updateCount);

          } catch(Exception flushFailure) {
            // the flush failure is "worse" as it implies possible database
            // corruption (failed write vs. a row failure which was not a
            // write failure).  we don't know the status of any rows at this
            // point (and the original failure is probably irrelevant)
            LOG.warn(withErrorContext(
                    "Secondary row failure which preceded the write failure"), 
                     rowWriteFailure);
            updateCount = 0;
            rowWriteFailure = flushFailure;
          }
        }

        throw new BatchUpdateException(
            updateCount, withErrorContext("Failed adding rows"),
            rowWriteFailure);
      }

    } finally {
      getPageChannel().finishWrite();
    }
    
    return rows;
  }

  private static boolean isWriteFailure(Throwable t) {
    while(t != null) {
      if((t instanceof IOException) && !(t instanceof JackcessException)) {
        return true;
      }
      t = t.getCause();
    } 
    // some other sort of exception which is not a write failure
    return false;
  }
  
  public Row updateRow(Row row) throws IOException {
    return updateRowFromMap(
        getDefaultCursor().getRowState(), (RowIdImpl)row.getId(), row);
  }

  /**
   * Update the row with the given id.  Provided RowId must have previously
   * been returned from this Table.
   * @return the given row, updated with the current row values
   * @throws IllegalStateException if the given row is not valid, or deleted.
   * @usage _intermediate_method_
   */
  public Object[] updateRow(RowId rowId, Object... row) throws IOException {
    return updateRow(
        getDefaultCursor().getRowState(), (RowIdImpl)rowId, row);
  }

  /**
   * Update the given column's value for the given row id.  Provided RowId
   * must have previously been returned from this Table.
   * @throws IllegalStateException if the given row is not valid, or deleted.
   * @usage _intermediate_method_
   */
  public void updateValue(Column column, RowId rowId, Object value) 
    throws IOException 
  {
    Object[] row = new Object[_columns.size()];
    Arrays.fill(row, Column.KEEP_VALUE);
    column.setRowValue(row, value);

    updateRow(rowId, row);
  }

  public <M extends Map<String,Object>> M updateRowFromMap(
      RowState rowState, RowIdImpl rowId, M row) 
     throws IOException 
  {
    Object[] rowValues = updateRow(rowState, rowId, asUpdateRow(row));
    returnRowValues(row, rowValues, _columns);
    return row;
  }

  /**
   * Update the row for the given rowId.
   * @usage _advanced_method_
   */
  public Object[] updateRow(RowState rowState, RowIdImpl rowId, Object... row) 
    throws IOException 
  {
    requireValidRowId(rowId);
    
    getPageChannel().startWrite();
    try {
    
      // ensure that the relevant row state is up-to-date
      ByteBuffer rowBuffer = positionAtRowData(rowState, rowId);
      int oldRowSize = rowBuffer.remaining();

      requireNonDeletedRow(rowState, rowId);

      // we need to make sure the row is the right length & type (fill with
      // null if too short).
      if((row.length < _columns.size()) || (row.getClass() != Object[].class)) {
        row = dupeRow(row, _columns.size());
      }

      // hang on to the raw values of var length columns we are "keeping".  this
      // will allow us to re-use pre-written var length data, which can save
      // space for things like long value columns.
      Map<ColumnImpl,byte[]> keepRawVarValues = 
        (!_varColumns.isEmpty() ? new HashMap<ColumnImpl,byte[]>() : null);

      // handle various value massaging activities
      for(ColumnImpl column : _columns) {
        
        Object rowValue = null;
        if(column.isAutoNumber()) {
          
          // fill in any auto-numbers (we don't allow autonumber values to be
          // modified)
          rowValue = getRowColumn(getFormat(), rowBuffer, column, rowState, null);
          
        } else {

          rowValue = column.getRowValue(row);
          if(rowValue == Column.KEEP_VALUE) {
            
            // fill in any "keep value" fields (restore old value)
            rowValue = getRowColumn(getFormat(), rowBuffer, column, rowState,
                                    keepRawVarValues);
            
          } else {

            // set oldValue to something that could not possibly be a real value
            Object oldValue = Column.KEEP_VALUE;
            if(_indexColumns.contains(column)) {
              // read (old) row value to help update indexes
              oldValue = getRowColumn(getFormat(), rowBuffer, column, rowState, null);
            } else {
              oldValue = rowState.getRowCacheValue(column.getColumnIndex());
            }

            // if the old value was passed back in, we don't need to validate
            if(oldValue != rowValue) {
              // pass input value through column validator
              rowValue = column.validate(rowValue);
            } 
          }
        }

        column.setRowValue(row, rowValue);
      }

      // generate new row bytes
      ByteBuffer newRowData = createRow(
          row, _writeRowBufferH.getPageBuffer(getPageChannel()), oldRowSize,
          keepRawVarValues);

      if (newRowData.limit() > getFormat().MAX_ROW_SIZE) {
        throw new IOException(withErrorContext(
                "Row size " + newRowData.limit() + " is too large"));
      }

      if(!_indexDatas.isEmpty()) {

        IndexData.PendingChange idxChange = null;
        try {

          Object[] oldRowValues = rowState.getRowCacheValues();

          // check foreign keys before actually updating
          _fkEnforcer.updateRow(oldRowValues, row);

          // prepare index updates
          for(IndexData indexData : _indexDatas) {
            idxChange = indexData.prepareUpdateRow(oldRowValues, rowId, row, 
                                                   idxChange);
          }

          // complete index updates
          IndexData.commitAll(idxChange);

        } catch(ConstraintViolationException ce) {
          IndexData.rollbackAll(idxChange);
          throw ce;
        }
      }
    
      // see if we can squeeze the new row data into the existing row
      rowBuffer.reset();
      int rowSize = newRowData.remaining();

      ByteBuffer dataPage = null;
      int pageNumber = PageChannel.INVALID_PAGE_NUMBER;

      if(oldRowSize >= rowSize) {

        // awesome, slap it in!
        rowBuffer.put(newRowData);

        // grab the page we just updated
        dataPage = rowState.getFinalPage();
        pageNumber = rowState.getFinalRowId().getPageNumber();

      } else {

        // bummer, need to find a new page for the data
        dataPage = findFreeRowSpace(rowSize, null, 
                                    PageChannel.INVALID_PAGE_NUMBER);
        pageNumber = _addRowBufferH.getPageNumber();

        RowIdImpl headerRowId = rowState.getHeaderRowId();      
        ByteBuffer headerPage = rowState.getHeaderPage();
        if(pageNumber == headerRowId.getPageNumber()) {
          // new row is on the same page as header row, share page
          dataPage = headerPage;
        }

        // write out the new row data (set the deleted flag on the new data row
        // so that it is ignored during normal table traversal)
        int rowNum = addDataPageRow(dataPage, rowSize, getFormat(),
                                    DELETED_ROW_MASK);
        dataPage.put(newRowData);

        // write the overflow info into the header row and clear out the
        // remaining header data
        rowBuffer = PageChannel.narrowBuffer(
            headerPage,
            findRowStart(headerPage, headerRowId.getRowNumber(), getFormat()),
            findRowEnd(headerPage, headerRowId.getRowNumber(), getFormat()));
        rowBuffer.put((byte)rowNum);
        ByteUtil.put3ByteInt(rowBuffer, pageNumber);
        ByteUtil.clearRemaining(rowBuffer);

        // set the overflow flag on the header row
        int headerRowIndex = getRowStartOffset(headerRowId.getRowNumber(),
                                               getFormat());
        headerPage.putShort(headerRowIndex,
                            (short)(headerPage.getShort(headerRowIndex)
                                    | OVERFLOW_ROW_MASK));
        if(pageNumber != headerRowId.getPageNumber()) {
          writeDataPage(headerPage, headerRowId.getPageNumber());
        }
      }

      writeDataPage(dataPage, pageNumber);

      updateTableDefinition(0);

    } finally {
      getPageChannel().finishWrite();
    }

    return row;
  }
   
  private ByteBuffer findFreeRowSpace(int rowSize, ByteBuffer dataPage, 
                                      int pageNumber)
    throws IOException
  {
    // assume incoming page is modified
    boolean modifiedPage = true;

    if(dataPage == null) {

      // find owned page w/ free space
      dataPage = findFreeRowSpace(_ownedPages, _freeSpacePages, 
                                  _addRowBufferH);

      if(dataPage == null) {
        // No data pages exist (with free space).  Create a new one.
        return newDataPage();
      }

      // found a page, see if it will work
      pageNumber = _addRowBufferH.getPageNumber();
      // since we just loaded this page, it is not yet modified
      modifiedPage = false;
    }

    if(!rowFitsOnDataPage(rowSize, dataPage, getFormat())) {

      // Last data page is full.  Write old one and create a new one.
      if(modifiedPage) {
        writeDataPage(dataPage, pageNumber);
      }
      _freeSpacePages.removePageNumber(pageNumber, true);

      dataPage = newDataPage();
    }

    return dataPage;
  }

  static ByteBuffer findFreeRowSpace(
      UsageMap ownedPages, UsageMap freeSpacePages,
      TempPageHolder rowBufferH)
    throws IOException
  {
    // find last data page (Not bothering to check other pages for free
    // space.)
    UsageMap.PageCursor revPageCursor = ownedPages.cursor();
    revPageCursor.afterLast();
    while(true) {
      int tmpPageNumber = revPageCursor.getPreviousPage();
      if(tmpPageNumber < 0) {
        break;
      }
      // only use if actually listed in free space pages
      if(!freeSpacePages.containsPageNumber(tmpPageNumber)) {
        continue;
      }
      ByteBuffer dataPage = rowBufferH.setPage(ownedPages.getPageChannel(),
                                               tmpPageNumber);
      if(dataPage.get() == PageTypes.DATA) {
        // found last data page with free space
        return dataPage;
      }
    }

    return null;
  }
    
  /**
   * Updates the table definition after rows are modified.
   */
  private void updateTableDefinition(int rowCountInc) throws IOException
  {
    // load table definition
    ByteBuffer tdefPage = _tableDefBufferH.setPage(getPageChannel(),
                                                   _tableDefPageNumber);
    
    // make sure rowcount and autonumber are up-to-date
    _rowCount += rowCountInc;
    tdefPage.putInt(getFormat().OFFSET_NUM_ROWS, _rowCount);
    tdefPage.putInt(getFormat().OFFSET_NEXT_AUTO_NUMBER, _lastLongAutoNumber);
    int ctypeOff = getFormat().OFFSET_NEXT_COMPLEX_AUTO_NUMBER;
    if(ctypeOff >= 0) {
      tdefPage.putInt(ctypeOff, _lastComplexTypeAutoNumber);
    }

    // write any index changes
    for (IndexData indexData : _indexDatas) {
      // write the unique entry count for the index to the table definition
      // page
      tdefPage.putInt(indexData.getUniqueEntryCountOffset(),
                      indexData.getUniqueEntryCount());
      // write the entry page for the index
      indexData.update();
    }

    // write modified table definition
    getPageChannel().writePage(tdefPage, _tableDefPageNumber);
  }
  
  /**
   * Create a new data page
   * @return Page number of the new page
   */
  private ByteBuffer newDataPage() throws IOException {
    ByteBuffer dataPage = _addRowBufferH.setNewPage(getPageChannel());
    dataPage.put(PageTypes.DATA); //Page type
    dataPage.put((byte) 1); //Unknown
    dataPage.putShort((short)getFormat().DATA_PAGE_INITIAL_FREE_SPACE); //Free space in this page
    dataPage.putInt(_tableDefPageNumber); //Page pointer to table definition
    dataPage.putInt(0); //Unknown
    dataPage.putShort((short)0); //Number of rows on this page
    int pageNumber = _addRowBufferH.getPageNumber();
    getPageChannel().writePage(dataPage, pageNumber);
    _ownedPages.addPageNumber(pageNumber);
    _freeSpacePages.addPageNumber(pageNumber);
    return dataPage;
  }
  
  protected ByteBuffer createRow(Object[] rowArray, ByteBuffer buffer)
    throws IOException
  {
    return createRow(rowArray, buffer, 0,
                     Collections.<ColumnImpl,byte[]>emptyMap());
  }

  /**
   * Serialize a row of Objects into a byte buffer.
   * 
   * @param rowArray row data, expected to be correct length for this table
   * @param buffer buffer to which to write the row data
   * @param minRowSize min size for result row
   * @param rawVarValues optional, pre-written values for var length columns
   *                     (enables re-use of previously written values).
   * @return the given buffer, filled with the row data
   */
  private ByteBuffer createRow(Object[] rowArray, ByteBuffer buffer,
                               int minRowSize, 
                               Map<ColumnImpl,byte[]> rawVarValues)
    throws IOException
  {
    buffer.putShort(_maxColumnCount);
    NullMask nullMask = new NullMask(_maxColumnCount);
    
    //Fixed length column data comes first
    int fixedDataStart = buffer.position();
    int fixedDataEnd = fixedDataStart;
    for (ColumnImpl col : _columns) {

      if(col.isVariableLength()) {
        continue;
      }
        
      Object rowValue = col.getRowValue(rowArray);

      if (col.storeInNullMask()) {
        
        if(col.writeToNullMask(rowValue)) {
          nullMask.markNotNull(col);
        }
        rowValue = null;
      }
          
      if(rowValue != null) {
        
        // we have a value to write
        nullMask.markNotNull(col);

        // remainingRowLength is ignored when writing fixed length data
        buffer.position(fixedDataStart + col.getFixedDataOffset());
        buffer.put(col.write(rowValue, 0));
      }

      // always insert space for the entire fixed data column length
      // (including null values), access expects the row to always be at least
      // big enough to hold all fixed values
      buffer.position(fixedDataStart + col.getFixedDataOffset() +
                      col.getLength());

      // keep track of the end of fixed data
      if(buffer.position() > fixedDataEnd) {
        fixedDataEnd = buffer.position();
      }                  
      
    }

    // reposition at end of fixed data
    buffer.position(fixedDataEnd);
      
    // only need this info if this table contains any var length data
    if(_maxVarColumnCount > 0) {

      int maxRowSize = getFormat().MAX_ROW_SIZE;

      // figure out how much space remains for var length data.  first,
      // account for already written space
      maxRowSize -= buffer.position();
      // now, account for trailer space
      int trailerSize = (nullMask.byteSize() + 4 + (_maxVarColumnCount * 2));
      maxRowSize -= trailerSize;

      // for each non-null long value column we need to reserve a small
      // amount of space so that we don't end up running out of row space
      // later by being too greedy
      for (ColumnImpl varCol : _varColumns) {
        if((varCol.getType().isLongValue()) &&
           (varCol.getRowValue(rowArray) != null)) {
          maxRowSize -= getFormat().SIZE_LONG_VALUE_DEF;
        }
      }
      
      //Now write out variable length column data
      short[] varColumnOffsets = new short[_maxVarColumnCount];
      int varColumnOffsetsIndex = 0;
      for (ColumnImpl varCol : _varColumns) {
        short offset = (short) buffer.position();
        Object rowValue = varCol.getRowValue(rowArray);
        if (rowValue != null) {
          // we have a value
          nullMask.markNotNull(varCol);

          byte[] rawValue = null;
          ByteBuffer varDataBuf = null;
          if(((rawValue = rawVarValues.get(varCol)) != null) && 
             (rawValue.length <= maxRowSize)) {
            // save time and potentially db space, re-use raw value
            varDataBuf = ByteBuffer.wrap(rawValue);
          } else {
            // write column value
            varDataBuf = varCol.write(rowValue, maxRowSize);
          }

          maxRowSize -= varDataBuf.remaining();
          if(varCol.getType().isLongValue()) {
            // we already accounted for some amount of the long value data
            // above.  add that space back so we don't double count
            maxRowSize += getFormat().SIZE_LONG_VALUE_DEF;
          }
          buffer.put(varDataBuf);
        }

        // we do a loop here so that we fill in offsets for deleted columns
        while(varColumnOffsetsIndex <= varCol.getVarLenTableIndex()) {
          varColumnOffsets[varColumnOffsetsIndex++] = offset;
        }
      }

      // fill in offsets for any remaining deleted columns
      while(varColumnOffsetsIndex < varColumnOffsets.length) {
        varColumnOffsets[varColumnOffsetsIndex++] = (short) buffer.position();
      }

      // record where we stopped writing
      int eod = buffer.position();

      // insert padding if necessary
      padRowBuffer(buffer, minRowSize, trailerSize);

      buffer.putShort((short) eod); //EOD marker

      //Now write out variable length offsets
      //Offsets are stored in reverse order
      for (int i = _maxVarColumnCount - 1; i >= 0; i--) {
        buffer.putShort(varColumnOffsets[i]);
      }
      buffer.putShort(_maxVarColumnCount);  //Number of var length columns

    } else {

      // insert padding for row w/ no var cols
      padRowBuffer(buffer, minRowSize, nullMask.byteSize());
    }

    nullMask.write(buffer);  //Null mask
    buffer.flip();
    return buffer;
  }

  /**
   * Fill in all autonumber column values.
   */
  private void handleAutoNumbersForAdd(Object[] row)
    throws IOException
  {
    if(_autoNumColumns.isEmpty()) {
      return;
    }
 
    Object complexAutoNumber = null;
    for(ColumnImpl col : _autoNumColumns) {
      // ignore given row value, use next autonumber
      ColumnImpl.AutoNumberGenerator autoNumGen = col.getAutoNumberGenerator();
      Object rowValue = null;
      if(autoNumGen.getType() != DataType.COMPLEX_TYPE) {
        rowValue = autoNumGen.getNext(null);
      } else {
        // complex type auto numbers are shared across all complex columns
        // in the row
        complexAutoNumber = autoNumGen.getNext(complexAutoNumber);
        rowValue = complexAutoNumber;
      }
      col.setRowValue(row, rowValue);
    }
  }
  
  /**
   * Restores all autonumber column values from a failed add row.
   */
  private void restoreAutoNumbersFromAdd(Object[] row)
    throws IOException
  {
    if(_autoNumColumns.isEmpty()) {
      return;
    }

    for(ColumnImpl col : _autoNumColumns) {
      // restore the last value from the row
      col.getAutoNumberGenerator().restoreLast(col.getRowValue(row));
    }
  }

  private static void padRowBuffer(ByteBuffer buffer, int minRowSize,
                                   int trailerSize)
  {
    int pos = buffer.position();
    if((pos + trailerSize) < minRowSize) {
      // pad the row to get to the min byte size
      int padSize = minRowSize - (pos + trailerSize);
      ByteUtil.clearRange(buffer, pos, pos + padSize);
      ByteUtil.forward(buffer, padSize);
    }
  }

  public int getRowCount() {
    return _rowCount;
  }

  int getNextLongAutoNumber() {
    // note, the saved value is the last one handed out, so pre-increment
    return ++_lastLongAutoNumber;
  }

  int getLastLongAutoNumber() {
    // gets the last used auto number (does not modify)
    return _lastLongAutoNumber;
  }

  void restoreLastLongAutoNumber(int lastLongAutoNumber) {
    // restores the last used auto number
    _lastLongAutoNumber = lastLongAutoNumber - 1;
  }
  
  int getNextComplexTypeAutoNumber() {
    // note, the saved value is the last one handed out, so pre-increment
    return ++_lastComplexTypeAutoNumber;
  }

  int getLastComplexTypeAutoNumber() {
    // gets the last used auto number (does not modify)
    return _lastComplexTypeAutoNumber;
  }

  void restoreLastComplexTypeAutoNumber(int lastComplexTypeAutoNumber) {
    // restores the last used auto number
    _lastComplexTypeAutoNumber = lastComplexTypeAutoNumber - 1;
  }
  
  @Override
  public String toString() {
    return CustomToStringStyle.builder(this)
      .append("type", (_tableType + (!isSystem() ? " (USER)" : " (SYSTEM)")))
      .append("name", _name)
      .append("rowCount", _rowCount)
      .append("columnCount", _columns.size())
      .append("indexCount(data)", _indexCount)
      .append("logicalIndexCount", _logicalIndexCount)
      .append("columns", _columns)
      .append("indexes", _indexes)
      .append("ownedPages", _ownedPages)
      .toString();
  }
  
  /**
   * @return A simple String representation of the entire table in
   *         tab-delimited format
   * @usage _general_method_
   */
  public String display() throws IOException {
    return display(Long.MAX_VALUE);
  }
  
  /**
   * @param limit Maximum number of rows to display
   * @return A simple String representation of the entire table in
   *         tab-delimited format
   * @usage _general_method_
   */
  public String display(long limit) throws IOException {
    reset();
    StringWriter rtn = new StringWriter();
    new ExportUtil.Builder(getDefaultCursor()).setDelimiter("\t").setHeader(true)
      .exportWriter(new BufferedWriter(rtn));
    return rtn.toString();
  }

  /**
   * Updates free space and row info for a new row of the given size in the
   * given data page.  Positions the page for writing the row data.
   * @return the row number of the new row
   * @usage _advanced_method_
   */
  public static int addDataPageRow(ByteBuffer dataPage,
                                   int rowSize,
                                   JetFormat format, 
                                   int rowFlags)
  {
    int rowSpaceUsage = getRowSpaceUsage(rowSize, format);
    
    // Decrease free space record.
    short freeSpaceInPage = dataPage.getShort(format.OFFSET_FREE_SPACE);
    dataPage.putShort(format.OFFSET_FREE_SPACE, (short) (freeSpaceInPage -
                                                         rowSpaceUsage));

    // Increment row count record.
    short rowCount = dataPage.getShort(format.OFFSET_NUM_ROWS_ON_DATA_PAGE);
    dataPage.putShort(format.OFFSET_NUM_ROWS_ON_DATA_PAGE,
                      (short) (rowCount + 1));

    // determine row position
    short rowLocation = findRowEnd(dataPage, rowCount, format);
    rowLocation -= rowSize;

    // write row position
    dataPage.putShort(getRowStartOffset(rowCount, format), 
                      (short)(rowLocation | rowFlags));

    // set position for row data
    dataPage.position(rowLocation);

    return rowCount;
  }

  /**
   * Returns the row count for the current page.  If the page is invalid
   * ({@code null}) or the page is not a DATA page, 0 is returned.
   */
  static int getRowsOnDataPage(ByteBuffer rowBuffer, JetFormat format)
    throws IOException
  {
    int rowsOnPage = 0;
    if((rowBuffer != null) && (rowBuffer.get(0) == PageTypes.DATA)) {
      rowsOnPage = rowBuffer.getShort(format.OFFSET_NUM_ROWS_ON_DATA_PAGE);
    }
    return rowsOnPage;
  }

  /**
   * @throws IllegalStateException if the given rowId is invalid
   */
  private void requireValidRowId(RowIdImpl rowId) {
    if(!rowId.isValid()) {
      throw new IllegalArgumentException(withErrorContext(
              "Given rowId is invalid: " + rowId));
    }
  }
  
  /**
   * @throws IllegalStateException if the given row is invalid or deleted
   */
  private void requireNonDeletedRow(RowState rowState, RowIdImpl rowId)
  {
    if(!rowState.isValid()) {
      throw new IllegalArgumentException(withErrorContext(
          "Given rowId is invalid for this table: " + rowId));
    }
    if(rowState.isDeleted()) {
      throw new IllegalStateException(withErrorContext(
          "Row is deleted: " + rowId));
    }
  }
  
  /**
   * @usage _advanced_method_
   */
  public static boolean isDeletedRow(short rowStart) {
    return ((rowStart & DELETED_ROW_MASK) != 0);
  }
  
  /**
   * @usage _advanced_method_
   */
  public static boolean isOverflowRow(short rowStart) {
    return ((rowStart & OVERFLOW_ROW_MASK) != 0);
  }

  /**
   * @usage _advanced_method_
   */
  public static short cleanRowStart(short rowStart) {
    return (short)(rowStart & OFFSET_MASK);
  }
  
  /**
   * @usage _advanced_method_
   */
  public static short findRowStart(ByteBuffer buffer, int rowNum,
                                   JetFormat format)
  {
    return cleanRowStart(
        buffer.getShort(getRowStartOffset(rowNum, format)));
  }

  /**
   * @usage _advanced_method_
   */
  public static int getRowStartOffset(int rowNum, JetFormat format)
  {
    return format.OFFSET_ROW_START + (format.SIZE_ROW_LOCATION * rowNum);
  }
  
  /**
   * @usage _advanced_method_
   */
  public static short findRowEnd(ByteBuffer buffer, int rowNum,
                                 JetFormat format)
  {
    return (short)((rowNum == 0) ?
                   format.PAGE_SIZE :
                   cleanRowStart(
                       buffer.getShort(getRowEndOffset(rowNum, format))));
  }

  /**
   * @usage _advanced_method_
   */
  public static int getRowEndOffset(int rowNum, JetFormat format)
  {
    return format.OFFSET_ROW_START + (format.SIZE_ROW_LOCATION * (rowNum - 1));
  }

  /**
   * @usage _advanced_method_
   */
  public static int getRowSpaceUsage(int rowSize, JetFormat format)
  {
    return rowSize + format.SIZE_ROW_LOCATION;
  }

  private void getAutoNumberColumns() {
    for(ColumnImpl c : _columns) {
      if(c.isAutoNumber()) {
        _autoNumColumns.add(c);
      }
    }
  }

  /**
   * Returns {@code true} if a row of the given size will fit on the given
   * data page, {@code false} otherwise.
   * @usage _advanced_method_
   */
  public static boolean rowFitsOnDataPage(
      int rowLength, ByteBuffer dataPage, JetFormat format)
    throws IOException
  {
    int rowSpaceUsage = getRowSpaceUsage(rowLength, format);
    short freeSpaceInPage = dataPage.getShort(format.OFFSET_FREE_SPACE);
    int rowsOnPage = getRowsOnDataPage(dataPage, format);
    return ((rowSpaceUsage <= freeSpaceInPage) &&
            (rowsOnPage < format.MAX_NUM_ROWS_ON_DATA_PAGE));
  }

  /**
   * Duplicates and returns a row of data, optionally with a longer length
   * filled with {@code null}.
   */
  static Object[] dupeRow(Object[] row, int newRowLength) {
    Object[] copy = new Object[newRowLength];
    System.arraycopy(row, 0, copy, 0, Math.min(row.length, newRowLength));
    return copy;
  }

  private String withErrorContext(String msg) {
    return withErrorContext(msg, getDatabase(), getName());
  }

  private static String withErrorContext(String msg, DatabaseImpl db,
                                         String tableName) {
    return msg + " (Db=" + db.getName() + ";Table=" + tableName + ")";
  }

  /** various statuses for the row data */
  private enum RowStatus {
    INIT, INVALID_PAGE, INVALID_ROW, VALID, DELETED, NORMAL, OVERFLOW;
  }

  /** the phases the RowState moves through as the data is parsed */
  private enum RowStateStatus {
    INIT, AT_HEADER, AT_FINAL;
  }

  /**
   * Maintains the state of reading a row of data.
   * @usage _advanced_class_
   */
  public final class RowState implements ErrorHandler.Location
  {
    /** Buffer used for reading the header row data pages */
    private final TempPageHolder _headerRowBufferH;
    /** the header rowId */
    private RowIdImpl _headerRowId = RowIdImpl.FIRST_ROW_ID;
    /** the number of rows on the header page */
    private int _rowsOnHeaderPage;
    /** the rowState status */
    private RowStateStatus _status = RowStateStatus.INIT;
    /** the row status */
    private RowStatus _rowStatus = RowStatus.INIT;
    /** buffer used for reading overflow pages */
    private final TempPageHolder _overflowRowBufferH =
      TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);
    /** the row buffer which contains the final data (after following any
        overflow pointers) */
    private ByteBuffer _finalRowBuffer;
    /** the rowId which contains the final data (after following any overflow
        pointers) */
    private RowIdImpl _finalRowId = null;
    /** true if the row values array has data */
    private boolean _haveRowValues;
    /** values read from the last row */
    private final Object[] _rowValues;
    /** null mask for the last row */
    private NullMask _nullMask;
    /** last modification count seen on the table we track this so that the
        rowState can detect updates to the table and re-read any buffered
        data */
    private int _lastModCount;
    /** optional error handler to use when row errors are encountered */
    private ErrorHandler _errorHandler;
    /** cached variable column offsets for jump-table based rows */
    private short[] _varColOffsets;
  
    private RowState(TempBufferHolder.Type headerType) {
      _headerRowBufferH = TempPageHolder.newHolder(headerType);
      _rowValues = new Object[TableImpl.this.getColumnCount()];
      _lastModCount = TableImpl.this._modCount;
    }

    public TableImpl getTable() {
      return TableImpl.this;
    }

    public ErrorHandler getErrorHandler() {
      return((_errorHandler != null) ? _errorHandler :
             getTable().getErrorHandler());
    }

    public void setErrorHandler(ErrorHandler newErrorHandler) {
      _errorHandler = newErrorHandler;
    }
    
    public void reset() {
      _finalRowId = null;
      _finalRowBuffer = null;
      _rowsOnHeaderPage = 0;
      _status = RowStateStatus.INIT;
      _rowStatus = RowStatus.INIT;
      _varColOffsets = null;
      _nullMask = null;
      if(_haveRowValues) {
        Arrays.fill(_rowValues, null);
        _haveRowValues = false;
      }
    }

    public boolean isUpToDate() {
      return(TableImpl.this._modCount == _lastModCount);
    }
    
    private void checkForModification() {
      if(!isUpToDate()) {
        reset();
        _headerRowBufferH.invalidate();
        _overflowRowBufferH.invalidate();
        _lastModCount = TableImpl.this._modCount;
      }
    }
    
    private ByteBuffer getFinalPage()
      throws IOException
    {
      if(_finalRowBuffer == null) {
        // (re)load current page
        _finalRowBuffer = getHeaderPage();
      }
      return _finalRowBuffer;
    }

    public RowIdImpl getFinalRowId() {
      if(_finalRowId == null) {
        _finalRowId = getHeaderRowId();
      }
      return _finalRowId;
    }

    private void setRowStatus(RowStatus rowStatus) {
      _rowStatus = rowStatus;
    }

    public boolean isValid() {
      return(_rowStatus.ordinal() >= RowStatus.VALID.ordinal());
    }
    
    public boolean isDeleted() {
      return(_rowStatus == RowStatus.DELETED);
    }
    
    public boolean isOverflow() {
      return(_rowStatus == RowStatus.OVERFLOW);
    }

    public boolean isHeaderPageNumberValid() {
      return(_rowStatus.ordinal() > RowStatus.INVALID_PAGE.ordinal());
    }
    
    public boolean isHeaderRowNumberValid() {
      return(_rowStatus.ordinal() > RowStatus.INVALID_ROW.ordinal());
    }
    
    private void setStatus(RowStateStatus status) {
      _status = status;
    }
    
    public boolean isAtHeaderRow() {
      return(_status.ordinal() >= RowStateStatus.AT_HEADER.ordinal());
    }
    
    public boolean isAtFinalRow() {
      return(_status.ordinal() >= RowStateStatus.AT_FINAL.ordinal());
    }

    private Object setRowCacheValue(int idx, Object value) {
      _haveRowValues = true;
      _rowValues[idx] = value;
      return value;
    }

    private Object getRowCacheValue(int idx) {
      Object value = _rowValues[idx];
      // only return immutable values.  mutable values could have been
      // modified externally and therefore could return an incorrect value
      return(ColumnImpl.isImmutableValue(value) ? value : null);
    }
    
    public Object[] getRowCacheValues() {
      return dupeRow(_rowValues, _rowValues.length);
    }

    public NullMask getNullMask(ByteBuffer rowBuffer) throws IOException {
      if(_nullMask == null) {
        _nullMask = getRowNullMask(rowBuffer);
      }
      return _nullMask;
    }

    private short[] getVarColOffsets() {
      return _varColOffsets;
    }

    private void setVarColOffsets(short[] varColOffsets) {
      _varColOffsets = varColOffsets;
    }
    
    public RowIdImpl getHeaderRowId() {
      return _headerRowId;
    }

    public int getRowsOnHeaderPage() {
      return _rowsOnHeaderPage;
    }
    
    private ByteBuffer getHeaderPage()
      throws IOException
    {
      checkForModification();
      return _headerRowBufferH.getPage(getPageChannel());
    }

    private ByteBuffer setHeaderRow(RowIdImpl rowId)
      throws IOException
    {
      checkForModification();

      // don't do any work if we are already positioned correctly
      if(isAtHeaderRow() && (getHeaderRowId().equals(rowId))) {
        return(isValid() ? getHeaderPage() : null);
      }

      // rejigger everything
      reset();
      _headerRowId = rowId;
      _finalRowId = rowId;

      int pageNumber = rowId.getPageNumber();
      int rowNumber = rowId.getRowNumber();
      if((pageNumber < 0) || !_ownedPages.containsPageNumber(pageNumber)) {
        setRowStatus(RowStatus.INVALID_PAGE);
        return null;
      }
      
      _finalRowBuffer = _headerRowBufferH.setPage(getPageChannel(),
                                                  pageNumber);
      _rowsOnHeaderPage = getRowsOnDataPage(_finalRowBuffer, getFormat());
      
      if((rowNumber < 0) || (rowNumber >= _rowsOnHeaderPage)) {
        setRowStatus(RowStatus.INVALID_ROW);
        return null;
      }

      setRowStatus(RowStatus.VALID);
      return _finalRowBuffer;
    }

    private ByteBuffer setOverflowRow(RowIdImpl rowId)
      throws IOException
    {
      // this should never see modifications because it only happens within
      // the positionAtRowData method
      if(!isUpToDate()) {
        throw new IllegalStateException(getTable().withErrorContext(
                                            "Table modified while searching?"));
      }
      if(_rowStatus != RowStatus.OVERFLOW) {
        throw new IllegalStateException(getTable().withErrorContext(
                                            "Row is not an overflow row?"));
      }
      _finalRowId = rowId;
      _finalRowBuffer = _overflowRowBufferH.setPage(getPageChannel(),
                                                    rowId.getPageNumber());
      return _finalRowBuffer;
    }

    private Object handleRowError(ColumnImpl column, byte[] columnData,
                                  Exception error)
      throws IOException
    {
      return getErrorHandler().handleRowError(column, columnData,
                                              this, error);
    }  

    @Override
    public String toString() {
      return CustomToStringStyle.valueBuilder(this)
        .append("headerRowId", _headerRowId)
        .append("finalRowId", _finalRowId)
        .toString();
    }
  }
  
}
