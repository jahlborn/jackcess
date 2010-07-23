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

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A single database table
 * <p>
 * Is not thread-safe.
 * 
 * @author Tim McCune
 */
public class Table
  implements Iterable<Map<String, Object>>
{
  
  private static final Log LOG = LogFactory.getLog(Table.class);

  private static final short OFFSET_MASK = (short)0x1FFF;

  private static final short DELETED_ROW_MASK = (short)0x8000;
  
  private static final short OVERFLOW_ROW_MASK = (short)0x4000;

  private static final int MAX_BYTE = 256;

  /** Table type code for system tables */
  public static final byte TYPE_SYSTEM = 0x53;
  /** Table type code for user tables */
  public static final byte TYPE_USER = 0x4e;

  /** comparator which sorts variable length columns vased on their index into
      the variable length offset table */
  private static final Comparator<Column> VAR_LEN_COLUMN_COMPARATOR =
    new Comparator<Column>() {
      public int compare(Column c1, Column c2) {
        return ((c1.getVarLenTableIndex() < c2.getVarLenTableIndex()) ? -1 :
                ((c1.getVarLenTableIndex() > c2.getVarLenTableIndex()) ? 1 :
                 0));
      }
    };

  /** owning database */
  private final Database _database;
  /** Type of the table (either TYPE_SYSTEM or TYPE_USER) */
  private byte _tableType;
  /** Number of indexes on the table */
  private int _indexCount;
  /** Number of index slots for the table */
  private int _indexSlotCount;
  /** Number of rows in the table */
  private int _rowCount;
  /** last long auto number for the table */
  private int _lastLongAutoNumber;
  /** page number of the definition of this table */
  private final int _tableDefPageNumber;
  /** max Number of columns in the table (includes previous deletions) */
  private short _maxColumnCount;
  /** max Number of variable columns in the table */
  private short _maxVarColumnCount;
  /** List of columns in this table, ordered by column number */
  private List<Column> _columns = new ArrayList<Column>();
  /** List of variable length columns in this table, ordered by offset */
  private List<Column> _varColumns = new ArrayList<Column>();
  /** List of indexes on this table */
  private List<Index> _indexes = new ArrayList<Index>();
  /** Table name as stored in Database */
  private final String _name;
  /** Usage map of pages that this table owns */
  private UsageMap _ownedPages;
  /** Usage map of pages that this table owns with free space on them */
  private UsageMap _freeSpacePages;
  /** modification count for the table, keeps row-states up-to-date */
  private int _modCount;
  /** page buffer used to update data pages when adding rows */
  private final TempPageHolder _addRowBufferH =
    TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);
  /** page buffer used to update the table def page */
  private final TempPageHolder _tableDefBufferH =
    TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);
  /** buffer used to writing single rows of data */
  private final TempBufferHolder _singleRowBufferH =
    TempBufferHolder.newHolder(TempBufferHolder.Type.SOFT, true);
  /** "buffer" used to writing multi rows of data (will create new buffer on
      every call) */
  private final TempBufferHolder _multiRowBufferH =
    TempBufferHolder.newHolder(TempBufferHolder.Type.NONE, true);
  /** page buffer used to write out-of-line "long value" data */
  private final TempPageHolder _longValueBufferH =
    TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);
  /** "big index support" is optional */
  private final boolean _useBigIndex;
  /** optional error handler to use when row errors are encountered */
  private ErrorHandler _tableErrorHandler;
  
  /** common cursor for iterating through the table, kept here for historic
      reasons */
  private Cursor _cursor;
  
  /**
   * Only used by unit tests
   */
  Table(boolean testing, List<Column> columns) throws IOException {
    if(!testing) {
      throw new IllegalArgumentException();
    }
    _database = null;
    _tableDefPageNumber = PageChannel.INVALID_PAGE_NUMBER;
    _name = null;
    _useBigIndex = true;
    setColumns(columns);
  }
  
  /**
   * @param database database which owns this table
   * @param tableBuffer Buffer to read the table with
   * @param pageNumber Page number of the table definition
   * @param name Table name
   * @param useBigIndex whether or not "big index support" should be enabled
   *                    for the table
   */
  protected Table(Database database, ByteBuffer tableBuffer,
                  int pageNumber, String name, boolean useBigIndex)
  throws IOException
  {
    _database = database;
    _tableDefPageNumber = pageNumber;
    _name = name;
    _useBigIndex = useBigIndex; 
    int nextPage = tableBuffer.getInt(getFormat().OFFSET_NEXT_TABLE_DEF_PAGE);
    ByteBuffer nextPageBuffer = null;
    while (nextPage != 0) {
      if (nextPageBuffer == null) {
        nextPageBuffer = getPageChannel().createPageBuffer();
      }
      getPageChannel().readPage(nextPageBuffer, nextPage);
      nextPage = nextPageBuffer.getInt(getFormat().OFFSET_NEXT_TABLE_DEF_PAGE);
      ByteBuffer newBuffer = getPageChannel().createBuffer(
          tableBuffer.capacity() + getFormat().PAGE_SIZE - 8);
      newBuffer.put(tableBuffer);
      newBuffer.put(nextPageBuffer.array(), 8, getFormat().PAGE_SIZE - 8);
      tableBuffer = newBuffer;
      tableBuffer.flip();
    }
    readTableDefinition(tableBuffer);
    tableBuffer = null;

    // setup common cursor
    _cursor = Cursor.createCursor(this);
  }

  /**
   * @return The name of the table
   */
  public String getName() {
    return _name;
  }

  public boolean doUseBigIndex() {
    return _useBigIndex;
  }
  
  public int getMaxColumnCount() {
    return _maxColumnCount;
  }
  
  public int getColumnCount() {
    return _columns.size();
  }
  
  public Database getDatabase() {
    return _database;
  }
  
  public JetFormat getFormat() {
    return getDatabase().getFormat();
  }

  public PageChannel getPageChannel() {
    return getDatabase().getPageChannel();
  }

  /**
   * Gets the currently configured ErrorHandler (always non-{@code null}).
   * This will be used to handle all errors unless overridden at the Cursor
   * level.
   */
  public ErrorHandler getErrorHandler() {
    return((_tableErrorHandler != null) ? _tableErrorHandler :
           getDatabase().getErrorHandler());
  }

  /**
   * Sets a new ErrorHandler.  If {@code null}, resets to using the
   * ErrorHandler configured at the Database level.
   */
  public void setErrorHandler(ErrorHandler newErrorHandler) {
    _tableErrorHandler = newErrorHandler;
  }    

  protected int getTableDefPageNumber() {
    return _tableDefPageNumber;
  }

  public RowState createRowState() {
    return new RowState(TempBufferHolder.Type.HARD);
  }

  protected UsageMap.PageCursor getOwnedPagesCursor() {
    return _ownedPages.cursor();
  }
  
  protected TempPageHolder getLongValueBuffer() {
    return _longValueBufferH;
  }

  /**
   * @return All of the columns in this table (unmodifiable List)
   */
  public List<Column> getColumns() {
    return Collections.unmodifiableList(_columns);
  }

  /**
   * @return the column with the given name
   */
  public Column getColumn(String name) {
    for(Column column : _columns) {
      if(column.getName().equalsIgnoreCase(name)) {
        return column;
      }
    }
    throw new IllegalArgumentException("Column with name " + name +
                                       " does not exist in this table");
  }
    
  /**
   * Only called by unit tests
   */
  private void setColumns(List<Column> columns) {
    _columns = columns;
    int colIdx = 0;
    int varLenIdx = 0;
    int fixedOffset = 0;
    for(Column col : _columns) {
      col.setColumnNumber((short)colIdx);
      col.setColumnIndex(colIdx++);
      if(col.isVariableLength()) {
        col.setVarLenTableIndex(varLenIdx++);
        _varColumns.add(col);
      } else {
        col.setFixedDataOffset(fixedOffset);
        fixedOffset += col.getType().getFixedSize();
      }
    }
    _maxColumnCount = (short)_columns.size();
    _maxVarColumnCount = (short)_varColumns.size();
  }
  
  /**
   * @return All of the Indexes on this table (unmodifiable List)
   */
  public List<Index> getIndexes() {
    return Collections.unmodifiableList(_indexes);
  }

  /**
   * @return the index with the given name
   */
  public Index getIndex(String name) {
    for(Index index : _indexes) {
      if(index.getName().equalsIgnoreCase(name)) {
        return index;
      }
    }
    throw new IllegalArgumentException("Index with name " + name +
                                       " does not exist on this table");
  }
    
  /**
   * Only called by unit tests
   */
  int getIndexSlotCount() {
    return _indexSlotCount;
  }

  /**
   * After calling this method, getNextRow will return the first row in the
   * table
   */
  public void reset() {
    _cursor.reset();
  }
  
  /**
   * Delete the current row (retrieved by a call to {@link #getNextRow()}).
   */
  public void deleteCurrentRow() throws IOException {
    _cursor.deleteCurrentRow();
  }

  /**
   * Delete the row on which the given rowState is currently positioned.
   */
  public void deleteRow(RowState rowState, RowId rowId) throws IOException {
    requireValidRowId(rowId);
    
    // ensure that the relevant row state is up-to-date
    ByteBuffer rowBuffer = positionAtRowHeader(rowState, rowId);

    requireNonDeletedRow(rowState, rowId);
    
    // delete flag always gets set in the "header" row (even if data is on
    // overflow row)
    int pageNumber = rowState.getHeaderRowId().getPageNumber();
    int rowNumber = rowState.getHeaderRowId().getRowNumber();

    // use any read rowValues to help update the indexes
    Object[] rowValues = (!_indexes.isEmpty() ?
                          rowState.getRowValues() : null);
    
    int rowIndex = getRowStartOffset(rowNumber, getFormat());
    rowBuffer.putShort(rowIndex, (short)(rowBuffer.getShort(rowIndex)
                                      | DELETED_ROW_MASK | OVERFLOW_ROW_MASK));
    writeDataPage(rowBuffer, pageNumber);

    // update the indexes
    for(Index index : _indexes) {
      index.deleteRow(rowValues, rowId);
    }
    
    // make sure table def gets updated
    updateTableDefinition(-1);
  }
  
  /**
   * @return The next row in this table (Column name -> Column value)
   */
  public Map<String, Object> getNextRow() throws IOException {
    return getNextRow(null);
  }

  /**
   * @param columnNames Only column names in this collection will be returned
   * @return The next row in this table (Column name -> Column value)
   */
  public Map<String, Object> getNextRow(Collection<String> columnNames) 
    throws IOException
  {
    return _cursor.getNextRow(columnNames);
  }
  
  /**
   * Reads a single column from the given row.
   */
  public Object getRowValue(RowState rowState, RowId rowId, Column column)
    throws IOException
  {
    if(this != column.getTable()) {
      throw new IllegalArgumentException(
          "Given column " + column + " is not from this table");
    }
    requireValidRowId(rowId);
    
    // position at correct row
    ByteBuffer rowBuffer = positionAtRowData(rowState, rowId);
    requireNonDeletedRow(rowState, rowId);
    
    return getRowColumn(getFormat(), rowBuffer, getRowNullMask(rowBuffer), column,
                        rowState);
  }

  /**
   * Reads some columns from the given row.
   * @param columnNames Only column names in this collection will be returned
   */
  public Map<String, Object> getRow(
      RowState rowState, RowId rowId, Collection<String> columnNames)
    throws IOException
  {
    requireValidRowId(rowId);

    // position at correct row
    ByteBuffer rowBuffer = positionAtRowData(rowState, rowId);
    requireNonDeletedRow(rowState, rowId);

    return getRow(getFormat(), rowState, rowBuffer, getRowNullMask(rowBuffer), _columns,
                  columnNames);
  }

  /**
   * Reads the row data from the given row buffer.  Leaves limit unchanged.
   * Saves parsed row values to the given rowState.
   */
  private static Map<String, Object> getRow(
	  JetFormat format,
      RowState rowState,
      ByteBuffer rowBuffer,
      NullMask nullMask,
      Collection<Column> columns,
      Collection<String> columnNames)
    throws IOException
  {
    Map<String, Object> rtn = new LinkedHashMap<String, Object>(
        columns.size());
    for(Column column : columns) {

      if((columnNames == null) || (columnNames.contains(column.getName()))) {
        // Add the value to the row data
        rtn.put(column.getName(), 
                getRowColumn(format, rowBuffer, nullMask, column, rowState));
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
                                     NullMask nullMask,
                                     Column column,
                                     RowState rowState)
    throws IOException
  {
    byte[] columnData = null;
    try {

      boolean isNull = nullMask.isNull(column);
      if(column.getType() == DataType.BOOLEAN) {
          // Boolean values are stored in the null mask.  see note about
          // caching below
        return rowState.setRowValue(column.getColumnIndex(),
                                    Boolean.valueOf(!isNull));
      } else if(isNull) {
        // well, that's easy! (no need to update cache w/ null)
        return null;
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
      columnData = new byte[colDataLen];
      rowBuffer.position(colDataPos);
      rowBuffer.get(columnData);

      // parse the column data.  we cache the row values in order to be able
      // to update the index on row deletion.  note, most of the returned
      // values are immutable, except for binary data (returned as byte[]),
      // but binary data shouldn't be indexed anyway.
      return rowState.setRowValue(column.getColumnIndex(), 
                                  column.read(columnData));

    } catch(Exception e) {

      // cache "raw" row value.  see note about caching above
      rowState.setRowValue(column.getColumnIndex(), 
                           Column.rawDataWrapper(columnData));

      return rowState.handleRowError(column, columnData, e);
    }
  }

  static short[] readJumpTableVarColOffsets(
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

      if((jumpsUsed < numJumps) && 
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
   */
  public static ByteBuffer positionAtRowHeader(RowState rowState,
                                               RowId rowId)
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
   */
  public static ByteBuffer positionAtRowData(RowState rowState,
                                             RowId rowId)
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
          throw new IOException("invalid overflow row info");
        }
      
        // Overflow page.  the "row" data in the current page points to
        // another page/row
        int overflowRowNum = ByteUtil.getUnsignedByte(rowBuffer, rowStart);
        int overflowPageNum = ByteUtil.get3ByteInt(rowBuffer, rowStart + 1);
        rowBuffer = rowState.setOverflowRow(
            new RowId(overflowPageNum, overflowRowNum));
        rowNum = overflowRowNum;
      
      } else {

        rowState.setStatus(RowStateStatus.AT_FINAL);
        return PageChannel.narrowBuffer(rowBuffer, rowStart, rowEnd);
      }
    }
  }

  
  /**
   * Calls <code>reset</code> on this table and returns an unmodifiable
   * Iterator which will iterate through all the rows of this table.  Use of
   * the Iterator follows the same restrictions as a call to
   * <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String, Object>> iterator()
  {
    return iterator(null);
  }
  
  /**
   * Calls <code>reset</code> on this table and returns an unmodifiable
   * Iterator which will iterate through all the rows of this table, returning
   * only the given columns.  Use of the Iterator follows the same
   * restrictions as a call to <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String, Object>> iterator(Collection<String> columnNames)
  {
    reset();
    return _cursor.iterator(columnNames);
  }

  /**
   * Writes a new table defined by the given columns to the database.
   * @return the first page of the new table's definition
   */
  public static int writeTableDefinition(
      List<Column> columns, PageChannel pageChannel, JetFormat format,
      Charset charset)
    throws IOException
  {
    // first, create the usage map page
    int usageMapPageNumber = pageChannel.writeNewPage(
        createUsageMapDefinitionBuffer(pageChannel, format));

    // next, determine how big the table def will be (in case it will be more
    // than one page)
    int totalTableDefSize = format.SIZE_TDEF_HEADER +
      (format.SIZE_COLUMN_DEF_BLOCK * columns.size()) +
      format.SIZE_TDEF_TRAILER;
    for(Column col : columns) {
      // we add the number of bytes for the column name and 2 bytes for the
      // length of the column name
      int nameByteLen = (col.getName().length() *
                         JetFormat.TEXT_FIELD_UNIT_SIZE);
      totalTableDefSize += nameByteLen + 2;
    }
    
    // now, create the table definition
    ByteBuffer buffer = pageChannel.createBuffer(Math.max(totalTableDefSize,
                                                          format.PAGE_SIZE));
    writeTableDefinitionHeader(buffer, columns, usageMapPageNumber,
                               totalTableDefSize, format);
    writeColumnDefinitions(buffer, columns, format, charset); 
    
    //End of tabledef
    buffer.put((byte) 0xff);
    buffer.put((byte) 0xff);

    // write table buffer to database
    int tdefPageNumber = PageChannel.INVALID_PAGE_NUMBER;
    if(totalTableDefSize <= format.PAGE_SIZE) {
      
      // easy case, fits on one page
      buffer.putShort(format.OFFSET_FREE_SPACE,
                      (short)(buffer.remaining() - 8)); // overwrite page free space
      // Write the tdef page to disk.
      tdefPageNumber = pageChannel.writeNewPage(buffer);
      
    } else {

      // need to split across multiple pages
      ByteBuffer partialTdef = pageChannel.createPageBuffer();
      buffer.rewind();
      int nextTdefPageNumber = PageChannel.INVALID_PAGE_NUMBER;
      while(buffer.hasRemaining()) {

        // reset for next write
        partialTdef.clear();
        
        if(tdefPageNumber == PageChannel.INVALID_PAGE_NUMBER) {
          
          // this is the first page.  note, the first page already has the
          // page header, so no need to write it here
          tdefPageNumber = pageChannel.allocateNewPage();
          nextTdefPageNumber = tdefPageNumber;
          
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
       
    return tdefPageNumber;
  }

  /**
   * @param buffer Buffer to write to
   * @param columns List of Columns in the table
   */
  private static void writeTableDefinitionHeader(
      ByteBuffer buffer, List<Column> columns,
      int usageMapPageNumber, int totalTableDefSize, JetFormat format)
    throws IOException
  {
    //Start writing the tdef
    writeTablePageHeader(buffer);
    buffer.putInt(totalTableDefSize);  //Length of table def
    buffer.put((byte) 0x59);  //Unknown
    buffer.put((byte) 0x06);  //Unknown
    buffer.putShort((short) 0); //Unknown
    buffer.putInt(0);  //Number of rows
    buffer.putInt(0); //Last Autonumber
    buffer.put((byte) 1); // this makes autonumbering work in access
    for (int i = 0; i < 15; i++) {  //Unknown
      buffer.put((byte) 0);
    }
    buffer.put(Table.TYPE_USER); //Table type
    buffer.putShort((short) columns.size()); //Max columns a row will have
    buffer.putShort(Column.countVariableLength(columns));  //Number of variable columns in table
    buffer.putShort((short) columns.size()); //Number of columns in table
    buffer.putInt(0);  //Number of indexes in table
    buffer.putInt(0);  //Number of indexes in table
    buffer.put((byte) 0); //Usage map row number
    ByteUtil.put3ByteInt(buffer, usageMapPageNumber);  //Usage map page number
    buffer.put((byte) 1); //Free map row number
    ByteUtil.put3ByteInt(buffer, usageMapPageNumber);  //Free map page number
    if (LOG.isDebugEnabled()) {
      int position = buffer.position();
      buffer.rewind();
      LOG.debug("Creating new table def block:\n" + ByteUtil.toHexString(
          buffer, format.SIZE_TDEF_HEADER));
      buffer.position(position);
    }
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
   * @param buffer Buffer to write to
   * @param columns List of Columns to write definitions for
   */
  private static void writeColumnDefinitions(
      ByteBuffer buffer, List<Column> columns, JetFormat format,
      Charset charset)
    throws IOException
  {
    short columnNumber = (short) 0;
    short fixedOffset = (short) 0;
    short variableOffset = (short) 0;
    // we specifically put the "long variable" values after the normal
    // variable length values so that we have a better chance of fitting it
    // all (because "long variable" values can go in separate pages)
    short longVariableOffset =
      Column.countNonLongVariableLength(columns);
    for (Column col : columns) {
      int position = buffer.position();
      buffer.put(col.getType().getValue());
      buffer.put((byte) 0x59);  //Unknown
      buffer.put((byte) 0x06);  //Unknown
      buffer.putShort((short) 0); //Unknown
      buffer.putShort(columnNumber);  //Column Number
      if (col.isVariableLength()) {
        if(!col.getType().isLongValue()) {
          buffer.putShort(variableOffset++);
        } else {
          buffer.putShort(longVariableOffset++);
        }          
      } else {
        buffer.putShort((short) 0);
      }
      buffer.putShort(columnNumber); //Column Number again
      if(col.getType().getHasScalePrecision()) {
        buffer.put(col.getPrecision());  // numeric precision
        buffer.put(col.getScale());  // numeric scale
      } else {
        buffer.put((byte) 0x00); //unused
        buffer.put((byte) 0x00); //unused
      }
      buffer.putShort((short) 0); //Unknown
      buffer.put(getColumnBitFlags(col)); // misc col flags
      if (col.isCompressedUnicode()) {  //Compressed
        buffer.put((byte) 1);
      } else {
        buffer.put((byte) 0);
      }
      buffer.putInt(0); //Unknown, but always 0.
      //Offset for fixed length columns
      if (col.isVariableLength()) {
        buffer.putShort((short) 0);
      } else {
        buffer.putShort(fixedOffset);
        fixedOffset += col.getType().getFixedSize(col.getLength());
      }
      if(!col.getType().isLongValue()) {
        buffer.putShort(col.getLength()); //Column length
      } else {
        buffer.putShort((short)0x0000); // unused
      }
      columnNumber++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating new column def block\n" + ByteUtil.toHexString(
            buffer, position, format.SIZE_COLUMN_DEF_BLOCK));
      }
    }
    for (Column col : columns) {
      writeName(buffer, col.getName(), charset);
    }
  }

  /**
   * Writes the given name into the given buffer in the format as expected by
   * {@link #readName}.
   */
  private static void writeName(ByteBuffer buffer, String name,
                                Charset charset)
  {
      ByteBuffer encName = Column.encodeUncompressedText(name, charset);
      buffer.putShort((short) encName.remaining());
      buffer.put(encName);
  }

  /**
   * Constructs a byte containing the flags for the given column.
   */
  private static byte getColumnBitFlags(Column col) {
    byte flags = Column.UNKNOWN_FLAG_MASK;
    if(!col.isVariableLength()) {
      flags |= Column.FIXED_LEN_FLAG_MASK;
    }
    if(col.isAutoNumber()) {
      flags |= col.getAutoNumberGenerator().getColumnFlags();
    }
    return flags;
  }
  
  /**
   * Create the usage map definition page buffer.  The "used pages" map is in
   * row 0, the "pages with free space" map is in row 1.
   */
  private static ByteBuffer createUsageMapDefinitionBuffer(
      PageChannel pageChannel, JetFormat format)
    throws IOException
  {
    int usageMapRowLength = format.OFFSET_USAGE_MAP_START +
      format.USAGE_MAP_TABLE_BYTE_LENGTH;
    int freeSpace = format.PAGE_INITIAL_FREE_SPACE
      - (2 * getRowSpaceUsage(usageMapRowLength, format));
    
    ByteBuffer rtn = pageChannel.createPageBuffer();
    rtn.put(PageTypes.DATA);
    rtn.put((byte) 0x1);  //Unknown
    rtn.putShort((short)freeSpace);  //Free space in page
    rtn.putInt(0); //Table definition
    rtn.putInt(0); //Unknown
    rtn.putShort((short) 2); //Number of records on this page

    // write two rows of usage map definitions
    int rowStart = findRowEnd(rtn, 0, format) - usageMapRowLength;
    for(int i = 0; i < 2; ++i) {
      rtn.putShort(getRowStartOffset(i, format), (short)rowStart);
      if(i == 0) {
        // initial "usage pages" map definition
        rtn.put(rowStart, UsageMap.MAP_TYPE_REFERENCE);
      } else {
        // initial "pages with free space" map definition
        rtn.put(rowStart, UsageMap.MAP_TYPE_INLINE);
      }
      rowStart -= usageMapRowLength;
    }
        
    return rtn;
  }
    
  /**
   * Read the table definition
   */
  private void readTableDefinition(ByteBuffer tableBuffer) throws IOException
  {
    if (LOG.isDebugEnabled()) {
      tableBuffer.rewind();
      LOG.debug("Table def block:\n" + ByteUtil.toHexString(tableBuffer,
          getFormat().SIZE_TDEF_HEADER));
    }
    _rowCount = tableBuffer.getInt(getFormat().OFFSET_NUM_ROWS);
    _lastLongAutoNumber = tableBuffer.getInt(getFormat().OFFSET_NEXT_AUTO_NUMBER);
    _tableType = tableBuffer.get(getFormat().OFFSET_TABLE_TYPE);
    _maxColumnCount = tableBuffer.getShort(getFormat().OFFSET_MAX_COLS);
    _maxVarColumnCount = tableBuffer.getShort(getFormat().OFFSET_NUM_VAR_COLS);
    short columnCount = tableBuffer.getShort(getFormat().OFFSET_NUM_COLS);
    _indexSlotCount = tableBuffer.getInt(getFormat().OFFSET_NUM_INDEX_SLOTS);
    _indexCount = tableBuffer.getInt(getFormat().OFFSET_NUM_INDEXES);

    int rowNum = ByteUtil.getUnsignedByte(
        tableBuffer, getFormat().OFFSET_OWNED_PAGES);
    int pageNum = ByteUtil.get3ByteInt(tableBuffer, getFormat().OFFSET_OWNED_PAGES + 1);
    _ownedPages = UsageMap.read(getDatabase(), pageNum, rowNum, false);
    rowNum = ByteUtil.getUnsignedByte(
        tableBuffer, getFormat().OFFSET_FREE_SPACE_PAGES);
    pageNum = ByteUtil.get3ByteInt(tableBuffer, getFormat().OFFSET_FREE_SPACE_PAGES + 1);
    _freeSpacePages = UsageMap.read(getDatabase(), pageNum, rowNum, false);
    
    for (int i = 0; i < _indexCount; i++) {
      int uniqueEntryCountOffset =
        (getFormat().OFFSET_INDEX_DEF_BLOCK +
         (i * getFormat().SIZE_INDEX_DEFINITION) + 4);
      int uniqueEntryCount = tableBuffer.getInt(uniqueEntryCountOffset);
      _indexes.add(createIndex(uniqueEntryCount, uniqueEntryCountOffset));
    }
    
    int colOffset = getFormat().OFFSET_INDEX_DEF_BLOCK +
        _indexCount * getFormat().SIZE_INDEX_DEFINITION;
    for (int i = 0; i < columnCount; i++) {
      Column column = new Column(this, tableBuffer,
          colOffset + (i * getFormat().SIZE_COLUMN_HEADER));
      _columns.add(column);
      if(column.isVariableLength()) {
        // also shove it in the variable columns list, which is ordered
        // differently from the _columns list
        _varColumns.add(column);
      }
    }
    tableBuffer.position(colOffset +
                         (columnCount * getFormat().SIZE_COLUMN_HEADER));
    for (int i = 0; i < columnCount; i++) {
      Column column = _columns.get(i);
      column.setName(readName(tableBuffer));
    }    
    Collections.sort(_columns);

    // setup the data index for the columns
    int colIdx = 0;
    for(Column col : _columns) {
      col.setColumnIndex(colIdx++);
    }

    // sort variable length columns based on their index into the variable
    // length offset table, because we will write the columns in this order
    Collections.sort(_varColumns, VAR_LEN_COLUMN_COMPARATOR);
    
    int idxOffset = tableBuffer.position();
    tableBuffer.position(idxOffset +
                     (getFormat().OFFSET_INDEX_NUMBER_BLOCK * _indexCount));

    // if there are more index slots than indexes, the initial slots are
    // always empty/invalid, so we skip that data
    int firstRealIdx = (_indexSlotCount - _indexCount);
    
    for (int i = 0; i < _indexSlotCount; i++) {

      ByteUtil.forward(tableBuffer, getFormat().SKIP_BEFORE_INDEX_SLOT); //Forward past Unknown
      tableBuffer.getInt(); //Forward past alternate index number
      int indexNumber = tableBuffer.getInt();
      ByteUtil.forward(tableBuffer, 11);
      byte indexType = tableBuffer.get();
      ByteUtil.forward(tableBuffer, getFormat().SKIP_AFTER_INDEX_SLOT); //Skip past Unknown

      if(i < firstRealIdx) {
        // ignore this info
        continue;
      }

      Index index = _indexes.get(i - firstRealIdx);
      index.setIndexNumber(indexNumber);
      index.setIndexType(indexType);
    }

    // read actual index names
    for (int i = 0; i < _indexSlotCount; i++) {
      if(i < firstRealIdx) {
        // for each empty index slot, there is some weird sort of name, skip
        // it
        skipName(tableBuffer);
        continue;
      }
        
      _indexes.get(i - firstRealIdx)
        .setName(readName(tableBuffer));
    }
    int idxEndOffset = tableBuffer.position();
    
    Collections.sort(_indexes);

    // go back to index column info after sorting
    tableBuffer.position(idxOffset);
    for (int i = 0; i < _indexCount; i++) {
      ByteUtil.forward(tableBuffer, getFormat().SKIP_BEFORE_INDEX); //Forward past Unknown
      _indexes.get(i).read(tableBuffer, _columns);
    }

    // reset to end of index info
    tableBuffer.position(idxEndOffset);
  }

  /**
   * Creates an index with the given initial info.
   */
  private Index createIndex(int uniqueEntryCount, int uniqueEntryCountOffset)
  {
    return(_useBigIndex ?
           new BigIndex(this, uniqueEntryCount, uniqueEntryCountOffset) :
           new SimpleIndex(this, uniqueEntryCount, uniqueEntryCountOffset));
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
    byte[] nameBytes = new byte[nameLength];
    buffer.get(nameBytes);
    return Column.decodeUncompressedText(nameBytes, 
                                         getDatabase().getCharset());
  }
  
  /**
   * Skips past a name int the buffer at the current position.  The
   * expected name format is the same as that for {@link #readName}.
   */
  private void skipName(ByteBuffer buffer) {
	int nameLength = readNameLength(buffer);
    ByteUtil.forward(buffer, nameLength);
  }
  
  /**
   * Returns a name length read from the buffer at the current position.
   */
  private int readNameLength(ByteBuffer buffer) { 
    return ByteUtil.getUnsignedVarInt(buffer, getFormat().SIZE_NAME_LENGTH);
  }
  
  /**
   * Converts a map of columnName -> columnValue to an array of row values
   * appropriate for a call to {@link #addRow(Object...)}.
   */
  public Object[] asRow(Map<String,Object> rowMap) {
    return asRow(rowMap, null);
  }
  
  /**
   * Converts a map of columnName -> columnValue to an array of row values
   * appropriate for a call to {@link #updateCurrentRow(Object...)}.
   */
  public Object[] asUpdateRow(Map<String,Object> rowMap) {
    return asRow(rowMap, Column.KEEP_VALUE);
  }

  /**
   * Converts a map of columnName -> columnValue to an array of row values.
   */
  private Object[] asRow(Map<String,Object> rowMap, Object defaultValue)
  {
    Object[] row = new Object[_columns.size()];
    if(defaultValue != null) {
      Arrays.fill(row, defaultValue);
    }
    if(rowMap == null) {
      return row;
    }
    for(Column col : _columns) {
      if(rowMap.containsKey(col.getName())) {
        row[col.getColumnIndex()] = rowMap.get(col.getName());
      }
    }
    return row;
  }
  
  /**
   * Add a single row to this table and write it to disk
   * <p>
   * Note, if this table has an auto-number column, the value written will be
   * put back into the given row array.
   *
   * @param row row values for a single row.  the row will be modified if
   *            this table contains an auto-number column, otherwise it
   *            will not be modified.
   */
  public void addRow(Object... row) throws IOException {
    addRows(Collections.singletonList(row), _singleRowBufferH);
  }
  
  /**
   * Add multiple rows to this table, only writing to disk after all
   * rows have been written, and every time a data page is filled.  This
   * is much more efficient than calling <code>addRow</code> multiple times.
   * <p>
   * Note, if this table has an auto-number column, the values written will be
   * put back into the given row arrays.
   * 
   * @param rows List of Object[] row values.  the rows will be modified if
   *             this table contains an auto-number column, otherwise they
   *             will not be modified.
   */
  public void addRows(List<? extends Object[]> rows) throws IOException {
    addRows(rows, _multiRowBufferH);
  }
  
  /**
   * Add multiple rows to this table, only writing to disk after all
   * rows have been written, and every time a data page is filled.
   * @param inRows List of Object[] row values
   * @param writeRowBufferH TempBufferHolder used to generate buffers for
   *                        writing the row data
   */
  private void addRows(List<? extends Object[]> inRows,
                       TempBufferHolder writeRowBufferH)
    throws IOException
  {
    if(inRows.isEmpty()) {
      return;
    }

    // copy the input rows to a modifiable list so we can update the elements
    List<Object[]> rows = new ArrayList<Object[]>(inRows);
    ByteBuffer[] rowData = new ByteBuffer[rows.size()];
    for (int i = 0; i < rows.size(); i++) {

      // we need to make sure the row is the right length (fill with null).
      // note, if the row is copied the caller will not be able to access any
      // generated auto-number value, but if they need that info they should
      // use a row array of the right size!
      Object[] row = rows.get(i);
      if(row.length < _columns.size()) {
        row = dupeRow(row, _columns.size());
        // we copied the row, so put the copy back into the rows list
        rows.set(i, row);
      }

      // write the row of data to a temporary buffer
      rowData[i] = createRow(row, getFormat().MAX_ROW_SIZE,
                             writeRowBufferH.getPageBuffer(getPageChannel()),
                             false, 0);
      
      if (rowData[i].limit() > getFormat().MAX_ROW_SIZE) {
        throw new IOException("Row size " + rowData[i].limit() +
                              " is too large");
      }
    }

    ByteBuffer dataPage = null;
    int pageNumber = PageChannel.INVALID_PAGE_NUMBER;
    
    for (int i = 0; i < rowData.length; i++) {
      int rowSize = rowData[i].remaining();

      // get page with space
      dataPage = findFreeRowSpace(rowSize, dataPage, pageNumber);
      pageNumber = _addRowBufferH.getPageNumber();

      // write out the row data
      int rowNum = addDataPageRow(dataPage, rowSize, getFormat(), 0);
      dataPage.put(rowData[i]);

      // update the indexes
      RowId rowId = new RowId(pageNumber, rowNum);
      for(Index index : _indexes) {
        index.addRow(rows.get(i), rowId);
      }
    }

    writeDataPage(dataPage, pageNumber);
    
    // Update tdef page
    updateTableDefinition(rows.size());
  }

  /**
   * Updates the current row to the new values.
   * <p>
   * Note, if this table has an auto-number column(s), the existing value(s)
   * will be maintained, unchanged.
   *
   * @param row new row values for the current row.
   */
  public void updateCurrentRow(Object... row) throws IOException {
     _cursor.updateCurrentRow(row);
  }
  
  /**
   * Update the row on which the given rowState is currently positioned.
   */
  public void updateRow(RowState rowState, RowId rowId, Object... row) 
    throws IOException 
  {
    requireValidRowId(rowId);
    
    // ensure that the relevant row state is up-to-date
    ByteBuffer rowBuffer = positionAtRowData(rowState, rowId);
    int oldRowSize = rowBuffer.remaining();

    requireNonDeletedRow(rowState, rowId);

    // we need to make sure the row is the right length (fill with null).
    if(row.length < _columns.size()) {
      row = dupeRow(row, _columns.size());
    }

    // fill in any auto-numbers (we don't allow autonumber values to be
    // modified) or "keep value" fields
    NullMask nullMask = getRowNullMask(rowBuffer);
    for(Column column : _columns) {
      if(column.isAutoNumber() || 
         (row[column.getColumnIndex()] == Column.KEEP_VALUE)) {
        row[column.getColumnIndex()] = getRowColumn(getFormat(), rowBuffer, nullMask,
                                                    column, rowState);
      }
    }

    // generate new row bytes
    ByteBuffer newRowData = createRow(
        row, getFormat().MAX_ROW_SIZE,
        _singleRowBufferH.getPageBuffer(getPageChannel()), true, oldRowSize);

    if (newRowData.limit() > getFormat().MAX_ROW_SIZE) {
      throw new IOException("Row size " + newRowData.limit() + 
                            " is too large");
    }

    Object[] oldRowValues = (!_indexes.isEmpty() ?
                             rowState.getRowValues() : null);

    // delete old values from indexes
    for(Index index : _indexes) {
      index.deleteRow(oldRowValues, rowId);
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

      RowId headerRowId = rowState.getHeaderRowId();      
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

    // update the indexes
    for(Index index : _indexes) {
      index.addRow(row, rowId);
    }

    writeDataPage(dataPage, pageNumber);

    updateTableDefinition(0);
  }
   
  private ByteBuffer findFreeRowSpace(int rowSize, ByteBuffer dataPage, 
                                      int pageNumber)
    throws IOException
  {
    if(dataPage == null) {

      // find last data page (Not bothering to check other pages for free
      // space.)
      UsageMap.PageCursor revPageCursor = _ownedPages.cursor();
      revPageCursor.afterLast();
      while(true) {
        int tmpPageNumber = revPageCursor.getPreviousPage();
        if(tmpPageNumber < 0) {
          break;
        }
        dataPage = _addRowBufferH.setPage(getPageChannel(), tmpPageNumber);
        if(dataPage.get() == PageTypes.DATA) {
          // found last data page, only use if actually listed in free space
          // pages
          if(_freeSpacePages.containsPageNumber(tmpPageNumber)) {
            pageNumber = tmpPageNumber;
          }
          break;
        }
      }

      if(pageNumber == PageChannel.INVALID_PAGE_NUMBER) {
        // No data pages exist (with free space).  Create a new one.
        return newDataPage();
      }
    
    }

    if(!rowFitsOnDataPage(rowSize, dataPage, getFormat())) {

      // Last data page is full.  Create a new one.
      writeDataPage(dataPage, pageNumber);
      _freeSpacePages.removePageNumber(pageNumber);

      dataPage = newDataPage();
    }

    return dataPage;
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

    // write any index changes
    for (Index index : _indexes) {
      // write the unique entry count for the index to the table definition
      // page
      tdefPage.putInt(index.getUniqueEntryCountOffset(),
                      index.getUniqueEntryCount());
      // write the entry page for the index
      index.update();
    }

    // write modified table definition
    getPageChannel().writePage(tdefPage, _tableDefPageNumber);
  }
  
  /**
   * Create a new data page
   * @return Page number of the new page
   */
  private ByteBuffer newDataPage() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating new data page");
    }
    ByteBuffer dataPage = _addRowBufferH.setNewPage(getPageChannel());
    dataPage.put(PageTypes.DATA); //Page type
    dataPage.put((byte) 1); //Unknown
    dataPage.putShort((short)getFormat().PAGE_INITIAL_FREE_SPACE); //Free space in this page
    dataPage.putInt(_tableDefPageNumber); //Page pointer to table definition
    dataPage.putInt(0); //Unknown
    dataPage.putShort((short)0); //Number of rows on this page
    int pageNumber = _addRowBufferH.getPageNumber();
    getPageChannel().writePage(dataPage, pageNumber);
    _ownedPages.addPageNumber(pageNumber);
    _freeSpacePages.addPageNumber(pageNumber);
    return dataPage;
  }
  
  /**
   * Serialize a row of Objects into a byte buffer.
   * <p>
   * Note, if this table has an auto-number column, the value written will be
   * put back into the given row array.
   * 
   * @param rowArray row data, expected to be correct length for this table
   * @param maxRowSize max size the data can be for this row
   * @param buffer buffer to which to write the row data
   * @return the given buffer, filled with the row data
   */
  ByteBuffer createRow(Object[] rowArray, int maxRowSize, ByteBuffer buffer,
                       boolean isUpdate, int minRowSize)
    throws IOException
  {
    buffer.putShort(_maxColumnCount);
    NullMask nullMask = new NullMask(_maxColumnCount);
    
    //Fixed length column data comes first
    int fixedDataStart = buffer.position();
    int fixedDataEnd = fixedDataStart;
    for (Column col : _columns) {

      if(!col.isVariableLength()) {
        
        Object rowValue = rowArray[col.getColumnIndex()];

        if (col.getType() == DataType.BOOLEAN) {
        
          if(Column.toBooleanValue(rowValue)) {
            //Booleans are stored in the null mask
            nullMask.markNotNull(col);
          }
        
        } else {

          if(col.isAutoNumber() && !isUpdate) {
            
            // ignore given row value, use next autonumber
            rowValue = col.getAutoNumberGenerator().getNext();

            // we need to stick this back in the row so that the indexes get
            // updated correctly (and caller can get the generated value)
            rowArray[col.getColumnIndex()] = rowValue;
          }
          
          if(rowValue != null) {
        
            // we have a value
            nullMask.markNotNull(col);

            //remainingRowLength is ignored when writing fixed length data
            buffer.position(fixedDataStart + col.getFixedDataOffset());
            buffer.put(col.write(rowValue, 0));

            // keep track of the end of fixed data
            if(buffer.position() > fixedDataEnd) {
              fixedDataEnd = buffer.position();
            }
          }
        }
      }
    }

    // reposition at end of fixed data
    buffer.position(fixedDataEnd);
      
    // only need this info if this table contains any var length data
    if(_maxVarColumnCount > 0) {

      // figure out how much space remains for var length data.  first,
      // account for already written space
      maxRowSize -= buffer.position();
      // now, account for trailer space
      int trailerSize = (nullMask.byteSize() + 4 + (_maxVarColumnCount * 2));
      maxRowSize -= trailerSize;

      // for each non-null long value column we need to reserve a small
      // amount of space so that we don't end up running out of row space
      // later by being too greedy
      for (Column varCol : _varColumns) {
        if((varCol.getType().isLongValue()) &&
           (rowArray[varCol.getColumnIndex()] != null)) {
          maxRowSize -= getFormat().SIZE_LONG_VALUE_DEF;
        }
      }
      
      //Now write out variable length column data
      short[] varColumnOffsets = new short[_maxVarColumnCount];
      int varColumnOffsetsIndex = 0;
      for (Column varCol : _varColumns) {
        short offset = (short) buffer.position();
        Object rowValue = rowArray[varCol.getColumnIndex()];
        if (rowValue != null) {
          // we have a value
          nullMask.markNotNull(varCol);

          ByteBuffer varDataBuf = varCol.write(rowValue, maxRowSize);
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating new data block:\n" + ByteUtil.toHexString(buffer, buffer.limit()));
    }
    return buffer;
  }

  private void padRowBuffer(ByteBuffer buffer, int minRowSize, int trailerSize)
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
  
  @Override
  public String toString() {
    StringBuilder rtn = new StringBuilder();
    rtn.append("Type: " + _tableType);
		rtn.append("\nName: " + _name);
    rtn.append("\nRow count: " + _rowCount);
    rtn.append("\nColumn count: " + _columns.size());
    rtn.append("\nIndex count: " + _indexCount);
    rtn.append("\nColumns:\n");
    for(Column col : _columns) {
      rtn.append(col);
    }
    rtn.append("\nIndexes:\n");
    for(Index index : _indexes) {
      rtn.append(index);
    }
    rtn.append("\nOwned pages: " + _ownedPages + "\n");
    return rtn.toString();
  }
  
  /**
   * @return A simple String representation of the entire table in tab-delimited format
   */
  public String display() throws IOException {
    return display(Long.MAX_VALUE);
  }
  
  /**
   * @param limit Maximum number of rows to display
   * @return A simple String representation of the entire table in tab-delimited format
   */
  public String display(long limit) throws IOException {
    reset();
    StringBuilder rtn = new StringBuilder();
    for(Iterator<Column> iter = _columns.iterator(); iter.hasNext(); ) {
      Column col = iter.next();
      rtn.append(col.getName());
      if (iter.hasNext()) {
        rtn.append("\t");
      }
    }
    rtn.append("\n");
    Map<String, Object> row;
    int rowCount = 0;
    while ((rowCount++ < limit) && (row = getNextRow()) != null) {
      for(Iterator<Object> iter = row.values().iterator(); iter.hasNext(); ) {
        Object obj = iter.next();
        if (obj instanceof byte[]) {
          byte[] b = (byte[]) obj;
          rtn.append(ByteUtil.toHexString(b));
          //This block can be used to easily dump a binary column to a file
          /*java.io.File f = java.io.File.createTempFile("ole", ".bin");
            java.io.FileOutputStream out = new java.io.FileOutputStream(f);
            out.write(b);
            out.flush();
            out.close();*/
        } else {
          rtn.append(String.valueOf(obj));
        }
        if (iter.hasNext()) {
          rtn.append("\t");
        }
      }
      rtn.append("\n");
    }
    return rtn.toString();
  }

  /**
   * Updates free space and row info for a new row of the given size in the
   * given data page.  Positions the page for writing the row data.
   * @return the row number of the new row
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
  private static int getRowsOnDataPage(ByteBuffer rowBuffer, JetFormat format)
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
  private static void requireValidRowId(RowId rowId) {
    if(!rowId.isValid()) {
      throw new IllegalArgumentException("Given rowId is invalid: " + rowId);
    }
  }
  
  /**
   * @throws IllegalStateException if the given row is invalid or deleted
   */
  private static void requireNonDeletedRow(RowState rowState, RowId rowId) {
    if(!rowState.isValid()) {
      throw new IllegalArgumentException(
          "Given rowId is invalid for this table: " + rowId);
    }
    if(rowState.isDeleted()) {
      throw new IllegalStateException("Row is deleted: " + rowId);
    }
  }
  
  public static boolean isDeletedRow(short rowStart) {
    return ((rowStart & DELETED_ROW_MASK) != 0);
  }
  
  public static boolean isOverflowRow(short rowStart) {
    return ((rowStart & OVERFLOW_ROW_MASK) != 0);
  }

  public static short cleanRowStart(short rowStart) {
    return (short)(rowStart & OFFSET_MASK);
  }
  
  public static short findRowStart(ByteBuffer buffer, int rowNum,
                                   JetFormat format)
  {
    return cleanRowStart(
        buffer.getShort(getRowStartOffset(rowNum, format)));
  }

  public static int getRowStartOffset(int rowNum, JetFormat format)
  {
    return format.OFFSET_ROW_START + (format.SIZE_ROW_LOCATION * rowNum);
  }
  
  public static short findRowEnd(ByteBuffer buffer, int rowNum,
                                 JetFormat format)
  {
    return (short)((rowNum == 0) ?
                   format.PAGE_SIZE :
                   cleanRowStart(
                       buffer.getShort(getRowEndOffset(rowNum, format))));
  }

  public static int getRowEndOffset(int rowNum, JetFormat format)
  {
    return format.OFFSET_ROW_START + (format.SIZE_ROW_LOCATION * (rowNum - 1));
  }

  public static int getRowSpaceUsage(int rowSize, JetFormat format)
  {
    return rowSize + format.SIZE_ROW_LOCATION;
  }

  /**
   * @return the "AutoNumber" columns in the given collection of columns.
   */
  public static List<Column> getAutoNumberColumns(Collection<Column> columns) {
    List<Column> autoCols = new ArrayList<Column>();
    for(Column c : columns) {
      if(c.isAutoNumber()) {
        autoCols.add(c);
      }
    }
    return autoCols;
  }

  /**
   * Returns {@code true} if a row of the given size will fit on the given
   * data page, {@code false} otherwise.
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
    System.arraycopy(row, 0, copy, 0, row.length);
    return copy;
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
   */
  public final class RowState
  {
    /** Buffer used for reading the header row data pages */
    private final TempPageHolder _headerRowBufferH;
    /** the header rowId */
    private RowId _headerRowId = RowId.FIRST_ROW_ID;
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
    private RowId _finalRowId = null;
    /** true if the row values array has data */
    private boolean _haveRowValues;
    /** values read from the last row */
    private final Object[] _rowValues;
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
      _rowValues = new Object[Table.this.getColumnCount()];
      _lastModCount = Table.this._modCount;
    }

    public Table getTable() {
      return Table.this;
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
      if(_haveRowValues) {
        Arrays.fill(_rowValues, null);
        _haveRowValues = false;
      }
    }

    public boolean isUpToDate() {
      return(Table.this._modCount == _lastModCount);
    }
    
    private void checkForModification() {
      if(!isUpToDate()) {
        reset();
        _headerRowBufferH.invalidate();
        _overflowRowBufferH.invalidate();
        _lastModCount = Table.this._modCount;
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

    public RowId getFinalRowId() {
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

    private Object setRowValue(int idx, Object value) {
      _haveRowValues = true;
      _rowValues[idx] = value;
      return value;
    }
    
    public Object[] getRowValues() {
      return dupeRow(_rowValues, _rowValues.length);
    }

    private short[] getVarColOffsets() {
      return _varColOffsets;
    }

    private void setVarColOffsets(short[] varColOffsets) {
      _varColOffsets = varColOffsets;
    }
    
    public RowId getHeaderRowId() {
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

    private ByteBuffer setHeaderRow(RowId rowId)
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

    private ByteBuffer setOverflowRow(RowId rowId)
      throws IOException
    {
      // this should never see modifications because it only happens within
      // the positionAtRowData method
      if(!isUpToDate()) {
        throw new IllegalStateException("Table modified while searching?");
      }
      if(_rowStatus != RowStatus.OVERFLOW) {
        throw new IllegalStateException("Row is not an overflow row?");
      }
      _finalRowId = rowId;
      _finalRowBuffer = _overflowRowBufferH.setPage(getPageChannel(),
                                                    rowId.getPageNumber());
      return _finalRowBuffer;
    }

    private Object handleRowError(Column column,
                                  byte[] columnData,
                                  Exception error)
      throws IOException
    {
      return getErrorHandler().handleRowError(column, columnData,
                                              this, error);
    }  

    @Override
    public String toString()
    {
      return "RowState: headerRowId = " + _headerRowId + ", finalRowId = " +
        _finalRowId;
    }
  }
  
}
