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
  /** last auto number for the table */
  private int _lastAutoNumber;
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
    setColumns(columns);
  }
  
  /**
   * @param database database which owns this table
   * @param tableBuffer Buffer to read the table with
   * @param pageNumber Page number of the table definition
   * @param name Table name
   */
  protected Table(Database database, ByteBuffer tableBuffer,
                  int pageNumber, String name)
  throws IOException
  {
    _database = database;
    _tableDefPageNumber = pageNumber;
    _name = name;
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

  protected int getTableDefPageNumber() {
    return _tableDefPageNumber;
  }

  public RowState createRowState() {
    return new RowState(true);
  }

  protected UsageMap.PageCursor getOwnedPagesCursor() {
    return _ownedPages.cursor();
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
      if(column.getName().equals(name)) {
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
      if(index.getName().equals(name)) {
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
    
    Object value = getRowColumn(rowBuffer, getRowNullMask(rowBuffer), column);

    // cache the row values in order to be able to update the index on row
    // deletion.  note, most of the returned values are immutable, except
    // for binary data (returned as byte[]), but binary data shouldn't be
    // indexed anyway.
    rowState.setRowValue(column.getColumnIndex(), value);

    return value;
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

    return getRow(rowState, rowBuffer, getRowNullMask(rowBuffer), _columns,
                  columnNames);
  }

  /**
   * Reads the row data from the given row buffer.  Leaves limit unchanged.
   * Saves parsed row values to the given rowState.
   */
  private static Map<String, Object> getRow(
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
        Object value = getRowColumn(rowBuffer, nullMask, column);
        rtn.put(column.getName(), value);

        // cache the row values in order to be able to update the index on row
        // deletion.  note, most of the returned values are immutable, except
        // for binary data (returned as byte[]), but binary data shouldn't be
        // indexed anyway.
        rowState.setRowValue(column.getColumnIndex(), value);
      }
    }
    return rtn;
  }
  
  /**
   * Reads the column data from the given row buffer.  Leaves limit unchanged.
   */
  private static Object getRowColumn(ByteBuffer rowBuffer,
                                     NullMask nullMask,
                                     Column column)
    throws IOException
  {
    boolean isNull = nullMask.isNull(column);
    if(column.getType() == DataType.BOOLEAN) {
      return Boolean.valueOf(!isNull);  //Boolean values are stored in the null mask
    } else if(isNull) {
      // well, that's easy!
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
      int dataStart = rowStart + 2;
      colDataPos = dataStart + column.getFixedDataOffset();
      colDataLen = column.getType().getFixedSize();
      
    } else {

      // read var length value
      int varColumnOffsetPos =
        (rowBuffer.limit() - nullMask.byteSize() - 4) -
        (column.getVarLenTableIndex() * 2);

      short varDataStart = rowBuffer.getShort(varColumnOffsetPos);
      short varDataEnd = rowBuffer.getShort(varColumnOffsetPos - 2);
      colDataPos = rowStart + varDataStart;
      colDataLen = varDataEnd - varDataStart;
    }

    // grab the column data
    byte[] columnData = new byte[colDataLen];
    rowBuffer.position(colDataPos);
    rowBuffer.get(columnData);

    // parse the column data
    return column.read(columnData);
  }

  /**
   * Reads the null mask from the given row buffer.  Leaves limit unchanged.
   */
  private static NullMask getRowNullMask(ByteBuffer rowBuffer)
    throws IOException
  {
    // reset position to row start
    rowBuffer.reset();
    
    short columnCount = rowBuffer.getShort(); // Number of columns in this row
    
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
      List<Column> columns, PageChannel pageChannel, JetFormat format)
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
    writeColumnDefinitions(buffer, columns, format); 
    
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
        buffer.position(buffer.position() + writeLen);

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
    if(countAutoNumberColumns(columns) > 0) {
      buffer.put((byte) 1);
    } else {
      buffer.put((byte) 0);
    }
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
      ByteBuffer buffer, List<Column> columns, JetFormat format)
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
        fixedOffset += col.getType().getFixedSize();
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
      writeName(buffer, col.getName(), format);
    }
  }

  /**
   * Writes the given name into the given buffer in the format as expected by
   * {@link #readName}.
   */
  private static void writeName(ByteBuffer buffer, String name,
                                JetFormat format)
  {
      ByteBuffer encName = Column.encodeUncompressedText(
          name, format);
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
      flags |= Column.AUTO_NUMBER_FLAG_MASK;
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
    // USAGE_MAP_DEF_FREE_SPACE = 3940;
    int usageMapRowLength = format.OFFSET_USAGE_MAP_START +
      format.USAGE_MAP_TABLE_BYTE_LENGTH;
    int freeSpace = getRowSpaceUsage(format.MAX_ROW_SIZE, format)
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
    _lastAutoNumber = tableBuffer.getInt(getFormat().OFFSET_NEXT_AUTO_NUMBER);
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
      Index index = new Index(this);
      _indexes.add(index);
      index.setRowCount(tableBuffer.getInt(getFormat().OFFSET_INDEX_DEF_BLOCK +
          i * getFormat().SIZE_INDEX_DEFINITION + 4));
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

      tableBuffer.getInt(); //Forward past Unknown
      tableBuffer.getInt(); //Forward past alternate index number
      int indexNumber = tableBuffer.getInt();
      tableBuffer.position(tableBuffer.position() + 11);
      byte indexType = tableBuffer.get();
      tableBuffer.position(tableBuffer.position() + 4);

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
      tableBuffer.getInt(); //Forward past Unknown
      _indexes.get(i).read(tableBuffer, _columns);
    }

    // reset to end of index info
    tableBuffer.position(idxEndOffset);
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

    // update modification count so any active RowStates can keep themselves
    // up-to-date
    ++_modCount;
  }

  /**
   * Returns a name read from the buffer at the current position.  The
   * expected name format is the name length as a short followed by (length *
   * 2) bytes encoded using the {@link JetFormat#CHARSET}
   */
  private String readName(ByteBuffer buffer) {
    short nameLength = buffer.getShort();
    byte[] nameBytes = new byte[nameLength];
    buffer.get(nameBytes);
    return Column.decodeUncompressedText(nameBytes, getFormat());
  }
  
  /**
   * Skips past a name int the buffer at the current position.  The
   * expected name format is the same as that for {@link #readName}.
   */
  private void skipName(ByteBuffer buffer) {
    short nameLength = buffer.getShort();
    buffer.position(buffer.position() + nameLength);
  }
  
  /**
   * Converts a map of columnName -> columnValue to an array of row values
   * appropriate for a call to {@link #addRow(Object...)}.
   */
  public Object[] asRow(Map<String,Object> rowMap) {
    Object[] row = new Object[_columns.size()];
    if(rowMap == null) {
      return row;
    }
    for(Column col : _columns) {
      row[col.getColumnIndex()] = rowMap.get(col.getName());
    }
    return row;
  }
  
  /**
   * Add a single row to this table and write it to disk
   */
  public void addRow(Object... row) throws IOException {
    addRows(Collections.singletonList(row));
  }
  
  /**
   * Add multiple rows to this table, only writing to disk after all
   * rows have been written, and every time a data page is filled.  This
   * is much more efficient than calling <code>addRow</code> multiple times.
   * @param rows List of Object[] row values
   */
  public void addRows(List<? extends Object[]> rows) throws IOException {
    ByteBuffer dataPage = getPageChannel().createPageBuffer();
    ByteBuffer[] rowData = new ByteBuffer[rows.size()];
    Iterator<? extends Object[]> iter = rows.iterator();
    for (int i = 0; iter.hasNext(); i++) {
      rowData[i] = createRow(iter.next(), getFormat().MAX_ROW_SIZE);
      if (rowData[i].limit() > getFormat().MAX_ROW_SIZE) {
        throw new IOException("Row size " + rowData[i].limit() +
                              " is too large");
      }
    }
    
    int pageNumber = PageChannel.INVALID_PAGE_NUMBER;
    int rowSize;

    // find last data page (Not bothering to check other pages for free
    // space.)
    UsageMap.PageCursor revPageCursor = _ownedPages.cursor();
    revPageCursor.afterLast();
    while(true) {
      int tmpPageNumber = revPageCursor.getPreviousPage();
      if(tmpPageNumber < 0) {
        break;
      }
      getPageChannel().readPage(dataPage, tmpPageNumber);
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
      pageNumber = newDataPage(dataPage);
    }
    
    for (int i = 0; i < rowData.length; i++) {
      rowSize = rowData[i].remaining();
      int rowSpaceUsage = getRowSpaceUsage(rowSize, getFormat());
      short freeSpaceInPage = dataPage.getShort(getFormat().OFFSET_FREE_SPACE);
      if (freeSpaceInPage < rowSpaceUsage) {

        // Last data page is full.  Create a new one.
        writeDataPage(dataPage, pageNumber);
        _freeSpacePages.removePageNumber(pageNumber);

        dataPage.clear();
        pageNumber = newDataPage(dataPage);
        
        freeSpaceInPage = dataPage.getShort(getFormat().OFFSET_FREE_SPACE);
      }

      // write out the row data
      int rowNum = addDataPageRow(dataPage, rowSize, getFormat());
      dataPage.put(rowData[i]);

      // update the indexes
      for(Index index : _indexes) {
        index.addRow(rows.get(i), new RowId(pageNumber, rowNum));
      }
    }
    writeDataPage(dataPage, pageNumber);
    
    // Update tdef page
    updateTableDefinition(rows.size());
  }

  /**
   * Updates the table definition after rows are modified.
   */
  private void updateTableDefinition(int rowCountInc) throws IOException
  {
    // load table definition
    ByteBuffer tdefPage = getPageChannel().createPageBuffer();
    getPageChannel().readPage(tdefPage, _tableDefPageNumber);
    
    // make sure rowcount and autonumber are up-to-date
    _rowCount += rowCountInc;
    tdefPage.putInt(getFormat().OFFSET_NUM_ROWS, _rowCount);
    tdefPage.putInt(getFormat().OFFSET_NEXT_AUTO_NUMBER, _lastAutoNumber);

    // write any index changes
    Iterator<Index> indIter = _indexes.iterator();
    for (int i = 0; i < _indexes.size(); i++) {
      tdefPage.putInt(getFormat().OFFSET_INDEX_DEF_BLOCK +
          (i * getFormat().SIZE_INDEX_DEFINITION) + 4, _rowCount);
      Index index = indIter.next();
      index.update();
    }

    // write modified table definition
    getPageChannel().writePage(tdefPage, _tableDefPageNumber);
  }
  
  /**
   * Create a new data page
   * @return Page number of the new page
   */
  private int newDataPage(ByteBuffer dataPage) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating new data page");
    }
    dataPage.put(PageTypes.DATA); //Page type
    dataPage.put((byte) 1); //Unknown
    dataPage.putShort((short)getRowSpaceUsage(getFormat().MAX_ROW_SIZE,
                                              getFormat())); //Free space in this page
    dataPage.putInt(_tableDefPageNumber); //Page pointer to table definition
    dataPage.putInt(0); //Unknown
    dataPage.putInt(0); //Number of records on this page
    int pageNumber = getPageChannel().writeNewPage(dataPage);
    _ownedPages.addPageNumber(pageNumber);
    _freeSpacePages.addPageNumber(pageNumber);
    return pageNumber;
  }
  
  /**
   * Serialize a row of Objects into a byte buffer
   */
  ByteBuffer createRow(Object[] rowArray, int maxRowSize) throws IOException {
    ByteBuffer buffer = getPageChannel().createPageBuffer();
    buffer.putShort(_maxColumnCount);
    NullMask nullMask = new NullMask(_maxColumnCount);
    
    List<Object> row = new ArrayList<Object>(_columns.size());
    for(Object rowValue : rowArray) {
      row.add(rowValue);
    }
    //Append null for arrays that are too small
    for (int i = rowArray.length; i < _columns.size(); i++) {
      row.add(null);
    }

    //Fixed length column data comes first
    int fixedDataStart = buffer.position();
    int fixedDataEnd = fixedDataStart;
    for (Column col : _columns) {

      if(!col.isVariableLength()) {
        
        Object rowValue = row.get(col.getColumnIndex());

        if (col.getType() == DataType.BOOLEAN) {
        
          if(Column.toBooleanValue(rowValue)) {
            //Booleans are stored in the null mask
            nullMask.markNotNull(col);
          }
        
        } else {

          if(col.isAutoNumber()) {
            // ignore given row value, use next autonumber
            rowValue = getNextAutoNumber();
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
      maxRowSize -= (nullMask.byteSize() + 4 + (_maxVarColumnCount * 2));
    
      //Now write out variable length column data
      short[] varColumnOffsets = new short[_maxVarColumnCount];
      int varColumnOffsetsIndex = 0;
      for (Column varCol : _varColumns) {
        short offset = (short) buffer.position();
        Object rowValue = row.get(varCol.getColumnIndex());
        if (rowValue != null) {
          // we have a value
          nullMask.markNotNull(varCol);

          ByteBuffer varDataBuf = varCol.write(rowValue, maxRowSize);
          maxRowSize -= varDataBuf.remaining();
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

      buffer.putShort((short) buffer.position()); //EOD marker
      //Now write out variable length offsets
      //Offsets are stored in reverse order
      for (int i = _maxVarColumnCount - 1; i >= 0; i--) {
        buffer.putShort(varColumnOffsets[i]);
      }
      buffer.putShort(_maxVarColumnCount);  //Number of var length columns
    }
    
    buffer.put(nullMask.wrap());  //Null mask
    buffer.limit(buffer.position());
    buffer.flip();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating new data block:\n" + ByteUtil.toHexString(buffer, buffer.limit()));
    }
    return buffer;
  }

  public int getRowCount() {
    return _rowCount;
  }

  private int getNextAutoNumber() {
    // note, the saved value is the last one handed out, so pre-increment
    return ++_lastAutoNumber;
  }

  int getLastAutoNumber() {
    // gets the last used auto number (does not modify)
    return _lastAutoNumber;
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
                                   JetFormat format)
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
    dataPage.putShort(getRowStartOffset(rowCount, format), rowLocation);

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
   * @return the number of "AutoNumber" columns in the given collection of
   *         columns.
   */
  public static int countAutoNumberColumns(Collection<Column> columns) {
    int numAutoNumCols = 0;
    for(Column c : columns) {
      if(c.isAutoNumber()) {
        ++numAutoNumCols;
      }
    }
    return numAutoNumCols;
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
      TempPageHolder.newHolder(false);
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
    
    private RowState(boolean hardRowBuffer) {
      _headerRowBufferH = TempPageHolder.newHolder(hardRowBuffer);
      _rowValues = new Object[Table.this.getColumnCount()];
      _lastModCount = Table.this._modCount;
    }

    public Table getTable() {
      return Table.this;
    }
    
    public void reset() {
      _finalRowId = null;
      _finalRowBuffer = null;
      _rowsOnHeaderPage = 0;
      _status = RowStateStatus.INIT;
      _rowStatus = RowStatus.INIT;
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

    private void setRowValue(int idx, Object value) {
      _haveRowValues = true;
      _rowValues[idx] = value;
    }
    
    public Object[] getRowValues() {
      Object[] copy = new Object[_rowValues.length];
      System.arraycopy(_rowValues, 0, copy, 0, _rowValues.length);
      return copy;
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

  }
  
}
