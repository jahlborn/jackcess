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
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A single database table
 * @author Tim McCune
 */
public class Table
  implements Iterable<Map<String, Object>>
{
  
  private static final Log LOG = LogFactory.getLog(Table.class);

  private static final int INVALID_ROW_NUMBER = -1;
  
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
  /** State used for reading the table rows */
  private RowState _rowState;
  /** Type of the table (either TYPE_SYSTEM or TYPE_USER) */
  private byte _tableType;
  /** Number of the current row in a data page */
  private int _currentRowInPage = INVALID_ROW_NUMBER;
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
  /** Number of rows left to be read on the current page */
  private short _rowsLeftOnPage = 0;
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
  /** Iterator over the pages that this table owns */
  private UsageMap.PageIterator _ownedPagesIterator;
  /** Usage map of pages that this table owns with free space on them */
  private UsageMap _freeSpacePages;
  
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
   * @param tableBuffer Buffer to read the table with
   * @param pageChannel Page channel to get database pages from
   * @param format Format of the database that contains this table
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

    _rowState = new RowState(true, _maxColumnCount);
  }

  /**
   * @return The name of the table
   */
  public String getName() {
    return _name;
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
  
  /**
   * @return All of the columns in this table (unmodifiable List)
   */
  public List<Column> getColumns() {
    return Collections.unmodifiableList(_columns);
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
    _rowsLeftOnPage = 0;
    _currentRowInPage = INVALID_ROW_NUMBER;
    _ownedPagesIterator.reset();
    _rowState.reset();
  }
  
  /**
   * Delete the current row (retrieved by a call to {@link #getNextRow}).
   */
  public void deleteCurrentRow() throws IOException {
    if (_currentRowInPage == INVALID_ROW_NUMBER) {
      throw new IllegalStateException("Must call getNextRow first");
    }

    // FIXME, want to make this static, but writeDataPage is not static, also, this may screw up other rowstates...

    // see if row was already deleted
    if(_rowState.isDeleted()) {
      throw new IllegalStateException("Deleting already deleted row");
    }
    
    // delete flag always gets set in the "root" page (even if overflow row)
    ByteBuffer rowBuffer = _rowState.getPage(getPageChannel());
    int pageNumber = _rowState.getPageNumber();
    int rowIndex = getRowStartOffset(_currentRowInPage, getFormat());
    rowBuffer.putShort(rowIndex, (short)(rowBuffer.getShort(rowIndex)
                                      | DELETED_ROW_MASK | OVERFLOW_ROW_MASK));
    writeDataPage(rowBuffer, pageNumber);
    _rowState.setDeleted(true);

    // update the indexes
    for(Index index : _indexes) {
      index.deleteRow(_rowState.getRowValues(), pageNumber,
                      (byte)_currentRowInPage);
    }
    
    // make sure table def gets updated
    --_rowCount;
    updateTableDefinition();
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
    // find next row
    ByteBuffer rowBuffer = positionAtNextRow();
    if (rowBuffer == null) {
      return null;
    }

    return getRow(_rowState, rowBuffer, getRowNullMask(rowBuffer), _columns,
                  columnNames);
  }
  
  /**
   * Reads a single column from the given row.
   */
  public static Object getRowSingleColumn(
      RowState rowState, int pageNumber, int rowNum,
      Column column, PageChannel pageChannel, JetFormat format)
    throws IOException
  {
    // set row state to correct page
    rowState.setPage(pageChannel, pageNumber);

    // position at correct row
    ByteBuffer rowBuffer = positionAtRow(rowState, rowNum, pageChannel,
                                         format);
    if(rowBuffer == null) {
      // note, row state will indicate that row was deleted
      return null;
    }
    
    return getRowColumn(rowBuffer, getRowNullMask(rowBuffer), column);
  }
 
  /**
   * Reads some columns from the given row.
   * @param columnNames Only column names in this collection will be returned
   */
  public static Map<String, Object> getRow(
      RowState rowState, int pageNumber, int rowNum,
      Collection<Column> columns, PageChannel pageChannel, JetFormat format,
      Collection<String> columnNames)
    throws IOException
  {
    // set row state to correct page
    rowState.setPage(pageChannel, pageNumber);

    // position at correct row
    ByteBuffer rowBuffer = positionAtRow(rowState, rowNum, pageChannel,
                                         format);
    if(rowBuffer == null) {
      // note, row state will indicate that row was deleted
      return null;
    }

    return getRow(rowState, rowBuffer, getRowNullMask(rowBuffer), columns,
                  columnNames);
  }

  /**
   * Reads the row data from the given row buffer.  Leaves limit unchanged.
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
      Object value = null;
      if((columnNames == null) || (columnNames.contains(column.getName()))) {
        // Add the value to the row data
        value = getRowColumn(rowBuffer, nullMask, column);
        rtn.put(column.getName(), value);
      }

      // cache the row values in order to be able to update the index on row
      // deletion.  note, most of the returned values are immutable, except
      // for binary data (returned as byte[]), but binary data shouldn't be
      // indexed anyway.
      rowState._rowValues[column.getColumnNumber()] = value;
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
    boolean isNull = nullMask.isNull(column.getColumnNumber());
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
   * Position the buffer at the next row in the table
   * @return a ByteBuffer narrowed to the next row, or null if none
   */
  private ByteBuffer positionAtNextRow() throws IOException {

    // loop until we find the next valid row or run out of pages
    while(true) {

      if (_rowsLeftOnPage == 0) {

        // reset row number
        _currentRowInPage = INVALID_ROW_NUMBER;
        
        int nextPageNumber = _ownedPagesIterator.getNextPage();
        if (nextPageNumber == PageChannel.INVALID_PAGE_NUMBER) {
          //No more owned pages.  No more rows.
          return null;
        }

        // load new page
        ByteBuffer rowBuffer = _rowState.setPage(getPageChannel(), nextPageNumber);
        if(rowBuffer.get() != PageTypes.DATA) {
          //Only interested in data pages
          continue;
        }

        _rowsLeftOnPage = rowBuffer.getShort(getFormat().OFFSET_NUM_ROWS_ON_DATA_PAGE);
        if(_rowsLeftOnPage == 0) {
          // no rows on this page?
          continue;
        }
        
      }

      // move to next row
      _currentRowInPage++;
      _rowsLeftOnPage--;

      ByteBuffer rowBuffer =
        positionAtRow(_rowState, _currentRowInPage, getPageChannel(), getFormat());
      if(rowBuffer != null) {
        // we found a non-deleted row, return it
        return rowBuffer;
      }
    }
  }

  /**
   * Sets the position and limit in a new buffer using the given rowState
   * according to the given row number and row end, following overflow row
   * pointers as necessary.
   * 
   * @return a ByteBuffer narrowed to the next row, or null if row was deleted
   */
  private static ByteBuffer positionAtRow(RowState rowState, int rowNum,
                                          PageChannel pageChannel,
                                          JetFormat format)
    throws IOException
  {
    // reset row state
    rowState.reset();
    
    while(true) {
      ByteBuffer rowBuffer = rowState.getFinalPage(pageChannel);
    
      // note, we don't use findRowStart here cause we need the unmasked value
      short rowStart = rowBuffer.getShort(getRowStartOffset(rowNum, format));
      short rowEnd = findRowEnd(rowBuffer, rowNum, format);

      // note, if we are reading from an overflow page, the row will be marked
      // as deleted on that page, so ignore the deletedRow flag on overflow
      // pages
      boolean deletedRow =
        (((rowStart & DELETED_ROW_MASK) != 0) && !rowState.isOverflow());
      boolean overflowRow = ((rowStart & OVERFLOW_ROW_MASK) != 0);

      if(deletedRow ^ overflowRow) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Row flags: deletedRow " + deletedRow + ", overflowRow " +
                    overflowRow);
        }
      }

      // now, strip flags from rowStart offset
      rowStart = (short)(rowStart & OFFSET_MASK);

      if (deletedRow) {
      
        // Deleted row.  Skip.
        if(LOG.isDebugEnabled()) {
          LOG.debug("Skipping deleted row");
        }
        rowState.setDeleted(true);
        return null;
      
      } else if (overflowRow) {

        if((rowEnd - rowStart) < 4) {
          throw new IOException("invalid overflow row info");
        }
      
        // Overflow page.  the "row" data in the current page points to another
        // page/row
        int overflowRowNum = rowBuffer.get(rowStart);
        int overflowPageNum = ByteUtil.get3ByteInt(rowBuffer, rowStart + 1);
        rowState.setOverflowPage(pageChannel, overflowPageNum);

        // reset row number and move to overflow page
        rowNum = overflowRowNum;
      
      } else {

        return PageChannel.narrowBuffer(rowBuffer, rowStart, rowEnd);
      }
    }    
  }

  
  /**
   * Calls <code>reset</code> on this table and returns a modifiable Iterator
   * which will iterate through all the rows of this table.  Use of the
   * Iterator follows the same restrictions as a call to
   * <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String, Object>> iterator()
  {
    return iterator(null);
  }
  
  /**
   * Calls <code>reset</code> on this table and returns a modifiable Iterator
   * which will iterate through all the rows of this table, returning only the
   * given columns.  Use of the Iterator follows the same restrictions as a
   * call to <code>getNextRow</code>.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String, Object>> iterator(Collection<String> columnNames)
  {
    return new RowIterator(columnNames);
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
    for (int i = 0; i < 16; i++) {  //Unknown
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
      (short) Column.countNonLongVariableLength(columns);
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
        buffer.put((byte) col.getPrecision());  // numeric precision
        buffer.put((byte) col.getScale());  // numeric scale
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
      ByteBuffer colName = format.CHARSET.encode(col.getName());
      buffer.putShort((short) colName.remaining());
      buffer.put(colName);
    }
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
        rtn.put(rowStart, (byte)UsageMap.MAP_TYPE_REFERENCE);
      } else {
        // initial "pages with free space" map definition
        rtn.put(rowStart, (byte)UsageMap.MAP_TYPE_INLINE);
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
    
    byte rowNum = tableBuffer.get(getFormat().OFFSET_OWNED_PAGES);
    int pageNum = ByteUtil.get3ByteInt(tableBuffer, getFormat().OFFSET_OWNED_PAGES + 1);
    _ownedPages = UsageMap.read(getDatabase(), pageNum, rowNum, false);
    _ownedPagesIterator = _ownedPages.iterator();
    rowNum = tableBuffer.get(getFormat().OFFSET_FREE_SPACE_PAGES);
    pageNum = ByteUtil.get3ByteInt(tableBuffer, getFormat().OFFSET_FREE_SPACE_PAGES + 1);
    _freeSpacePages = UsageMap.read(getDatabase(), pageNum, rowNum, false);
    
    for (int i = 0; i < _indexCount; i++) {
      Index index = new Index(this);
      _indexes.add(index);
      index.setRowCount(tableBuffer.getInt(getFormat().OFFSET_INDEX_DEF_BLOCK +
          i * getFormat().SIZE_INDEX_DEFINITION + 4));
    }
    
    int offset = getFormat().OFFSET_INDEX_DEF_BLOCK +
        _indexCount * getFormat().SIZE_INDEX_DEFINITION;
    Column column;
    for (int i = 0; i < columnCount; i++) {
      column = new Column(this, tableBuffer,
          offset + i * getFormat().SIZE_COLUMN_HEADER);
      _columns.add(column);
      if(column.isVariableLength()) {
        // also shove it in the variable columns list, which is ordered
        // differently from the _columns list
        _varColumns.add(column);
      }
    }
    offset += columnCount * getFormat().SIZE_COLUMN_HEADER;
    for (int i = 0; i < columnCount; i++) {
      column = (Column) _columns.get(i);
      short nameLength = tableBuffer.getShort(offset);
      offset += 2;
      byte[] nameBytes = new byte[nameLength];
      tableBuffer.position(offset);
      tableBuffer.get(nameBytes, 0, (int) nameLength);
      column.setName(getFormat().CHARSET.decode(ByteBuffer.wrap(nameBytes)).toString());
      offset += nameLength;
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

    // there are _indexSlotCount blocks here, we ignore any slot with an index
    // number greater than the number of actual indexes
    int curIndex = 0;
    for (int i = 0; i < _indexSlotCount; i++) {
      
      tableBuffer.getInt(); //Forward past Unknown
      int indexNumber = tableBuffer.getInt();
      tableBuffer.position(tableBuffer.position() + 15);
      byte indexType = tableBuffer.get();
      tableBuffer.position(tableBuffer.position() + 4);

      if(indexNumber < _indexCount) {
        Index index = _indexes.get(curIndex++);
        index.setIndexNumber(indexNumber);
        index.setPrimaryKey(indexType == 1);
      }
    }

    // for each empty index slot, there is some weird sort of name
    for(int i = 0; i < (_indexSlotCount - _indexCount); ++i) {
      int skipBytes = tableBuffer.getShort();
      tableBuffer.position(tableBuffer.position() + skipBytes);
    }

    // read actual index names
    // FIXME, we still are not always getting the names matched correctly with
    // the index info, some weird indexing we are not figuring out yet
    for (int i = 0; i < _indexCount; i++) {
      byte[] nameBytes = new byte[tableBuffer.getShort()];
      tableBuffer.get(nameBytes);
      _indexes.get(i).setName(getFormat().CHARSET.decode(ByteBuffer.wrap(
          nameBytes)).toString());
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

    // if the overflow buffer is this page, invalidate it
    _rowState.possiblyInvalidate(pageNumber, pageBuffer);
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
    for(UsageMap.PageIterator revPageIter = _ownedPages.reverseIterator();
        revPageIter.hasNextPage(); )
    {
      int tmpPageNumber = revPageIter.getNextPage();
      getPageChannel().readPage(dataPage, tmpPageNumber);
      if(dataPage.get() == PageTypes.DATA) {
        // found last data page
        pageNumber = tmpPageNumber;
        break;
      }
    }

    if(pageNumber == PageChannel.INVALID_PAGE_NUMBER) {
      //No data pages exist.  Create a new one.
      pageNumber = newDataPage(dataPage);
    }
    
    for (int i = 0; i < rowData.length; i++) {
      rowSize = rowData[i].remaining();
      int rowSpaceUsage = getRowSpaceUsage(rowSize, getFormat());
      short freeSpaceInPage = dataPage.getShort(getFormat().OFFSET_FREE_SPACE);
      if (freeSpaceInPage < rowSpaceUsage) {

        //Last data page is full.  Create a new one.
        writeDataPage(dataPage, pageNumber);
        dataPage.clear();
        _freeSpacePages.removePageNumber(pageNumber);

        pageNumber = newDataPage(dataPage);
        freeSpaceInPage = dataPage.getShort(getFormat().OFFSET_FREE_SPACE);
      }

      // write out the row data
      int rowNum = addDataPageRow(dataPage, rowSize, getFormat());
      dataPage.put(rowData[i]);

      // update the indexes
      for(Index index : _indexes) {
        index.addRow(rows.get(i), pageNumber, (byte)rowNum);
      }
    }
    writeDataPage(dataPage, pageNumber);
    
    //Update tdef page
    _rowCount += rows.size();
    updateTableDefinition();
  }

  /**
   * Updates the table definition after rows are modified.
   */
  private void updateTableDefinition() throws IOException
  {
    // load table definition
    ByteBuffer tdefPage = getPageChannel().createPageBuffer();
    getPageChannel().readPage(tdefPage, _tableDefPageNumber);
    
    // make sure rowcount and autonumber are up-to-date
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
    buffer.putShort((short) _maxColumnCount);
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
            nullMask.markNotNull(col.getColumnNumber());
          }
        
        } else {

          if(col.isAutoNumber()) {
            // ignore given row value, use next autonumber
            rowValue = getNextAutoNumber();
          }
          
          if(rowValue != null) {
        
            // we have a value
            nullMask.markNotNull(col.getColumnNumber());

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
          nullMask.markNotNull(varCol.getColumnNumber());

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
      buffer.putShort((short) _maxVarColumnCount);  //Number of var length columns
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
          rtn.append(ByteUtil.toHexString(ByteBuffer.wrap(b), b.length));
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
  
  public static short findRowStart(ByteBuffer buffer, int rowNum,
                                   JetFormat format)
  {
    return (short)(buffer.getShort(getRowStartOffset(rowNum, format))
                   & OFFSET_MASK);
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
                   (buffer.getShort(getRowEndOffset(rowNum, format))
                    & OFFSET_MASK));
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
   * Row iterator for this table, supports modification.
   */
  private final class RowIterator implements Iterator<Map<String, Object>>
  {
    private Collection<String> _columnNames;
    private Map<String, Object> _next;
    
    private RowIterator(Collection<String> columnNames)
    {
      try {
        reset();
        _columnNames = columnNames;
        _next = getNextRow(_columnNames);
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public boolean hasNext() { return _next != null; }

    public void remove() {
      try {
        deleteCurrentRow();
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }        
    }
    
    public Map<String, Object> next() {
      if(!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        Map<String, Object> rtn = _next;
        _next = getNextRow(_columnNames);
        return rtn;
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }
    }
    
  }

  /**
   * Maintains the state of reading a row of data.
   */
  public static class RowState
  {
    /** Buffer used for reading the row data pages */
    private TempPageHolder _rowBufferH;
    /** true if the current row is an overflow row */
    public boolean _overflow;
    /** true if the current row is a deleted row */
    public boolean _deleted;
    /** buffer used for reading overflow pages */
    public TempPageHolder _overflowRowBufferH =
      TempPageHolder.newHolder(false);
    /** the row buffer which contains the final data (after following any
        overflow pointers) */
    public ByteBuffer _finalRowBuffer;
    /** values read from the last row */
    public Object[] _rowValues;
    
    public RowState(boolean hardRowBuffer, int colCount) {
      _rowBufferH = TempPageHolder.newHolder(hardRowBuffer);
      _rowValues = new Object[colCount];
    }
    
    public void reset() {
      _finalRowBuffer = null;
      _deleted = false;
      _overflow = false;
      Arrays.fill(_rowValues, null);
    }

    public int getPageNumber() {
      return _rowBufferH.getPageNumber();
    }

    public ByteBuffer getFinalPage(PageChannel pageChannel)
      throws IOException
    {
      if(_finalRowBuffer == null) {
        // (re)load current page
        _finalRowBuffer = getPage(pageChannel);
      }
      return _finalRowBuffer;
    }

    public void setDeleted(boolean deleted) {
      _deleted = deleted;
    }

    public boolean isDeleted() {
      return _deleted;
    }
    
    public boolean isOverflow() {
      return _overflow;
    }

    public Object[] getRowValues() {
      return _rowValues;
    }
    
    public void possiblyInvalidate(int modifiedPageNumber,
                                   ByteBuffer modifiedBuffer) {
      _rowBufferH.possiblyInvalidate(modifiedPageNumber,
                                     modifiedBuffer);
      _overflowRowBufferH.possiblyInvalidate(modifiedPageNumber,
                                             modifiedBuffer);
    }
    
    public ByteBuffer getPage(PageChannel pageChannel)
      throws IOException
    {
      return _rowBufferH.getPage(pageChannel);
    }

    public ByteBuffer setPage(PageChannel pageChannel, int pageNumber)
      throws IOException
    {
      reset();
      _finalRowBuffer = _rowBufferH.setPage(pageChannel, pageNumber);
      return _finalRowBuffer;
    }

    public ByteBuffer setOverflowPage(PageChannel pageChannel, int pageNumber)
      throws IOException
    {
      _overflow = true;
      _finalRowBuffer = _overflowRowBufferH.setPage(pageChannel, pageNumber);
      return _finalRowBuffer;
    }

  }
  
}
