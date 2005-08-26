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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A single database table
 * @author Tim McCune
 */
public class Table {
  
  private static final Log LOG = LogFactory.getLog(Table.class);
  
  /** Table type code for system tables */
  public static final byte TYPE_SYSTEM = 0x53;
  /** Table type code for user tables */
  public static final byte TYPE_USER = 0x4e;
  
  /** Buffer used for reading the table */
  private ByteBuffer _buffer;
  /** Type of the table (either TYPE_SYSTEM or TYPE_USER) */
  private byte _tableType;
  /** Number of the current row in a data page */
  private int _currentRowInPage;
  /** Number of indexes on the table */
  private int _indexCount;
  /** Offset index in the buffer where the last row read started */
  private short _lastRowStart;
  /** Number of rows in the table */
  private int _rowCount;
  private int _tableDefPageNumber;
  /** Number of rows left to be read on the current page */
  private short _rowsLeftOnPage = 0;
  /** Offset index in the buffer of the start of the current row */
  private short _rowStart;
  /** Number of columns in the table */
  private short _columnCount;
  /** Format of the database that contains this table */
  private JetFormat _format;
  /** List of columns in this table */
  private List<Column> _columns = new ArrayList<Column>();
  /** List of indexes on this table */
  private List<Index> _indexes = new ArrayList<Index>();
  /** Used to read in pages */
  private PageChannel _pageChannel;
  /** Usage map of pages that this table owns */
  private UsageMap _ownedPages;
  /** Usage map of pages that this table owns with free space on them */
  private UsageMap _freeSpacePages;
  
  /**
   * Only used by unit tests
   */
  Table() throws IOException {
    _pageChannel = new PageChannel(null, JetFormat.VERSION_4);
  }
  
  /**
   * @param buffer Buffer to read the table with
   * @param pageChannel Page channel to get database pages from
   * @param format Format of the database that contains this table
   * @param pageNumber Page number of the table definition
   */
  protected Table(ByteBuffer buffer, PageChannel pageChannel, JetFormat format, int pageNumber)
  throws IOException, SQLException
  {
    _buffer = buffer;
    _pageChannel = pageChannel;
    _format = format;
    _tableDefPageNumber = pageNumber;
    int nextPage;
    do {
      readPage();
      nextPage = _buffer.getInt(_format.OFFSET_NEXT_TABLE_DEF_PAGE);
    } while (nextPage > 0);
  }
  
  /**
   * @return All of the columns in this table (unmodifiable List)
   */
  public List getColumns() {
    return Collections.unmodifiableList(_columns);
  }
  /**
   * Only called by unit tests
   */
  void setColumns(List<Column> columns) {
    _columns = columns;
  }
  
  /**
   * @return All of the Indexes on this table (unmodifiable List)
   */
  public List getIndexes() {
    return Collections.unmodifiableList(_indexes);
  }
  
  /**
   * After calling this method, getNextRow will return the first row in the table
   */
  public void reset() {
    _rowsLeftOnPage = 0;
    _ownedPages.reset();
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
    if (!positionAtNextRow()) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Data block at position " + Integer.toHexString(_buffer.position()) +
          ":\n" + ByteUtil.toHexString(_buffer, _buffer.position(),
          _buffer.limit() - _buffer.position()));
    }
    short columnCount = _buffer.getShort(); //Number of columns in this table
    Map<String, Object> rtn = new LinkedHashMap<String, Object>(columnCount);
    NullMask nullMask = new NullMask(columnCount);
    _buffer.position(_buffer.limit() - nullMask.byteSize());  //Null mask at end
    nullMask.read(_buffer);
    _buffer.position(_buffer.limit() - nullMask.byteSize() - 2);
    short varColumnCount = _buffer.getShort();  //Number of variable length columns
    byte[][] varColumnData = new byte[varColumnCount][];  //Holds variable length column data
    
    //Read in the offsets of each of the variable length columns
    short[] varColumnOffsets = new short[varColumnCount];
    _buffer.position(_buffer.position() - 2 - (varColumnCount * 2) - 2);
    short lastVarColumnStart = _buffer.getShort();
    for (short i = 0; i < varColumnCount; i++) {
      varColumnOffsets[i] = _buffer.getShort();
    }
    
    //Read in the actual data for each of the variable length columns
    for (short i = 0; i < varColumnCount; i++) {
      _buffer.position(_rowStart + varColumnOffsets[i]);
      varColumnData[i] = new byte[lastVarColumnStart - varColumnOffsets[i]];
      _buffer.get(varColumnData[i]);
      lastVarColumnStart = varColumnOffsets[i];
    }
    int columnNumber = 0;
    int varColumnDataIndex = varColumnCount - 1;
    
    _buffer.position(_rowStart + 2);  //Move back to the front of the buffer
    
    //Now read in the fixed length columns and populate the columnData array
    //with the combination of fixed length and variable length data.
    byte[] columnData = null;
    for (Iterator iter = _columns.iterator(); iter.hasNext(); columnNumber++) {
      Column column = (Column) iter.next();
      boolean isNull = nullMask.isNull(columnNumber);
      Object value = null;
      if (column.getType() == DataType.BOOLEAN) {
        value = new Boolean(!isNull);  //Boolean values are stored in the null mask
      } else {
        if (!column.isVariableLength()) {
          //Read in fixed length column data
          columnData = new byte[column.size()];
          _buffer.get(columnData);
        } else if (!isNull) {
          //Refer to already-read-in variable length data
          columnData = varColumnData[varColumnDataIndex--];
        }
        if (!isNull && columnData != null &&
            (columnNames == null || columnNames.contains(column.getName())))
        {
          //Add the value if we are interested in it.
          value = column.read(columnData);
        }
      }
      rtn.put(column.getName(), value);
    }
    return rtn;
  }
  
  /**
   * Position the buffer at the next row in the table
   * @return True if another row was found, false if there are no more rows
   */
  private boolean positionAtNextRow() throws IOException {
    if (_rowsLeftOnPage == 0) {
      do {
        if (!_ownedPages.getNextPage(_buffer)) {
          //No more owned pages.  No more rows.
          return false;
        }
      } while (_buffer.get() != PageTypes.DATA);  //Only interested in data pages
      _rowsLeftOnPage = _buffer.getShort(_format.OFFSET_NUM_ROWS_ON_DATA_PAGE);
      _currentRowInPage = 0;
      _lastRowStart = (short) _format.PAGE_SIZE;
    }
    _rowStart = _buffer.getShort(_format.OFFSET_DATA_ROW_LOCATION_BLOCK +
        _currentRowInPage * _format.SIZE_ROW_LOCATION);
    _currentRowInPage++;
    _rowsLeftOnPage--;
    if (_rowStart < 0) {
      // Deleted row.  Skip.
      return positionAtNextRow();
    } else if ((_rowStart & 0x4000) > 0) {
      // Overflow page.
      // FIXME - Currently skipping this.  Need to figure out how to read it.
      _buffer.position(_rowStart - 0x4000);
      int overflow = _buffer.getInt();
      _lastRowStart -= 4;
      return positionAtNextRow();
    } else {
      _buffer.position(_rowStart);
      _buffer.limit(_lastRowStart);
      _lastRowStart = _rowStart;
      return true;
    }
  }

  /**
   * Read the table definition
   */
  private void readPage() throws IOException, SQLException {
    if (LOG.isDebugEnabled()) {
      _buffer.rewind();
      LOG.debug("Table def block:\n" + ByteUtil.toHexString(_buffer,
          _format.SIZE_TDEF_BLOCK));
    }
    _rowCount = _buffer.getInt(_format.OFFSET_NUM_ROWS);
    _tableType = _buffer.get(_format.OFFSET_TABLE_TYPE);
    _columnCount = _buffer.getShort(_format.OFFSET_NUM_COLS);
    _indexCount = _buffer.getInt(_format.OFFSET_NUM_INDEXES);
    
    byte rowNum = _buffer.get(_format.OFFSET_OWNED_PAGES);
    int pageNum = ByteUtil.get3ByteInt(_buffer, _format.OFFSET_OWNED_PAGES + 1);
    _ownedPages = UsageMap.read(_pageChannel, pageNum, rowNum, _format);
    rowNum = _buffer.get(_format.OFFSET_FREE_SPACE_PAGES);
    pageNum = ByteUtil.get3ByteInt(_buffer, _format.OFFSET_FREE_SPACE_PAGES + 1);
    _freeSpacePages = UsageMap.read(_pageChannel, pageNum, rowNum, _format);
    
    for (int i = 0; i < _indexCount; i++) {
      Index index = new Index(_tableDefPageNumber, _pageChannel, _format);
      _indexes.add(index);
      index.setRowCount(_buffer.getInt(_format.OFFSET_INDEX_DEF_BLOCK +
          i * _format.SIZE_INDEX_DEFINITION + 4));
    }
    
    int offset = _format.OFFSET_INDEX_DEF_BLOCK +
        _indexCount * _format.SIZE_INDEX_DEFINITION;
    Column column;
    for (int i = 0; i < _columnCount; i++) {
      column = new Column(_buffer,
          offset + i * _format.SIZE_COLUMN_HEADER, _pageChannel, _format);
      _columns.add(column);
    }
    offset += _columnCount * _format.SIZE_COLUMN_HEADER;
    for (int i = 0; i < _columnCount; i++) {
      column = (Column) _columns.get(i);
      short nameLength = _buffer.getShort(offset);
      offset += 2;
      byte[] nameBytes = new byte[nameLength];
      _buffer.position(offset);
      _buffer.get(nameBytes, 0, (int) nameLength);
      column.setName(_format.CHARSET.decode(ByteBuffer.wrap(nameBytes)).toString());
      offset += nameLength;
    }
    Collections.sort(_columns);
    
    for (int i = 0; i < _indexCount; i++) {
      _buffer.getInt(); //Forward past Unknown
      ((Index) _indexes.get(i)).read(_buffer, _columns);
    }
    for (int i = 0; i < _indexCount; i++) {
      _buffer.getInt(); //Forward past Unknown
      ((Index) _indexes.get(i)).setIndexNumber(_buffer.getInt());
      _buffer.position(_buffer.position() + 20);
    }
    Collections.sort(_indexes);
    for (int i = 0; i < _indexCount; i++) {
      byte[] nameBytes = new byte[_buffer.getShort()];
      _buffer.get(nameBytes);
      ((Index) _indexes.get(i)).setName(_format.CHARSET.decode(ByteBuffer.wrap(
          nameBytes)).toString());
    }
    
  }
  
  /**
   * Add a single row to this table and write it to disk
   */
  public void addRow(Object[] row) throws IOException {
    addRows(Collections.singletonList(row));
  }
  
  /**
   * Add multiple rows to this table, only writing to disk after all
   * rows have been written, and every time a data page is filled.  This
   * is much more efficient than calling <code>addRow</code> multiple times.
   * @param rows List of Object[] row values
   */
  public void addRows(List rows) throws IOException {
    ByteBuffer dataPage = _pageChannel.createPageBuffer();
    ByteBuffer[] rowData = new ByteBuffer[rows.size()];
    Iterator iter = rows.iterator();
    for (int i = 0; iter.hasNext(); i++) {
      rowData[i] = createRow((Object[]) iter.next());
    }
    List pageNumbers = _ownedPages.getPageNumbers();
    int pageNumber;
    int rowSize;
    if (pageNumbers.size() == 0) {
      //No data pages exist.  Create a new one.
      pageNumber = newDataPage(dataPage, rowData[0]);
    } else {
      //Get the last data page.
      //Not bothering to check other pages for free space.
      pageNumber = ((Integer) pageNumbers.get(pageNumbers.size() - 1)).intValue();
      _pageChannel.readPage(dataPage, pageNumber);
    }
    for (int i = 0; i < rowData.length; i++) {
      rowSize = rowData[i].limit();
      short freeSpaceInPage = dataPage.getShort(_format.OFFSET_FREE_SPACE);
      if (freeSpaceInPage < (rowSize + _format.SIZE_ROW_LOCATION)) {
        //Last data page is full.  Create a new one.
        if (rowSize + _format.SIZE_ROW_LOCATION > _format.MAX_ROW_SIZE) {
          throw new IOException("Row size " + rowSize + " is too large");
        }
        _pageChannel.writePage(dataPage, pageNumber);
        dataPage.clear();
        pageNumber = newDataPage(dataPage, rowData[i]);
        _freeSpacePages.removePageNumber(pageNumber);
        freeSpaceInPage = dataPage.getShort(_format.OFFSET_FREE_SPACE);
      }
      //Decrease free space record.
      dataPage.putShort(_format.OFFSET_FREE_SPACE, (short) (freeSpaceInPage -
          rowSize - _format.SIZE_ROW_LOCATION));
      //Increment row count record.
      short rowCount = dataPage.getShort(_format.OFFSET_NUM_ROWS_ON_DATA_PAGE);
      dataPage.putShort(_format.OFFSET_NUM_ROWS_ON_DATA_PAGE, (short) (rowCount + 1));
      short rowLocation = (short) _format.PAGE_SIZE;
      if (rowCount > 0) {
        rowLocation = dataPage.getShort(_format.OFFSET_DATA_ROW_LOCATION_BLOCK +
            (rowCount - 1) * _format.SIZE_ROW_LOCATION);
      }
      rowLocation -= rowSize;
      dataPage.putShort(_format.OFFSET_DATA_ROW_LOCATION_BLOCK +
          rowCount * _format.SIZE_ROW_LOCATION, rowLocation);
      dataPage.position(rowLocation);
      dataPage.put(rowData[i]);
      iter = _indexes.iterator();
      while (iter.hasNext()) {
        Index index = (Index) iter.next();
        index.addRow((Object[]) rows.get(i), pageNumber, (byte) rowCount);
      }
    }
    _pageChannel.writePage(dataPage, pageNumber);
    
    //Update tdef page
    ByteBuffer tdefPage = _pageChannel.createPageBuffer();
    _pageChannel.readPage(tdefPage, _tableDefPageNumber);
    tdefPage.putInt(_format.OFFSET_NUM_ROWS, ++_rowCount);
    iter = _indexes.iterator();
    for (int i = 0; i < _indexes.size(); i++) {
      tdefPage.putInt(_format.OFFSET_INDEX_DEF_BLOCK +
          i * _format.SIZE_INDEX_DEFINITION + 4, _rowCount);
      Index index = (Index) iter.next();
      index.update();
    }
    _pageChannel.writePage(tdefPage, _tableDefPageNumber);
  }
  
  /**
   * Create a new data page
   * @return Page number of the new page
   */
  private int newDataPage(ByteBuffer dataPage, ByteBuffer rowData) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating new data page");
    }
    dataPage.put(PageTypes.DATA); //Page type
    dataPage.put((byte) 1); //Unknown
    dataPage.putShort((short) (_format.PAGE_SIZE - _format.OFFSET_DATA_ROW_LOCATION_BLOCK -
        (rowData.limit() - 1) - _format.SIZE_ROW_LOCATION)); //Free space in this page
    dataPage.putInt(_tableDefPageNumber); //Page pointer to table definition
    dataPage.putInt(0); //Unknown
    dataPage.putInt(0); //Number of records on this page
    int pageNumber = _pageChannel.writeNewPage(dataPage);
    _ownedPages.addPageNumber(pageNumber);
    _freeSpacePages.addPageNumber(pageNumber);
    return pageNumber;
  }
  
  /**
   * Serialize a row of Objects into a byte buffer
   */
  ByteBuffer createRow(Object[] rowArray) throws IOException {
    ByteBuffer buffer = _pageChannel.createPageBuffer();
    buffer.putShort((short) _columns.size());
    NullMask nullMask = new NullMask(_columns.size());
    Iterator iter;
    int index = 0;
    Column col;
    List<Object> row = new ArrayList<Object>(Arrays.asList(rowArray));
    
    //Append null for arrays that are too small
    for (int i = rowArray.length; i < _columnCount; i++) {
      row.add(null);
    }
    
    for (iter = _columns.iterator(); iter.hasNext() && index < row.size(); index++) {
      col = (Column) iter.next();
      if (!col.isVariableLength()) {
        //Fixed length column data comes first
        buffer.put(col.write(row.get(index)));
      }
      if (col.getType() == DataType.BOOLEAN) {
        if (row.get(index) != null) {
          if (!((Boolean) row.get(index)).booleanValue()) {
            //Booleans are stored in the null mask
            nullMask.markNull(index);
          }
        }
      } else if (row.get(index) == null) {
        nullMask.markNull(index);
      }
    }
    int varLengthCount = Column.countVariableLength(_columns);
    short[] varColumnOffsets = new short[varLengthCount];
    index = 0;
    int varColumnOffsetsIndex = 0;
    //Now write out variable length column data
    for (iter = _columns.iterator(); iter.hasNext() && index < row.size(); index++) {
      col = (Column) iter.next();
      short offset = (short) buffer.position();
      if (col.isVariableLength()) {
        if (row.get(index) != null) {
          buffer.put(col.write(row.get(index)));
        }
        varColumnOffsets[varColumnOffsetsIndex++] = offset;
      }
    }
    buffer.putShort((short) buffer.position()); //EOD marker
    //Now write out variable length offsets
    //Offsets are stored in reverse order
    for (int i = varColumnOffsets.length - 1; i >= 0; i--) {
      buffer.putShort(varColumnOffsets[i]);
    }
    buffer.putShort((short) varLengthCount);  //Number of var length columns
    buffer.put(nullMask.wrap());  //Null mask
    buffer.limit(buffer.position());
    buffer.flip();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating new data block:\n" + ByteUtil.toHexString(buffer, buffer.limit()));
    }
    return buffer;
  }
  
  public String toString() {
    StringBuffer rtn = new StringBuffer();
    rtn.append("Type: " + _tableType);
    rtn.append("\nRow count: " + _rowCount);
    rtn.append("\nColumn count: " + _columnCount);
    rtn.append("\nIndex count: " + _indexCount);
    rtn.append("\nColumns:\n");
    Iterator iter = _columns.iterator();
    while (iter.hasNext()) {
      rtn.append(iter.next().toString());
    }
    rtn.append("\nIndexes:\n");
    iter = _indexes.iterator();
    while (iter.hasNext()) {
      rtn.append(iter.next().toString());
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
    StringBuffer rtn = new StringBuffer();
    Iterator iter = _columns.iterator();
    while (iter.hasNext()) {
      Column col = (Column) iter.next();
      rtn.append(col.getName());
      if (iter.hasNext()) {
        rtn.append("\t");
      }
    }
    rtn.append("\n");
    Map row;
    int rowCount = 0;
    while ((rowCount++ < limit) && (row = getNextRow()) != null) {
      iter = row.values().iterator();
      while (iter.hasNext()) {
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
  
}
