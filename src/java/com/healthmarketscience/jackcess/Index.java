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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Access table (logical) index.  Logical indexes are backed for IndexData,
 * where one or more logical indexes could be backed by the same data.
 * 
 * @author Tim McCune
 */
public class Index implements Comparable<Index> {
  
  protected static final Log LOG = LogFactory.getLog(Index.class);
    
  /** index type for primary key indexes */
  private static final byte PRIMARY_KEY_INDEX_TYPE = (byte)1;
  
  /** index type for foreign key indexes */
  private static final byte FOREIGN_KEY_INDEX_TYPE = (byte)2;

  /** the actual data backing this index (more than one index may be backed by
      the same data */
  private final IndexData _data;
  /** 0-based index number */
  private final int _indexNumber;
  /** the type of the index */
  private final byte _indexType;
  /** Index name */
  private String _name;
  
  protected Index(IndexData data, int indexNumber, byte indexType) {
    _data = data;
    _indexNumber = indexNumber;
    _indexType = indexType;
    data.addIndex(this);
  }

  public IndexData getIndexData() {
    return _data;
  }

  public Table getTable() {
    return getIndexData().getTable();
  }
  
  public JetFormat getFormat() {
    return getTable().getFormat();
  }

  public PageChannel getPageChannel() {
    return getTable().getPageChannel();
  }

  public int getIndexNumber() {
    return _indexNumber;
  }

  public byte getIndexFlags() {
    return getIndexData().getIndexFlags();
  }
  
  public int getUniqueEntryCount() {
    return getIndexData().getUniqueEntryCount();
  }

  public int getUniqueEntryCountOffset() {
    return getIndexData().getUniqueEntryCountOffset();
  }

  public String getName() {
    return _name;
  }
  
  public void setName(String name) {
    _name = name;
  }

  public boolean isPrimaryKey() {
    return _indexType == PRIMARY_KEY_INDEX_TYPE;
  }

  public boolean isForeignKey() {
    return _indexType == FOREIGN_KEY_INDEX_TYPE;
  }

  /**
   * Whether or not {@code null} values are actually recorded in the index.
   */
  public boolean shouldIgnoreNulls() {
    return getIndexData().shouldIgnoreNulls();
  }

  /**
   * Whether or not index entries must be unique.
   * <p>
   * Some notes about uniqueness:
   * <ul>
   * <li>Access does not seem to consider multiple {@code null} entries
   *     invalid for a unique index</li>
   * <li>text indexes collapse case, and Access seems to compare <b>only</b>
   *     the index entry bytes, therefore two strings which differ only in
   *     case <i>will violate</i> the unique constraint</li>
   * </ul>
   */
  public boolean isUnique() {
    return getIndexData().isUnique();
  }
  
  /**
   * Returns the Columns for this index (unmodifiable)
   */
  public List<IndexData.ColumnDescriptor> getColumns() {
    return getIndexData().getColumns();
  }

  /**
   * Whether or not the complete index state has been read.
   */
  public boolean isInitialized() {
    return getIndexData().isInitialized();
  }
  
  /**
   * Forces initialization of this index (actual parsing of index pages).
   * normally, the index will not be initialized until the entries are
   * actually needed.
   */
  public void initialize() throws IOException {
    getIndexData().initialize();
  }

  /**
   * Writes the current index state to the database.
   * <p>
   * Forces index initialization.
   */
  public void update() throws IOException {
    getIndexData().update();
  }

  /**
   * Adds a row to this index
   * <p>
   * Forces index initialization.
   * 
   * @param row Row to add
   * @param rowId rowId of the row to be added
   */
  public void addRow(Object[] row, RowId rowId)
    throws IOException
  {
    getIndexData().addRow(row, rowId);
  }
  
  /**
   * Removes a row from this index
   * <p>
   * Forces index initialization.
   * 
   * @param row Row to remove
   * @param rowId rowId of the row to be removed
   */
  public void deleteRow(Object[] row, RowId rowId)
    throws IOException
  {
    getIndexData().deleteRow(row, rowId);
  }
      
  /**
   * Gets a new cursor for this index.
   * <p>
   * Forces index initialization.
   */
  public IndexData.EntryCursor cursor()
    throws IOException
  {
    return cursor(null, true, null, true);
  }
  
  /**
   * Gets a new cursor for this index, narrowed to the range defined by the
   * given startRow and endRow.
   * <p>
   * Forces index initialization.
   * 
   * @param startRow the first row of data for the cursor, or {@code null} for
   *                 the first entry
   * @param startInclusive whether or not startRow is inclusive or exclusive
   * @param endRow the last row of data for the cursor, or {@code null} for
   *               the last entry
   * @param endInclusive whether or not endRow is inclusive or exclusive
   */
  public IndexData.EntryCursor cursor(Object[] startRow,
                                      boolean startInclusive,
                                      Object[] endRow,
                                      boolean endInclusive)
    throws IOException
  {
    return getIndexData().cursor(startRow, startInclusive, endRow,
                                 endInclusive);
  }

  /**
   * Constructs an array of values appropriate for this index from the given
   * column values, expected to match the columns for this index.
   * @return the appropriate sparse array of data
   * @throws IllegalArgumentException if the wrong number of values are
   *         provided
   */
  public Object[] constructIndexRowFromEntry(Object... values)
  {
    return getIndexData().constructIndexRowFromEntry(values);
  }
    
  /**
   * Constructs an array of values appropriate for this index from the given
   * column value.
   * @return the appropriate sparse array of data or {@code null} if not all
   *         columns for this index were provided
   */
  public Object[] constructIndexRow(String colName, Object value)
  {
    return constructIndexRow(Collections.singletonMap(colName, value));
  }
  
  /**
   * Constructs an array of values appropriate for this index from the given
   * column values.
   * @return the appropriate sparse array of data or {@code null} if not all
   *         columns for this index were provided
   */
  public Object[] constructIndexRow(Map<String,Object> row)
  {
    return getIndexData().constructIndexRow(row);
  }  

  @Override
  public String toString() {
    StringBuilder rtn = new StringBuilder();
    rtn.append("\tName: (" + getTable().getName() + ") " + _name);
    rtn.append("\n\tNumber: " + _indexNumber);
    rtn.append("\n\tIs Primary Key: " + isPrimaryKey());
    rtn.append("\n\tIs Foreign Key: " + isForeignKey());
    rtn.append(_data.toString());
    rtn.append("\n\n");
    return rtn.toString();
  }
  
  public int compareTo(Index other) {
    if (_indexNumber > other.getIndexNumber()) {
      return 1;
    } else if (_indexNumber < other.getIndexNumber()) {
      return -1;
    } else {
      return 0;
    }
  }

}
