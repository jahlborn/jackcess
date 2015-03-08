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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.IndexBuilder;

/**
 * Helper class used to maintain state during table creation.
 *
 * @author James Ahlborn
 * @usage _advanced_class_
 */
class TableCreator 
{
  private final DatabaseImpl _database;
  private final String _name;
  private final List<ColumnBuilder> _columns;
  private final List<IndexBuilder> _indexes;
  private final Map<IndexBuilder,IndexState> _indexStates = 
    new IdentityHashMap<IndexBuilder,IndexState>();
  private final Map<ColumnBuilder,ColumnState> _columnStates = 
    new IdentityHashMap<ColumnBuilder,ColumnState>();
  private final List<ColumnBuilder> _lvalCols = new ArrayList<ColumnBuilder>();
  private int _tdefPageNumber = PageChannel.INVALID_PAGE_NUMBER;
  private int _umapPageNumber = PageChannel.INVALID_PAGE_NUMBER;
  private int _indexCount;
  private int _logicalIndexCount;

  public TableCreator(DatabaseImpl database, String name, List<ColumnBuilder> columns,
                      List<IndexBuilder> indexes) {
    _database = database;
    _name = name;
    _columns = columns;
    _indexes = ((indexes != null) ? indexes : 
                Collections.<IndexBuilder>emptyList());
  }

  public String getName() {
    return _name;
  }

  public DatabaseImpl getDatabase() {
    return _database;
  }

  public JetFormat getFormat() {
    return _database.getFormat();
  }

  public PageChannel getPageChannel() {
    return _database.getPageChannel();
  }

  public Charset getCharset() {
    return _database.getCharset();
  }

  public int getTdefPageNumber() {
    return _tdefPageNumber;
  }

  public int getUmapPageNumber() {
    return _umapPageNumber;
  }

  public List<ColumnBuilder> getColumns() {
    return _columns;
  }

  public List<IndexBuilder> getIndexes() {
    return _indexes;
  }

  public boolean hasIndexes() {
    return !_indexes.isEmpty();
  }

  public int getIndexCount() {
    return _indexCount;
  }

  public int getLogicalIndexCount() {
    return _logicalIndexCount;
  }

  public IndexState getIndexState(IndexBuilder idx) {
    return _indexStates.get(idx);
  }

  public int reservePageNumber() throws IOException {
    return getPageChannel().allocateNewPage();
  }

  public ColumnState getColumnState(ColumnBuilder col) {
    return _columnStates.get(col);
  }

  public List<ColumnBuilder> getLongValueColumns() {
    return _lvalCols;
  }

  /**
   * Creates the table in the database.
   * @usage _advanced_method_
   */
  public void createTable() throws IOException {

    validate();

    // assign column numbers and do some assorted column bookkeeping
    short columnNumber = (short) 0;
    for(ColumnBuilder col : _columns) {
      col.setColumnNumber(columnNumber++);
      if(col.getType().isLongValue()) {
        _lvalCols.add(col);
        // only lval columns need extra state
        _columnStates.put(col, new ColumnState());
      }
    }

    if(hasIndexes()) {
      // sort out index numbers.  for now, these values will always match
      // (until we support writing foreign key indexes)
      for(IndexBuilder idx : _indexes) {
        IndexState idxState = new IndexState();
        idxState.setIndexNumber(_logicalIndexCount++);
        idxState.setIndexDataNumber(_indexCount++);
        _indexStates.put(idx, idxState);
      }
    }

    getPageChannel().startWrite();
    try {
      
      // reserve some pages
      _tdefPageNumber = reservePageNumber();
      _umapPageNumber = reservePageNumber();
    
      //Write the tdef page to disk.
      TableImpl.writeTableDefinition(this);

      // update the database with the new table info
      _database.addNewTable(_name, _tdefPageNumber, DatabaseImpl.TYPE_TABLE, null, null);

    } finally {
      getPageChannel().finishWrite();
    }
  }

  /**
   * Validates the new table information before attempting creation.
   */
  private void validate() {

    DatabaseImpl.validateIdentifierName(
        _name, getFormat().MAX_TABLE_NAME_LENGTH, "table");
    
    if((_columns == null) || _columns.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot create table with no columns");
    }
    if(_columns.size() > getFormat().MAX_COLUMNS_PER_TABLE) {
      throw new IllegalArgumentException(
          "Cannot create table with more than " +
          getFormat().MAX_COLUMNS_PER_TABLE + " columns");
    }
    
    ColumnImpl.SortOrder dbSortOrder = null;
    try {
      dbSortOrder = _database.getDefaultSortOrder();
    } catch(IOException e) {
      // ignored, just use the jet format default
    }

    Set<String> colNames = new HashSet<String>();
    // next, validate the column definitions
    for(ColumnBuilder column : _columns) {

      // FIXME for now, we can't create complex columns
      if(column.getType() == DataType.COMPLEX_TYPE) {
        throw new UnsupportedOperationException(
            "Complex column creation is not yet implemented");
      }
      
      column.validate(getFormat());
      if(!colNames.add(column.getName().toUpperCase())) {
        throw new IllegalArgumentException("duplicate column name: " +
                                           column.getName());
      }

      // set the sort order to the db default (if unspecified)
      if(column.getType().isTextual() && (column.getTextSortOrder() == null)) {
        column.setTextSortOrder(dbSortOrder);
      }
    }

    List<ColumnBuilder> autoCols = getAutoNumberColumns();
    if(autoCols.size() > 1) {
      // for most autonumber types, we can only have one of each type
      Set<DataType> autoTypes = EnumSet.noneOf(DataType.class);
      for(ColumnBuilder c : autoCols) {
        if(!c.getType().isMultipleAutoNumberAllowed() &&
           !autoTypes.add(c.getType())) {
          throw new IllegalArgumentException(
              "Can have at most one AutoNumber column of type " + c.getType() +
              " per table");
        }
      }
    }

    if(hasIndexes()) {

      if(_indexes.size() > getFormat().MAX_INDEXES_PER_TABLE) {
        throw new IllegalArgumentException(
            "Cannot create table with more than " +
            getFormat().MAX_INDEXES_PER_TABLE + " indexes");
      }

      // now, validate the indexes
      Set<String> idxNames = new HashSet<String>();
      boolean foundPk = false;
      for(IndexBuilder index : _indexes) {
        index.validate(colNames, getFormat());
        if(!idxNames.add(index.getName().toUpperCase())) {
          throw new IllegalArgumentException("duplicate index name: " +
                                             index.getName());
        }
        if(index.isPrimaryKey()) {
          if(foundPk) {
            throw new IllegalArgumentException(
                "found second primary key index: " + index.getName());
          }
          foundPk = true;
        }
      }
    }
  }

  private List<ColumnBuilder> getAutoNumberColumns() 
  {
    List<ColumnBuilder> autoCols = new ArrayList<ColumnBuilder>(1);
    for(ColumnBuilder c : _columns) {
      if(c.isAutoNumber()) {
        autoCols.add(c);
      }
    }
    return autoCols;
  }

  /**
   * Maintains additional state used during index creation.
   * @usage _advanced_class_
   */
  static final class IndexState
  {
    private int _indexNumber;
    private int _indexDataNumber;
    private byte _umapRowNumber;
    private int _umapPageNumber;
    private int _rootPageNumber;

    public int getIndexNumber() {
      return _indexNumber;
    }

    public void setIndexNumber(int newIndexNumber) {
      _indexNumber = newIndexNumber;
    }

    public int getIndexDataNumber() {
      return _indexDataNumber;
    }

    public void setIndexDataNumber(int newIndexDataNumber) {
      _indexDataNumber = newIndexDataNumber;
    }

    public byte getUmapRowNumber() {
      return _umapRowNumber;
    }

    public void setUmapRowNumber(byte newUmapRowNumber) {
      _umapRowNumber = newUmapRowNumber;
    }

    public int getUmapPageNumber() {
      return _umapPageNumber;
    }

    public void setUmapPageNumber(int newUmapPageNumber) {
      _umapPageNumber = newUmapPageNumber;
    }

    public int getRootPageNumber() {
      return _rootPageNumber;
    }

    public void setRootPageNumber(int newRootPageNumber) {
      _rootPageNumber = newRootPageNumber;
    }
  }
    
  /**
   * Maintains additional state used during column creation.
   * @usage _advanced_class_
   */
  static final class ColumnState
  {
    private byte _umapOwnedRowNumber;
    private byte _umapFreeRowNumber;
    // we always put both usage maps on the same page
    private int _umapPageNumber;

    public byte getUmapOwnedRowNumber() {
      return _umapOwnedRowNumber;
  }

    public void setUmapOwnedRowNumber(byte newUmapOwnedRowNumber) {
      _umapOwnedRowNumber = newUmapOwnedRowNumber;
}

    public byte getUmapFreeRowNumber() {
      return _umapFreeRowNumber;
    }

    public void setUmapFreeRowNumber(byte newUmapFreeRowNumber) {
      _umapFreeRowNumber = newUmapFreeRowNumber;
    }

    public int getUmapPageNumber() {
      return _umapPageNumber;
    }

    public void setUmapPageNumber(int newUmapPageNumber) {
      _umapPageNumber = newUmapPageNumber;
    }
  }
}
