/*
Copyright (c) 2011 James Ahlborn

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
class TableCreator extends DBMutator
{
  private final String _name;
  private final List<ColumnBuilder> _columns;
  private final List<IndexBuilder> _indexes;
  private final List<IndexDataState> _indexDataStates = 
    new ArrayList<IndexDataState>();
  private final Map<ColumnBuilder,ColumnState> _columnStates = 
    new IdentityHashMap<ColumnBuilder,ColumnState>();
  private final List<ColumnBuilder> _lvalCols = new ArrayList<ColumnBuilder>();
  private int _tdefPageNumber = PageChannel.INVALID_PAGE_NUMBER;
  private int _umapPageNumber = PageChannel.INVALID_PAGE_NUMBER;
  private int _indexCount;
  private int _logicalIndexCount;

  public TableCreator(DatabaseImpl database, String name, 
                      List<ColumnBuilder> columns, List<IndexBuilder> indexes) {
    super(database);
    _name = name;
    _columns = columns;
    _indexes = ((indexes != null) ? indexes : 
                Collections.<IndexBuilder>emptyList());
  }

  public String getName() {
    return _name;
  }

  @Override
  String getTableName() {
    return getName();
  }
  
  @Override
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

  @Override
  public IndexDataState getIndexDataState(IndexBuilder idx) {
    for(IndexDataState idxDataState : _indexDataStates) {
      for(IndexBuilder curIdx : idxDataState.getIndexes()) {
        if(idx == curIdx) {
          return idxDataState;
        }
      }
    }
    throw new IllegalStateException("could not find state for index");
  }

  public List<IndexDataState> getIndexDataStates() {
    return _indexDataStates;
  }

  @Override
  public ColumnState getColumnState(ColumnBuilder col) {
    return _columnStates.get(col);
  }

  public List<ColumnBuilder> getLongValueColumns() {
    return _lvalCols;
  }

  @Override
  short getColumnNumber(String colName) {
    for(ColumnBuilder col : _columns) {
      if(col.getName().equalsIgnoreCase(colName)) {
        return col.getColumnNumber();
      }
    }
    return IndexData.COLUMN_UNUSED;
  }  

  /**
   * @return The number of variable length columns which are not long values
   *         found in the list
   * @usage _advanced_method_
   */
  public short countNonLongVariableLength() {
    short rtn = 0;
    for (ColumnBuilder col : _columns) {
      if (col.isVariableLength() && !col.getType().isLongValue()) {
        rtn++;
      }
    }
    return rtn;
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
      // sort out index numbers (and backing index data).  
      for(IndexBuilder idx : _indexes) {
        idx.setIndexNumber(_logicalIndexCount++);
        findIndexDataState(idx);
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
      getDatabase().addNewTable(_name, _tdefPageNumber, DatabaseImpl.TYPE_TABLE, 
                                null, null);

    } finally {
      getPageChannel().finishWrite();
    }
  }

  private IndexDataState findIndexDataState(IndexBuilder idx) {

    // search for an index which matches the given index (in terms of the
    // backing data)
    for(IndexDataState idxDataState : _indexDataStates) {
      if(sameIndexData(idxDataState.getFirstIndex(), idx)) {
        idxDataState.addIndex(idx);
        return idxDataState;
      }
    }

    // no matches found, need new index data state
    IndexDataState idxDataState = new IndexDataState();
    idxDataState.setIndexDataNumber(_indexCount++);
    idxDataState.addIndex(idx);
    _indexDataStates.add(idxDataState);
    return idxDataState;
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
    
    Set<String> colNames = new HashSet<String>();
    // next, validate the column definitions
    for(ColumnBuilder column : _columns) {
      validateColumn(colNames, column);
    }

    List<ColumnBuilder> autoCols = getAutoNumberColumns();
    if(autoCols.size() > 1) {
      // for most autonumber types, we can only have one of each type
      Set<DataType> autoTypes = EnumSet.noneOf(DataType.class);
      for(ColumnBuilder c : autoCols) {
        validateAutoNumberColumn(autoTypes, c);
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
      boolean foundPk[] = new boolean[1];
      for(IndexBuilder index : _indexes) {
        validateIndex(colNames, idxNames, foundPk, index);
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

  private static boolean sameIndexData(IndexBuilder idx1, IndexBuilder idx2) {
    // index data can be combined if flags match and columns (and col flags)
    // match
    if(idx1.getFlags() != idx2.getFlags()) {
      return false;
    }

    if(idx1.getColumns().size() != idx2.getColumns().size()) {
      return false;
    }
    
    for(int i = 0; i < idx1.getColumns().size(); ++i) {
      IndexBuilder.Column col1 = idx1.getColumns().get(i);
      IndexBuilder.Column col2 = idx2.getColumns().get(i);

      if(!sameIndexData(col1, col2)) {
        return false;
      }
    }

    return true;
  }

  private static boolean sameIndexData(
      IndexBuilder.Column col1, IndexBuilder.Column col2) {
    return (col1.getName().equals(col2.getName()) && 
            (col1.getFlags() == col2.getFlags()));
  }

}
