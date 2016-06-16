/*
Copyright (c) 2016 James Ahlborn

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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.IndexBuilder;

/**
 * Helper class used to maintain state during table mutation.
 *
 * @author James Ahlborn
 * @usage _advanced_class_
 */
public class TableMutator extends DBMutator
{
  private final TableImpl _table;

  private ColumnBuilder _column;
  private IndexBuilder _index;
  private int _origTdefLen;
  private int _addedTdefLen;
  private List<Integer> _nextPages = new ArrayList<Integer>(1);
  private ColumnState _colState;
  private IndexDataState _idxDataState;

  public TableMutator(TableImpl table) {
    super(table.getDatabase());
    _table = table;
  }

  public ColumnBuilder getColumn() {
    return _column;
  }

  public IndexBuilder getIndex() {
    return _index;
  }

  @Override
  public int getTdefPageNumber() {
    return _table.getTableDefPageNumber();
  }

  @Override
  short getColumnNumber(String colName) {
    for(ColumnImpl col : _table.getColumns()) {
      if(col.getName().equalsIgnoreCase(colName)) {
        return col.getColumnNumber();
      }
    }
    return IndexData.COLUMN_UNUSED;
  }

  @Override
  public ColumnState getColumnState(ColumnBuilder col) {
    return ((col == _column) ? _colState : null);
  }

  @Override
  public IndexDataState getIndexDataState(IndexBuilder idx) {
    return ((idx == _index) ? _idxDataState : null);
  }

  int getAddedTdefLen() {
    return _addedTdefLen;
  }

  void addTdefLen(int add) {
    _addedTdefLen += add;
  }

  void setOrigTdefLen(int len) {
    _origTdefLen = len;
  }

  List<Integer> getNextPages() {
    return _nextPages;
  }

  public ColumnImpl addColumn(ColumnBuilder column) throws IOException {

    _column = column;

    validateAddColumn();
    
    // assign column number and do some assorted column bookkeeping
    short columnNumber = (short)_table.getMaxColumnCount();
    _column.setColumnNumber(columnNumber);
    if(_column.getType().isLongValue()) {
      _colState = new ColumnState();
    }

    getPageChannel().startExclusiveWrite();
    try {

      return _table.mutateAddColumn(this);

    } finally {
      getPageChannel().finishWrite();
    }
  }

  public IndexImpl addIndex(IndexBuilder index) throws IOException {

    _index = index;

    validateAddIndex();

    // assign index number and do some assorted index bookkeeping
    int indexNumber = _table.getLogicalIndexCount();
    _index.setIndexNumber(indexNumber);

    // find backing index state

    // FIXME, writeme!


    getPageChannel().startExclusiveWrite();
    try {

      // FIXME, maybe add index data
      // _table.mutateAddIndexData(this);

      return _table.mutateAddIndex(this);

    } finally {
      getPageChannel().finishWrite();
    }
  }

  boolean validateUpdatedTdef(ByteBuffer tableBuffer) {
    // sanity check the updates
    return((_origTdefLen + _addedTdefLen) == tableBuffer.limit());
  }

  private void validateAddColumn() {

    if(_column == null) {
      throw new IllegalArgumentException("Cannot add column with no column");
    }
    if((_table.getColumnCount() + 1) > getFormat().MAX_COLUMNS_PER_TABLE) {
      throw new IllegalArgumentException(
          "Cannot add column to table with " +
          getFormat().MAX_COLUMNS_PER_TABLE + " columns");
    }
    
    Set<String> colNames = getColumnNames();
    // next, validate the column definition
    validateColumn(colNames, _column);
    
    if(_column.isAutoNumber()) {
      // for most autonumber types, we can only have one of each type
      Set<DataType> autoTypes = EnumSet.noneOf(DataType.class);
      for(ColumnImpl column : _table.getAutoNumberColumns()) {
        autoTypes.add(column.getType());
      }

      validateAutoNumberColumn(autoTypes, _column);
    }
  }

  private void validateAddIndex() {
    
    if(_index == null) {
      throw new IllegalArgumentException("Cannot add index with no index");
    }
    if((_table.getLogicalIndexCount() + 1) > getFormat().MAX_INDEXES_PER_TABLE) {
      throw new IllegalArgumentException(
          "Cannot add index to table with " +
          getFormat().MAX_INDEXES_PER_TABLE + " indexes");
    }
    
    boolean foundPk[] = new boolean[1];
    Set<String> idxNames = getIndexNames(foundPk);
    // next, validate the index definition
    validateIndex(getColumnNames(), idxNames, foundPk, _index);    
  }

  private Set<String> getColumnNames() {
    Set<String> colNames = new HashSet<String>();
    for(ColumnImpl column : _table.getColumns()) {
      colNames.add(column.getName().toUpperCase());
    }
    return colNames;
  }

  private Set<String> getIndexNames(boolean[] foundPk) {
    Set<String> idxNames = new HashSet<String>();
    for(IndexImpl index : _table.getIndexes()) {
      idxNames.add(index.getName().toUpperCase());
      if(index.isPrimaryKey()) {
        foundPk[0] = true;
      }
    }
    return idxNames;
  }
}
