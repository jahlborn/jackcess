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
public class TableUpdater extends TableMutator
{
  private final TableImpl _table;

  private ColumnBuilder _column;
  private IndexBuilder _index;
  private int _origTdefLen;
  private int _addedTdefLen;
  private List<Integer> _nextPages = new ArrayList<Integer>(1);
  private ColumnState _colState;
  private IndexDataState _idxDataState;
  private IndexImpl.ForeignKeyReference _fkReference;

  public TableUpdater(TableImpl table) {
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
  String getTableName() {
    return _table.getName();
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

  void setForeignKey(IndexImpl.ForeignKeyReference fkReference) {
    _fkReference = fkReference;
  }

  @Override
  public IndexImpl.ForeignKeyReference getForeignKey(IndexBuilder idx) {
    return ((idx == _index) ? _fkReference : null);
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

  void resetTdefInfo() {
    _addedTdefLen = 0;
    _origTdefLen = 0;
    _nextPages.clear();
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
    return addIndex(index, false, (byte)0, (byte)0);
  }

  IndexImpl addIndex(IndexBuilder index, boolean isInternal, byte ignoreIdxFlags,
                     byte ignoreColFlags) 
    throws IOException 
  {
    _index = index;

    if(!isInternal) {
      validateAddIndex();
    }

    // assign index number and do some assorted index bookkeeping
    int indexNumber = _table.getLogicalIndexCount();
    _index.setIndexNumber(indexNumber);

    // initialize backing index state
    initIndexDataState(ignoreIdxFlags, ignoreColFlags);

    if(!isInternal) {
      getPageChannel().startExclusiveWrite();
    } else {
      // if "internal" update, this is part of a larger operation which
      // already holds an exclusive write lock
      getPageChannel().startWrite();      
    }
    try {

      if(_idxDataState.getIndexDataNumber() == _table.getIndexCount()) {
        // we need a new backing index data
        _table.mutateAddIndexData(this);

        // we need to modify the table def again when adding the Index, so reset
        resetTdefInfo();
      }

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
      throw new IllegalArgumentException(withErrorContext(
          "Cannot add column with no column"));
    }
    if((_table.getColumnCount() + 1) > getFormat().MAX_COLUMNS_PER_TABLE) {
      throw new IllegalArgumentException(withErrorContext(
          "Cannot add column to table with " +
          getFormat().MAX_COLUMNS_PER_TABLE + " columns"));
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
      throw new IllegalArgumentException(withErrorContext(
          "Cannot add index with no index"));
    }
    if((_table.getLogicalIndexCount() + 1) > getFormat().MAX_INDEXES_PER_TABLE) {
      throw new IllegalArgumentException(withErrorContext(
          "Cannot add index to table with " +
          getFormat().MAX_INDEXES_PER_TABLE + " indexes"));
    }
    
    boolean foundPk[] = new boolean[1];
    Set<String> idxNames = getIndexNames(_table, foundPk);
    // next, validate the index definition
    validateIndex(getColumnNames(), idxNames, foundPk, _index);    
  }

  private Set<String> getColumnNames() {
    Set<String> colNames = new HashSet<String>();
    for(ColumnImpl column : _table.getColumns()) {
      colNames.add(DatabaseImpl.toLookupName(column.getName()));
    }
    return colNames;
  }

  static Set<String> getIndexNames(TableImpl table, boolean[] foundPk) {
    Set<String> idxNames = new HashSet<String>();
    for(IndexImpl index : table.getIndexes()) {
      idxNames.add(DatabaseImpl.toLookupName(index.getName()));
      if(index.isPrimaryKey() && (foundPk != null)) {
        foundPk[0] = true;
      }
    }
    return idxNames;
  }

  private void initIndexDataState(byte ignoreIdxFlags, byte ignoreColFlags) {

    _idxDataState = new IndexDataState();
    _idxDataState.addIndex(_index);
    
    // search for an existing index which matches the given index (in terms of
    // the backing data)
    IndexData idxData = findIndexData(
        _index, _table, ignoreIdxFlags, ignoreColFlags);

    int idxDataNumber = ((idxData != null) ?
                         idxData.getIndexDataNumber() :
                         _table.getIndexCount());

    _idxDataState.setIndexDataNumber(idxDataNumber);
  }

  static IndexData findIndexData(IndexBuilder idx, TableImpl table,
                                 byte ignoreIdxFlags, byte ignoreColFlags)
  {
    for(IndexData idxData : table.getIndexDatas()) {
      if(sameIndexData(idx, idxData, ignoreIdxFlags, ignoreColFlags)) {
        return idxData;
      }
    }
    return null;
  }
  
  private static boolean sameIndexData(IndexBuilder idx1, IndexData idx2,
                                       byte ignoreIdxFlags, byte ignoreColFlags) {
    // index data can be combined if flags match and columns (and col flags)
    // match
    if((idx1.getFlags() | ignoreIdxFlags) !=
       (idx2.getIndexFlags() | ignoreIdxFlags)) {
      return false;
    }

    if(idx1.getColumns().size() != idx2.getColumnCount()) {
      return false;
    }
    
    for(int i = 0; i < idx1.getColumns().size(); ++i) {
      IndexBuilder.Column col1 = idx1.getColumns().get(i);
      IndexData.ColumnDescriptor col2 = idx2.getColumns().get(i);

      if(!sameIndexData(col1, col2, ignoreColFlags)) {
        return false;
      }
    }

    return true;
  }

  private static boolean sameIndexData(
      IndexBuilder.Column col1, IndexData.ColumnDescriptor col2,
      int ignoreColFlags) {
    return (col1.getName().equals(col2.getName()) && 
            ((col1.getFlags() | ignoreColFlags) ==
             (col2.getFlags() | ignoreColFlags)));
  }

  @Override
  protected String withErrorContext(String msg) {
    String objStr = "";
    if(_column != null) {
      objStr = ";Column=" + _column.getName();
    } else if(_index != null) {
      objStr = ";Index=" + _index.getName();
    }
    return msg + "(Table=" + _table.getName() + objStr + ")";
  }
}
