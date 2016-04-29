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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;

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

  public TableMutator(TableImpl table) {
    super(table.getDatabase());
    _table = table;
  }

  public ColumnBuilder getColumn() {
    return _column;
  }

  public ColumnImpl addColumn(ColumnBuilder column) throws IOException
  {
    _column = column;

    validateAddColumn();
    
    // assign column numbers and do some assorted column bookkeeping
    short columnNumber = (short)_table.getMaxColumnCount();
    _column.setColumnNumber(columnNumber);

    getPageChannel().startExclusiveWrite();
    try {

      return _table.mutateAddColumn(this);

    } finally {
      getPageChannel().finishWrite();
    }
  }

  private void validateAddColumn()
  {
    if(_column == null) {
      throw new IllegalArgumentException("Cannot add column with no column");
    }
    if((_table.getColumnCount() + 1) > getFormat().MAX_COLUMNS_PER_TABLE) {
      throw new IllegalArgumentException(
          "Cannot add column to table with " +
          getFormat().MAX_COLUMNS_PER_TABLE + " columns");
    }
    
    Set<String> colNames = new HashSet<String>();
    // next, validate the column definitions
    for(ColumnImpl column : _table.getColumns()) {
      colNames.add(column.getName().toUpperCase());
    }

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
}
