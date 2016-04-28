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
import java.nio.charset.Charset;
import java.util.Set;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;

/**
 * Helper class used to maintain state during database mutation.
 *
 * @author James Ahlborn
 */
abstract class DBMutator 
{
  private final DatabaseImpl _database;
  private ColumnOffsets _colOffsets;

  protected DBMutator(DatabaseImpl database) {
    _database = database;
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

  public int reservePageNumber() throws IOException {
    return getPageChannel().allocateNewPage();
  }

  public void setColumnOffsets(
      int fixedOffset, int varOffset, int longVarOffset) {
    if(_colOffsets == null) {
      _colOffsets = new ColumnOffsets();
    }
    _colOffsets.set(fixedOffset, varOffset, longVarOffset);
  }

  public ColumnOffsets getColumnOffsets() {
    return _colOffsets;
  }

  public static int calculateNameLength(String name) {
    return (name.length() * JetFormat.TEXT_FIELD_UNIT_SIZE) + 2;
  }

  protected void validateColumn(Set<String> colNames, ColumnBuilder column) {

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

      setColumnSortOrder(column);
  }

  protected void validateAutoNumberColumn(Set<DataType> autoTypes, 
                                          ColumnBuilder column) 
  {
    if(!column.getType().isMultipleAutoNumberAllowed() &&
       !autoTypes.add(column.getType())) {
      throw new IllegalArgumentException(
          "Can have at most one AutoNumber column of type " + column.getType() +
          " per table");
    }    
  }

  private void setColumnSortOrder(ColumnBuilder column) {
      // set the sort order to the db default (if unspecified)
      if(column.getType().isTextual() && (column.getTextSortOrder() == null)) {
        column.setTextSortOrder(getDbSortOrder());
      }
  }

  private ColumnImpl.SortOrder getDbSortOrder() {
    try {
      return _database.getDefaultSortOrder();
    } catch(IOException e) {
      // ignored, just use the jet format default
    }
    return null;
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

  /**
   * Maintains additional state used during column writing.
   * @usage _advanced_class_
   */
  static final class ColumnOffsets
  {
    private short _fixedOffset;
    private short _varOffset;
    private short _longVarOffset;

    public void set(int fixedOffset, int varOffset, int longVarOffset) {
      _fixedOffset = (short)fixedOffset;
      _varOffset = (short)varOffset;
      _longVarOffset = (short)longVarOffset;
    }
    
    public short getNextVariableOffset(ColumnBuilder col) {
      if(!col.getType().isLongValue()) {
        return _varOffset++;
      }
      return _longVarOffset++;
    }

    public short getNextFixedOffset(ColumnBuilder col) {
      short offset = _fixedOffset;
      _fixedOffset += col.getType().getFixedSize(col.getLength());
      return offset;
    }
  }
}
