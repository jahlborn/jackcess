/*
Copyright (c) 2005 Health Market Science, Inc.

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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.IndexBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Access table (logical) index.  Logical indexes are backed for IndexData,
 * where one or more logical indexes could be backed by the same data.
 * 
 * @author Tim McCune
 */
public class IndexImpl implements Index, Comparable<IndexImpl> 
{
  protected static final Log LOG = LogFactory.getLog(IndexImpl.class);
    
  /** index type for primary key indexes */
  public static final byte PRIMARY_KEY_INDEX_TYPE = (byte)1;
  
  /** index type for foreign key indexes */
  public static final byte FOREIGN_KEY_INDEX_TYPE = (byte)2;

  /** flag for indicating that updates should cascade in a foreign key index */
  private static final byte CASCADE_UPDATES_FLAG = (byte)1;
  /** flag for indicating that deletes should cascade in a foreign key index */
  private static final byte CASCADE_DELETES_FLAG = (byte)1;
  /** flag for indicating that deletes should cascade a null in a foreign key
      index */
  private static final byte CASCADE_NULL_FLAG = (byte)2;

  /** index table type for the "primary" table in a foreign key index */
  static final byte FK_PRIMARY_TABLE_TYPE = (byte)1;
  /** index table type for the "secondary" table in a foreign key index */
  static final byte FK_SECONDARY_TABLE_TYPE = (byte)2;

  /** indicate an invalid index number for foreign key field */
  private static final int INVALID_INDEX_NUMBER = -1;

  /** the actual data backing this index (more than one index may be backed by
      the same data */
  private final IndexData _data;
  /** 0-based index number */
  private final int _indexNumber;
  /** the type of the index */
  private final byte _indexType;
  /** Index name */
  private String _name;
  /** foreign key reference info, if any */
  private final ForeignKeyReference _reference;
  
  protected IndexImpl(ByteBuffer tableBuffer, List<IndexData> indexDatas,
                      JetFormat format) 
    throws IOException
  {
    ByteUtil.forward(tableBuffer, format.SKIP_BEFORE_INDEX_SLOT); //Forward past Unknown
    _indexNumber = tableBuffer.getInt();
    int indexDataNumber = tableBuffer.getInt();
      
    // read foreign key reference info
    byte relIndexType = tableBuffer.get();
    int relIndexNumber = tableBuffer.getInt();
    int relTablePageNumber = tableBuffer.getInt();
    byte cascadeUpdatesFlag = tableBuffer.get();
    byte cascadeDeletesFlag = tableBuffer.get();

    _indexType = tableBuffer.get();
 
    if((_indexType == FOREIGN_KEY_INDEX_TYPE) && 
       (relIndexNumber != INVALID_INDEX_NUMBER)) {
      _reference = new ForeignKeyReference(
          relIndexType, relIndexNumber, relTablePageNumber,
          ((cascadeUpdatesFlag & CASCADE_UPDATES_FLAG) != 0),
          ((cascadeDeletesFlag & CASCADE_DELETES_FLAG) != 0),
          ((cascadeDeletesFlag & CASCADE_NULL_FLAG) != 0));
    } else {
      _reference = null;
    }

    ByteUtil.forward(tableBuffer, format.SKIP_AFTER_INDEX_SLOT); //Skip past Unknown

    _data = indexDatas.get(indexDataNumber);

    _data.addIndex(this);
  }

  public IndexData getIndexData() {
    return _data;
  }

  @Override
  public TableImpl getTable() {
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

  @Override
  public String getName() {
    return _name;
  }
  
  void setName(String name) {
    _name = name;
  }

  @Override
  public boolean isPrimaryKey() {
    return _indexType == PRIMARY_KEY_INDEX_TYPE;
  }

  @Override
  public boolean isForeignKey() {
    return _indexType == FOREIGN_KEY_INDEX_TYPE;
  }

  public ForeignKeyReference getReference() {
    return _reference;
  }

  @Override
  public IndexImpl getReferencedIndex() throws IOException {

    if(_reference == null) {
      return null;
    }

    TableImpl refTable = getTable().getDatabase().getTable(
        _reference.getOtherTablePageNumber());

    if(refTable == null) {
      throw new IOException(withErrorContext(
              "Reference to missing table " + _reference.getOtherTablePageNumber()));
    }

    IndexImpl refIndex = null;
    int idxNumber = _reference.getOtherIndexNumber();
    for(IndexImpl idx : refTable.getIndexes()) {
      if(idx.getIndexNumber() == idxNumber) {
        refIndex = idx;
        break;
      }
    }
    
    if(refIndex == null) {
      throw new IOException(withErrorContext(
              "Reference to missing index " + idxNumber + 
              " on table " + refTable.getName()));
    }

    // finally verify that we found the expected index (should reference this
    // index)
    ForeignKeyReference otherRef = refIndex.getReference();
    if((otherRef == null) ||
       (otherRef.getOtherTablePageNumber() != 
        getTable().getTableDefPageNumber()) ||
       (otherRef.getOtherIndexNumber() != _indexNumber)) {
      throw new IOException(withErrorContext(
              "Found unexpected index " + refIndex.getName() +
              " on table " + refTable.getName() + " with reference " + otherRef));
    }

    return refIndex;
  }

  @Override
  public boolean shouldIgnoreNulls() {
    return getIndexData().shouldIgnoreNulls();
  }

  @Override
  public boolean isUnique() {
    return getIndexData().isUnique();
  }
  
  @Override
  public boolean isRequired() {
    return getIndexData().isRequired();
  }
  
  @Override
  public List<IndexData.ColumnDescriptor> getColumns() {
    return getIndexData().getColumns();
  }

  @Override
  public int getColumnCount() {
    return getIndexData().getColumnCount();
  }

  @Override
  public CursorBuilder newCursor() {
    return getTable().newCursor().setIndex(this);
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
   * column values, possibly only providing a prefix subset of the index
   * columns (at least one value must be provided).  If a prefix entry is
   * provided, any missing, trailing index entry values will use the given
   * filler value.
   * @return the appropriate sparse array of data
   * @throws IllegalArgumentException if at least one value is not provided
   */
  public Object[] constructPartialIndexRowFromEntry(
      Object filler, Object... values)
  {
    return getIndexData().constructPartialIndexRowFromEntry(filler, values);
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
   * column value, which must be the first column of the index.  Any missing,
   * trailing index entry values will use the given filler value.
   * @return the appropriate sparse array of data or {@code null} if no prefix
   *         list of columns for this index were provided
   */
  public Object[] constructPartialIndexRow(Object filler, String colName, Object value)
  {
    return constructPartialIndexRow(filler, Collections.singletonMap(colName, value));
  }
  
  /**
   * Constructs an array of values appropriate for this index from the given
   * column values.
   * @return the appropriate sparse array of data or {@code null} if not all
   *         columns for this index were provided
   */
  public Object[] constructIndexRow(Map<String,?> row)
  {
    return getIndexData().constructIndexRow(row);
  }  

  /**
   * Constructs an array of values appropriate for this index from the given
   * column values, possibly only using a subset of the given values.  A
   * partial row can be created if one or more prefix column values are
   * provided.  If a prefix can be found, any missing, trailing index entry
   * values will use the given filler value.
   * @return the appropriate sparse array of data or {@code null} if no prefix
   *         list of columns for this index were provided
   */
  public Object[] constructPartialIndexRow(Object filler, Map<String,?> row)
  {
    return getIndexData().constructPartialIndexRow(filler, row);
  }  

  @Override
  public String toString() {
    ToStringBuilder sb = CustomToStringStyle.builder(this)
      .append("name", "(" + getTable().getName() + ") " + _name)
      .append("number", _indexNumber)
      .append("isPrimaryKey", isPrimaryKey())
      .append("isForeignKey", isForeignKey());
    if(_reference != null) {
      sb.append("foreignKeyReference", _reference);
    }
    sb.append("data", _data);
    return sb.toString();
  }
  
  @Override
  public int compareTo(IndexImpl other) {
    if (_indexNumber > other.getIndexNumber()) {
      return 1;
    } else if (_indexNumber < other.getIndexNumber()) {
      return -1;
    } else {
      return 0;
    }
  }

  /**
   * Writes the logical index definitions into a table definition buffer.
   * @param creator description of the indexes to write
   * @param buffer Buffer to write to
   */
  protected static void writeDefinitions(
      TableCreator creator, ByteBuffer buffer)
    throws IOException
  {
    // write logical index information
    for(IndexBuilder idx : creator.getIndexes()) {
      writeDefinition(creator, idx, buffer);
    }

    // write index names
    for(IndexBuilder idx : creator.getIndexes()) {
      TableImpl.writeName(buffer, idx.getName(), creator.getCharset());
    }
  }

  protected static void writeDefinition(
      TableMutator mutator, IndexBuilder idx, ByteBuffer buffer)
    throws IOException
  {
    TableMutator.IndexDataState idxDataState = mutator.getIndexDataState(idx);

    // write logical index information
    buffer.putInt(TableImpl.MAGIC_TABLE_NUMBER); // seemingly constant magic value which matches the table def
    buffer.putInt(idx.getIndexNumber()); // index num
    buffer.putInt(idxDataState.getIndexDataNumber()); // index data num

    byte idxType = idx.getType();
    if(idxType != FOREIGN_KEY_INDEX_TYPE) {
      buffer.put((byte)0); // related table type
      buffer.putInt(INVALID_INDEX_NUMBER); // related index num
      buffer.putInt(0); // related table definition page number
      buffer.put((byte)0); // cascade updates flag
      buffer.put((byte)0); // cascade deletes flag
    } else {
      ForeignKeyReference reference = mutator.getForeignKey(idx);
      buffer.put(reference.getTableType()); // related table type
      buffer.putInt(reference.getOtherIndexNumber()); // related index num
      buffer.putInt(reference.getOtherTablePageNumber()); // related table definition page number
      byte updateFlags = 0;
      if(reference.isCascadeUpdates()) {
        updateFlags |= CASCADE_UPDATES_FLAG;
      }
      byte deleteFlags = 0;
      if(reference.isCascadeDeletes()) {
        deleteFlags |= CASCADE_DELETES_FLAG;
      }
      if(reference.isCascadeNullOnDelete()) {
        deleteFlags |= CASCADE_NULL_FLAG;
      }
      buffer.put(updateFlags); // cascade updates flag
      buffer.put(deleteFlags); // cascade deletes flag
    }
    buffer.put(idxType); // index type flags
    buffer.putInt(0); // unknown
  }

  private String withErrorContext(String msg) {
    return withErrorContext(msg, getTable().getDatabase(), getName());
  }

  private static String withErrorContext(String msg, DatabaseImpl db,
                                         String idxName) {
    return msg + " (Db=" + db.getName() + ";Index=" + idxName + ")";
  }
  

  /**
   * Information about a foreign key reference defined in an index (when
   * referential integrity should be enforced).
   */
  public static class ForeignKeyReference
  {
    private final byte _tableType;
    private final int _otherIndexNumber;
    private final int _otherTablePageNumber;
    private final boolean _cascadeUpdates;
    private final boolean _cascadeDeletes;
    private final boolean _cascadeNull;
    
    public ForeignKeyReference(
        byte tableType, int otherIndexNumber, int otherTablePageNumber,
        boolean cascadeUpdates, boolean cascadeDeletes, boolean cascadeNull)
    {
      _tableType = tableType;
      _otherIndexNumber = otherIndexNumber;
      _otherTablePageNumber = otherTablePageNumber;
      _cascadeUpdates = cascadeUpdates;
      _cascadeDeletes = cascadeDeletes;
      _cascadeNull = cascadeNull;
    }

    public byte getTableType() {
      return _tableType;
    }

    public boolean isPrimaryTable() {
      return(getTableType() == FK_PRIMARY_TABLE_TYPE);
    }

    public int getOtherIndexNumber() {
      return _otherIndexNumber;
    }

    public int getOtherTablePageNumber() {
      return _otherTablePageNumber;
    }

    public boolean isCascadeUpdates() {
      return _cascadeUpdates;
    }

    public boolean isCascadeDeletes() {
      return _cascadeDeletes;
    }

    public boolean isCascadeNullOnDelete() {
      return _cascadeNull;
    }

    @Override
    public String toString() {
      return CustomToStringStyle.builder(this)
        .append("otherIndexNumber", _otherIndexNumber)
        .append("otherTablePageNum", _otherTablePageNumber)
        .append("isPrimaryTable", isPrimaryTable())
        .append("isCascadeUpdates", isCascadeUpdates())
        .append("isCascadeDeletes", isCascadeDeletes())
        .append("isCascadeNullOnDelete", isCascadeNullOnDelete())
        .toString();
    }
  }
}
