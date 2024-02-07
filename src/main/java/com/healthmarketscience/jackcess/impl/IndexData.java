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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.ConstraintViolationException;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.RuntimeIOException;
import static com.healthmarketscience.jackcess.impl.ByteUtil.ByteStream;
import static com.healthmarketscience.jackcess.impl.IndexCodes.*;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Access table index data.  This is the actual data which backs a logical
 * Index, where one or more logical indexes can be backed by the same index
 * data.
 *
 * @author Tim McCune
 */
public class IndexData {

  protected static final Log LOG = LogFactory.getLog(Index.class);

  /** special entry which is less than any other entry */
  public static final Entry FIRST_ENTRY =
    createSpecialEntry(RowIdImpl.FIRST_ROW_ID);

  /** special entry which is greater than any other entry */
  public static final Entry LAST_ENTRY =
    createSpecialEntry(RowIdImpl.LAST_ROW_ID);

  /** special object which will always be greater than any other value, when
      searching for an index entry range in a multi-value index */
  public static final Object MAX_VALUE = new Object();

  /** special object which will always be greater than any other value, when
      searching for an index entry range in a multi-value index */
  public static final Object MIN_VALUE = new Object();

  private static final DataPage NEW_ROOT_DATA_PAGE = new RootDataPage();

  protected static final int INVALID_INDEX_PAGE_NUMBER = 0;

  /** Max number of columns in an index */
  public static final int MAX_COLUMNS = 10;

  protected static final byte[] EMPTY_PREFIX = new byte[0];

  private static final byte[] ASC_EXT_DATE_TRAILER =
    {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02};
  private static final byte[] DESC_EXT_DATE_TRAILER =
    flipBytes(ByteUtil.copyOf(ASC_EXT_DATE_TRAILER, ASC_EXT_DATE_TRAILER.length));

  static final short COLUMN_UNUSED = -1;

  public static final byte ASCENDING_COLUMN_FLAG = (byte)0x01;

  public static final byte UNIQUE_INDEX_FLAG = (byte)0x01;
  public static final byte IGNORE_NULLS_INDEX_FLAG = (byte)0x02;
  public static final byte REQUIRED_INDEX_FLAG = (byte)0x08;
  public static final byte UNKNOWN_INDEX_FLAG = (byte)0x80; // always seems to be set on indexes in access 2000+

  private static final int MAGIC_INDEX_NUMBER = 1923;

  private static final ByteOrder ENTRY_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

  /** type attributes for Entries which simplify comparisons */
  public enum EntryType {
    /** comparable type indicating this Entry should always compare less than
        valid RowIds */
    ALWAYS_FIRST,
    /** comparable type indicating this Entry should always compare less than
        other valid entries with equal entryBytes */
    FIRST_VALID,
    /** comparable type indicating this RowId should always compare
        normally */
    NORMAL,
    /** comparable type indicating this Entry should always compare greater
        than other valid entries with equal entryBytes */
    LAST_VALID,
    /** comparable type indicating this Entry should always compare greater
        than valid RowIds */
    ALWAYS_LAST;
  }

  public static final Comparator<byte[]> BYTE_CODE_COMPARATOR =
    new Comparator<byte[]>() {
      @Override
      public int compare(byte[] left, byte[] right) {
        if(left == right) {
          return 0;
        }
        if(left == null) {
          return -1;
        }
        if(right == null) {
          return 1;
        }

        int len = Math.min(left.length, right.length);
        int pos = 0;
        while((pos < len) && (left[pos] == right[pos])) {
          ++pos;
        }
        if(pos < len) {
          return ((ByteUtil.asUnsignedByte(left[pos]) <
                   ByteUtil.asUnsignedByte(right[pos])) ? -1 : 1);
        }
        return ((left.length < right.length) ? -1 :
                ((left.length > right.length) ? 1 : 0));
      }
    };


  /** name, generated on demand */
  private String _name;
  /** owning table */
  private final TableImpl _table;
  /** 0-based index data number */
  private final int _number;
  /** Page number of the root index data */
  private int _rootPageNumber;
  /** offset within the tableDefinition buffer of the uniqueEntryCount for
      this index */
  private final int _uniqueEntryCountOffset;
  /** The number of unique entries which have been added to this index.  note,
      however, that it is never decremented, only incremented (as observed in
      Access). */
  private int _uniqueEntryCount;
  /** List of columns and flags */
  private final List<ColumnDescriptor> _columns =
    new ArrayList<ColumnDescriptor>();
  /** the logical indexes which this index data backs */
  private final List<Index> _indexes = new ArrayList<Index>();
  /** flags for this index */
  private byte _indexFlags;
  /** Usage map of pages that this index owns */
  private UsageMap _ownedPages;
  /** <code>true</code> if the index entries have been initialized,
      <code>false</code> otherwise */
  private boolean _initialized;
  /** modification count for the table, keeps cursors up-to-date */
  private int _modCount;
  /** temp buffer used to read/write the index pages */
  private final TempBufferHolder _indexBufferH =
    TempBufferHolder.newHolder(TempBufferHolder.Type.SOFT, true);
  /** temp buffer used to create index entries */
  private ByteStream _entryBuffer;
  /** max size for all the entries written to a given index data page */
  private final int _maxPageEntrySize;
  /** whether or not this index data is backing a primary key logical index */
  private boolean _primaryKey;
  /** if non-null, the reason why we cannot create entries for this index */
  private String _unsupportedReason;
  /** Cache which manages the index pages */
  private final IndexPageCache _pageCache;

  protected IndexData(TableImpl table, int number, int uniqueEntryCount,
                      int uniqueEntryCountOffset)
  {
    _table  = table;
    _number = number;
    _uniqueEntryCount = uniqueEntryCount;
    _uniqueEntryCountOffset = uniqueEntryCountOffset;
    _maxPageEntrySize = calcMaxPageEntrySize(_table.getFormat());
    _pageCache = new IndexPageCache(this);
  }

  /**
   * Creates an IndexData appropriate for the given table, using information
   * from the given table definition buffer.
   */
  public static IndexData create(TableImpl table, ByteBuffer tableBuffer,
                                 int number, JetFormat format)
    throws IOException
  {
    int uniqueEntryCountOffset =
      (format.OFFSET_INDEX_DEF_BLOCK +
       (number * format.SIZE_INDEX_DEFINITION) + 4);
    int uniqueEntryCount = tableBuffer.getInt(uniqueEntryCountOffset);

    return new IndexData(table, number, uniqueEntryCount, uniqueEntryCountOffset);
  }

  public String getName() {
    if(_name == null) {
      if(_indexes.size() == 1) {
        _name = _indexes.get(0).getName();
      } else if(!_indexes.isEmpty()) {
        List<String> names = new ArrayList<String>(_indexes.size());
        for(Index idx : _indexes) {
          names.add(idx.getName());
        }
        _name = names.toString();
      } else {
        _name = String.valueOf(_number);
      }
    }
    return _name;
  }

  public TableImpl getTable() {
    return _table;
  }

  public JetFormat getFormat() {
    return getTable().getFormat();
  }

  public PageChannel getPageChannel() {
    return getTable().getPageChannel();
  }

  /**
   * @return the "main" logical index which is backed by this data.
   */
  public Index getPrimaryIndex() {
    return _indexes.get(0);
  }

  /**
   * @return All of the Indexes backed by this data (unmodifiable List)
   */
  public List<Index> getIndexes() {
    return Collections.unmodifiableList(_indexes);
  }

  /**
   * Adds a logical index which this data is backing.
   */
  void addIndex(Index index) {

    // we keep foreign key indexes at the back of the list.  this way the
    // primary index will be a non-foreign key index (if any)
    if(index.isForeignKey()) {
      _indexes.add(index);
    } else {
      int pos = _indexes.size();
      while(pos > 0) {
        if(!_indexes.get(pos - 1).isForeignKey()) {
          break;
        }
        --pos;
      }
      _indexes.add(pos, index);

      // also, keep track of whether or not this is a primary key index
      _primaryKey |= index.isPrimaryKey();
    }

    // force name to be regenerated
    _name = null;
  }

  public byte getIndexFlags() {
    return _indexFlags;
  }

  public int getIndexDataNumber() {
    return _number;
  }

  public int getUniqueEntryCount() {
    return _uniqueEntryCount;
  }

  public int getUniqueEntryCountOffset() {
    return _uniqueEntryCountOffset;
  }

  protected boolean isBackingPrimaryKey() {
    return _primaryKey;
  }

  /**
   * Whether or not {@code null} values are actually recorded in the index.
   */
  public boolean shouldIgnoreNulls() {
    return((_indexFlags & IGNORE_NULLS_INDEX_FLAG) != 0);
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
    return(isBackingPrimaryKey() || ((_indexFlags & UNIQUE_INDEX_FLAG) != 0));
  }

  /**
   * Whether or not values are required in the columns.
   */
  public boolean isRequired() {
    return((_indexFlags & REQUIRED_INDEX_FLAG) != 0);
  }

  /**
   * Returns the Columns for this index (unmodifiable)
   */
  public List<ColumnDescriptor> getColumns() {
    return Collections.unmodifiableList(_columns);
  }

  public int getColumnCount() {
    return _columns.size();
  }

  /**
   * Whether or not the complete index state has been read.
   */
  public boolean isInitialized() {
    return _initialized;
  }

  protected int getRootPageNumber() {
    return _rootPageNumber;
  }

  private void setUnsupportedReason(String reason, ColumnImpl col) {
    _unsupportedReason = withErrorContext(reason);
    if(!col.getTable().isSystem()) {
      LOG.warn(_unsupportedReason + ", making read-only");
    } else {
      if(LOG.isDebugEnabled()) {
        LOG.debug(_unsupportedReason + ", making read-only");
      }
    }
  }

  String getUnsupportedReason() {
    return _unsupportedReason;
  }

  protected int getMaxPageEntrySize() {
    return _maxPageEntrySize;
  }

  /**
   * Returns the number of database pages owned by this index data.
   * @usage _intermediate_method_
   */
  public int getOwnedPageCount() {
    return _ownedPages.getPageCount();
  }

  void addOwnedPage(int pageNumber) throws IOException {
    _ownedPages.addPageNumber(pageNumber);
  }

  void collectUsageMapPages(Collection<Integer> pages) {
    pages.add(_ownedPages.getTablePageNumber());
  }

  /**
   * Used by unit tests to validate the internal status of the index.
   * @param forceLoad if {@code false} only validate currently loaded index
   *                  data pages, otherwise, load and validate all index pages
   * @usage _advanced_method_
   */
  public void validate(boolean forceLoad) throws IOException {
    _pageCache.validate(forceLoad);
  }

  /**
   * Returns the number of index entries in the index.  Only called by unit
   * tests.
   * <p>
   * Forces index initialization.
   * @usage _advanced_method_
   */
  public int getEntryCount()
    throws IOException
  {
    initialize();
    EntryCursor cursor = cursor();
    Entry endEntry = cursor.getLastEntry();
    int count = 0;
    while(!endEntry.equals(cursor.getNextEntry())) {
      ++count;
    }
    return count;
  }

  /**
   * Forces initialization of this index (actual parsing of index pages).
   * normally, the index will not be initialized until the entries are
   * actually needed.
   */
  public void initialize() throws IOException {
    if(!_initialized) {
      _pageCache.setRootPageNumber(getRootPageNumber());
      _initialized = true;
    }
  }

  /**
   * Writes the current index state to the database.
   * <p>
   * Forces index initialization.
   */
  public void update() throws IOException
  {
    // make sure we've parsed the entries
    initialize();

    if(_unsupportedReason != null) {
      throw new UnsupportedOperationException(
          "Cannot write indexes of this type due to " + _unsupportedReason);
    }
    _pageCache.write();
  }

  /**
   * Read the rest of the index info from a tableBuffer
   * @param tableBuffer table definition buffer to read from initial info
   * @param availableColumns Columns that this index may use
   */
  public void read(ByteBuffer tableBuffer, List<ColumnImpl> availableColumns)
    throws IOException
  {
    ByteUtil.forward(tableBuffer, getFormat().SKIP_BEFORE_INDEX); //Forward past Unknown

    for (int i = 0; i < MAX_COLUMNS; i++) {
      short columnNumber = tableBuffer.getShort();
      byte colFlags = tableBuffer.get();
      if (columnNumber != COLUMN_UNUSED) {
        // find the desired column by column number (which is not necessarily
        // the same as the column index)
        ColumnImpl idxCol = null;
        for(ColumnImpl col : availableColumns) {
          if(col.getColumnNumber() == columnNumber) {
            idxCol = col;
            break;
          }
        }
        if(idxCol == null) {
          throw new IOException(withErrorContext(
                  "Could not find column with number "
                  + columnNumber + " for index"));
        }
        _columns.add(newColumnDescriptor(idxCol, colFlags));
      }
    }

    _ownedPages = UsageMap.read(getTable().getDatabase(), tableBuffer);

    _rootPageNumber = tableBuffer.getInt();

    ByteUtil.forward(tableBuffer, getFormat().SKIP_BEFORE_INDEX_FLAGS); //Forward past Unknown
    _indexFlags = tableBuffer.get();
    ByteUtil.forward(tableBuffer, getFormat().SKIP_AFTER_INDEX_FLAGS); //Forward past other stuff
  }

  /**
   * Writes the index row count definitions into a table definition buffer.
   * @param creator description of the indexes to write
   * @param buffer Buffer to write to
   */
  protected static void writeRowCountDefinitions(
      TableCreator creator, ByteBuffer buffer)
  {
    writeRowCountDefinitions(creator, buffer, creator.getIndexCount());
  }

  /**
   * Writes the index row count definitions into a table definition buffer.
   * @param creator description of the indexes to write
   * @param buffer Buffer to write to
   * @param idxCount num indexes to write
   */
  protected static void writeRowCountDefinitions(
      TableMutator creator, ByteBuffer buffer, int idxCount)
  {
    // index row counts (empty data)
    ByteUtil.forward(buffer, (idxCount *
                              creator.getFormat().SIZE_INDEX_DEFINITION));
  }

  /**
   * Writes the index definitions into a table definition buffer.
   * @param creator description of the indexes to write
   * @param buffer Buffer to write to
   */
  protected static void writeDefinitions(
      TableCreator creator, ByteBuffer buffer)
    throws IOException
  {
    ByteBuffer rootPageBuffer = createRootPageBuffer(creator);

    for(TableMutator.IndexDataState idxDataState : creator.getIndexDataStates()) {
      writeDefinition(creator, buffer, idxDataState, rootPageBuffer);
    }
  }

  /**
   * Writes the index definitions into a table definition buffer.
   * @param creator description of the indexes to write
   * @param buffer Buffer to write to
   */
  @SuppressWarnings("resource")
  protected static void writeDefinition(
      TableMutator creator, ByteBuffer buffer,
      TableMutator.IndexDataState idxDataState, ByteBuffer rootPageBuffer)
    throws IOException
  {
    if(rootPageBuffer == null) {
      rootPageBuffer = createRootPageBuffer(creator);
    }

    buffer.putInt(MAGIC_INDEX_NUMBER); // seemingly constant magic value

    // write column information (always MAX_COLUMNS entries)
    IndexBuilder idx = idxDataState.getFirstIndex();
    List<IndexBuilder.Column> idxColumns = idx.getColumns();
    for(int i = 0; i < MAX_COLUMNS; ++i) {

      short columnNumber = COLUMN_UNUSED;
      byte flags = 0;

      if(i < idxColumns.size()) {

        // determine column info
        IndexBuilder.Column idxCol = idxColumns.get(i);
        flags = idxCol.getFlags();

        // find actual table column number
        columnNumber = creator.getColumnNumber(idxCol.getName());
        if(columnNumber == COLUMN_UNUSED) {
          // should never happen as this is validated before
          throw new IllegalArgumentException(
              withErrorContext(
                  "Column with name " + idxCol.getName() + " not found",
                  creator.getDatabase(), creator.getTableName(), idx.getName()));
        }
      }

      buffer.putShort(columnNumber); // table column number
      buffer.put(flags); // column flags (e.g. ordering)
    }

    buffer.put(idxDataState.getUmapRowNumber()); // umap row
    ByteUtil.put3ByteInt(buffer, idxDataState.getUmapPageNumber()); // umap page

    // write empty root index page
    creator.getPageChannel().writePage(rootPageBuffer,
                                       idxDataState.getRootPageNumber());

    buffer.putInt(idxDataState.getRootPageNumber());
    buffer.putInt(0); // unknown
    buffer.put(idx.getFlags()); // index flags (unique, etc.)
    ByteUtil.forward(buffer, 5); // unknown
  }

  private static ByteBuffer createRootPageBuffer(TableMutator creator)
    throws IOException
  {
    ByteBuffer rootPageBuffer = creator.getPageChannel().createPageBuffer();
    writeDataPage(rootPageBuffer, NEW_ROOT_DATA_PAGE,
                  creator.getTdefPageNumber(), creator.getFormat());
    return rootPageBuffer;
  }

  /**
   * Prepares to add a row to this index.  All constraints are checked before
   * this method returns.
   * <p>
   * Forces index initialization.
   *
   * @param row Row to add
   * @param rowId rowId of the row to be added
   *
   * @return a PendingChange which can complete the addition or roll it back
   */
  public PendingChange prepareAddRow(Object[] row, RowIdImpl rowId,
                                     PendingChange nextChange)
    throws IOException
  {
    return prepareAddRow(row, rowId, new AddRowPendingChange(nextChange));
  }

  private PendingChange prepareAddRow(Object[] row, RowIdImpl rowId,
                                      AddRowPendingChange change)
    throws IOException
  {
    int nullCount = countNullValues(row);
    boolean isNullEntry = (nullCount == _columns.size());
    if(shouldIgnoreNulls() && isNullEntry) {
      // nothing to do
      return change;
    }
    if((nullCount > 0) && (isBackingPrimaryKey() || isRequired())) {
      throw new ConstraintViolationException(withErrorContext(
          "Null value found in row " + Arrays.asList(row) +
          " for primary key or required index"));
    }

    // make sure we've parsed the entries
    initialize();

    return prepareAddEntry(new Entry(createEntryBytes(row), rowId), isNullEntry,
                           row, change);
  }

  /**
   * Adds an entry to the correct index dataPage, maintaining the order.
   */
  private PendingChange prepareAddEntry(Entry newEntry, boolean isNullEntry,
                                        Object[] row, AddRowPendingChange change)
    throws IOException
  {
    DataPage dataPage = findDataPage(newEntry);
    int idx = dataPage.findEntry(newEntry);
    if(idx < 0) {

      // this is a new entry
      idx = missingIndexToInsertionPoint(idx);

      Position newPos = new Position(dataPage, idx, newEntry, true);
      Position nextPos = getNextPosition(newPos);
      Position prevPos = getPreviousPosition(newPos);

      // determine if the addition of this entry would break the uniqueness
      // constraint.  See isUnique() for some notes about uniqueness as
      // defined by Access.
      boolean isDupeEntry =
        (((nextPos != null) &&
          newEntry.equalsEntryBytes(nextPos.getEntry())) ||
          ((prevPos != null) &&
           newEntry.equalsEntryBytes(prevPos.getEntry())));
      if(isUnique() && !isNullEntry && isDupeEntry) {
        throw new ConstraintViolationException(withErrorContext(
            "New row " + Arrays.asList(row) +
            " violates uniqueness constraint for index"));
      }

      change.setAddRow(newEntry, dataPage, idx, isDupeEntry);

    } else {

      change.setOldRow(newEntry);
    }
    return change;
  }

  /**
   * Completes a prepared row addition.
   */
  private void commitAddRow(Entry newEntry, DataPage dataPage, int idx,
                            boolean isDupeEntry, Entry oldEntry)
    throws IOException
  {
    if(newEntry != null) {
      dataPage.addEntry(idx, newEntry);
      // if we are adding a duplicate entry, or replacing an existing entry,
      // then the unique entry count doesn't change
      if(!isDupeEntry && (oldEntry == null)) {
        ++_uniqueEntryCount;
      }
      ++_modCount;
    } else {
      LOG.warn(withErrorContext("Added duplicate index entry " + oldEntry));
    }
  }

  /**
   * Prepares to update a row in this index.  All constraints are checked
   * before this method returns.
   * <p>
   * Forces index initialization.
   *
   * @param oldRow Row to be removed
   * @param newRow Row to be added
   * @param rowId rowId of the row to be updated
   *
   * @return a PendingChange which can complete the update or roll it back
   */
  public PendingChange prepareUpdateRow(Object[] oldRow, RowIdImpl rowId,
                                        Object[] newRow,
                                        PendingChange nextChange)
    throws IOException
  {
    UpdateRowPendingChange change = new UpdateRowPendingChange(nextChange);
    change.setOldRow(deleteRowImpl(oldRow, rowId));

    try {
      prepareAddRow(newRow, rowId, change);
      return change;
    } catch(ConstraintViolationException e) {
      // need to undo the deletion before bailing
      change.rollback();
      throw e;
    }
  }

  /**
   * Removes a row from this index
   * <p>
   * Forces index initialization.
   *
   * @param row Row to remove
   * @param rowId rowId of the row to be removed
   */
  public void deleteRow(Object[] row, RowIdImpl rowId)
    throws IOException
  {
    deleteRowImpl(row, rowId);
  }

  private Entry deleteRowImpl(Object[] row, RowIdImpl rowId)
    throws IOException
  {
    int nullCount = countNullValues(row);
    if(shouldIgnoreNulls() && (nullCount == _columns.size())) {
      // nothing to do
      return null;
    }

    // make sure we've parsed the entries
    initialize();

    Entry oldEntry = new Entry(createEntryBytes(row), rowId);
    Entry removedEntry = removeEntry(oldEntry);
    if(removedEntry != null) {
      ++_modCount;
    } else {
      LOG.warn(withErrorContext(
          "Failed removing index entry " + oldEntry + " for row: " +
          Arrays.asList(row)));
    }
    return removedEntry;
  }

  /**
   * Undoes a previous row deletion.
   */
  private void rollbackDeletedRow(Entry removedEntry)
    throws IOException
  {
    if(removedEntry == null) {
      // no change was made
      return;
    }

    // unfortunately, stuff might have shuffled around when we first removed
    // the row, so in order to re-insert it, we need to re-find and insert it.
    DataPage dataPage = findDataPage(removedEntry);
    int idx = dataPage.findEntry(removedEntry);
    if(idx < 0) {
      dataPage.addEntry(missingIndexToInsertionPoint(idx), removedEntry);
    }
  }

  /**
   * Removes an entry from the relevant index dataPage, maintaining the order.
   * Will search by RowId if entry is not found (in case a partial entry was
   * provided).
   */
  private Entry removeEntry(Entry oldEntry)
    throws IOException
  {
    DataPage dataPage = findDataPage(oldEntry);
    int idx = dataPage.findEntry(oldEntry);
    boolean doRemove = false;
    if(idx < 0) {
      // the caller may have only read some of the row data, if this is the
      // case, just search for the page/row numbers
      // TODO, we could force caller to get relevant values?
      EntryCursor cursor = cursor();
      Position tmpPos = null;
      Position endPos = cursor._lastPos;
      while(!endPos.equals(
                tmpPos = cursor.getAnotherPosition(CursorImpl.MOVE_FORWARD))) {
        if(tmpPos.getEntry().getRowId().equals(oldEntry.getRowId())) {
          dataPage = tmpPos.getDataPage();
          idx = tmpPos.getIndex();
          doRemove = true;
          break;
        }
      }
    } else {
      doRemove = true;
    }

    Entry removedEntry = null;
    if(doRemove) {
      // found it!
      removedEntry = dataPage.removeEntry(idx);
    }

    return removedEntry;
  }

  public static void commitAll(PendingChange change) throws IOException {
    while(change != null) {
      change.commit();
      change = change.getNext();
    }
  }

  public static void rollbackAll(PendingChange change) throws IOException {
    while(change != null) {
      change.rollback();
      change = change.getNext();
    }
  }

  /**
   * Gets a new cursor for this index.
   * <p>
   * Forces index initialization.
   */
  public EntryCursor cursor()
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
  public EntryCursor cursor(Object[] startRow,
                            boolean startInclusive,
                            Object[] endRow,
                            boolean endInclusive)
    throws IOException
  {
    initialize();
    Entry startEntry = FIRST_ENTRY;
    byte[] startEntryBytes = null;
    if(startRow != null) {
      startEntryBytes = createEntryBytes(startRow);
      startEntry = new Entry(startEntryBytes,
                             (startInclusive ?
                              RowIdImpl.FIRST_ROW_ID : RowIdImpl.LAST_ROW_ID));
    }
    Entry endEntry = LAST_ENTRY;
    if(endRow != null) {
      // reuse startEntryBytes if startRow and endRow are same array.  this is
      // common for "lookup" code
      byte[] endEntryBytes = ((startRow == endRow) ?
                              startEntryBytes :
                              createEntryBytes(endRow));
      endEntry = new Entry(endEntryBytes,
                           (endInclusive ?
                            RowIdImpl.LAST_ROW_ID : RowIdImpl.FIRST_ROW_ID));
    }
    return new EntryCursor(findEntryPosition(startEntry),
                           findEntryPosition(endEntry));
  }

  private Position findEntryPosition(Entry entry)
    throws IOException
  {
    DataPage dataPage = findDataPage(entry);
    int idx = dataPage.findEntry(entry);
    boolean between = false;
    if(idx < 0) {
      // given entry was not found exactly.  our current position is now
      // really between two indexes, but we cannot support that as an integer
      // value, so we set a flag instead
      idx = missingIndexToInsertionPoint(idx);
      between = true;
    }
    return new Position(dataPage, idx, entry, between);
  }

  private Position getNextPosition(Position curPos)
    throws IOException
  {
    // get the next index (between-ness is handled internally)
    int nextIdx = curPos.getNextIndex();
    Position nextPos = null;
    if(nextIdx < curPos.getDataPage().getEntries().size()) {
      nextPos = new Position(curPos.getDataPage(), nextIdx);
    } else {
      int nextPageNumber = curPos.getDataPage().getNextPageNumber();
      DataPage nextDataPage = null;
      while(nextPageNumber != INVALID_INDEX_PAGE_NUMBER) {
        DataPage dp = getDataPage(nextPageNumber);
        if(!dp.isEmpty()) {
          nextDataPage = dp;
          break;
        }
        nextPageNumber = dp.getNextPageNumber();
      }
      if(nextDataPage != null) {
        nextPos = new Position(nextDataPage, 0);
      }
    }
    return nextPos;
  }

  /**
   * Returns the Position before the given one, or {@code null} if none.
   */
  private Position getPreviousPosition(Position curPos)
    throws IOException
  {
    // get the previous index (between-ness is handled internally)
    int prevIdx = curPos.getPrevIndex();
    Position prevPos = null;
    if(prevIdx >= 0) {
      prevPos = new Position(curPos.getDataPage(), prevIdx);
    } else {
      int prevPageNumber = curPos.getDataPage().getPrevPageNumber();
      DataPage prevDataPage = null;
      while(prevPageNumber != INVALID_INDEX_PAGE_NUMBER) {
        DataPage dp = getDataPage(prevPageNumber);
        if(!dp.isEmpty()) {
          prevDataPage = dp;
          break;
        }
        prevPageNumber = dp.getPrevPageNumber();
      }
      if(prevDataPage != null) {
        prevPos = new Position(prevDataPage,
                               (prevDataPage.getEntries().size() - 1));
      }
    }
    return prevPos;
  }

  /**
   * Returns the valid insertion point for an index indicating a missing
   * entry.
   */
  protected static int missingIndexToInsertionPoint(int idx) {
    return -(idx + 1);
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
    if(values.length != _columns.size()) {
      throw new IllegalArgumentException(withErrorContext(
          "Wrong number of column values given " + values.length +
          ", expected " + _columns.size()));
    }
    int valIdx = 0;
    Object[] idxRow = new Object[getTable().getColumnCount()];
    for(ColumnDescriptor col : _columns) {
      idxRow[col.getColumnIndex()] = values[valIdx++];
    }
    return idxRow;
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
    if(values.length == 0) {
      throw new IllegalArgumentException(withErrorContext(
          "At least one column value must be provided"));
    }
    if(values.length > _columns.size()) {
      throw new IllegalArgumentException(withErrorContext(
          "Too many column values given " + values.length +
          ", expected at most " + _columns.size()));
    }
    int valIdx = 0;
    Object[] idxRow = new Object[getTable().getColumnCount()];
    for(ColumnDescriptor col : _columns) {
      idxRow[col.getColumnIndex()] =
        ((valIdx < values.length) ? values[valIdx] : filler);
      ++valIdx;
    }
    return idxRow;
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
    for(ColumnDescriptor col : _columns) {
      if(!row.containsKey(col.getName())) {
        return null;
      }
    }

    Object[] idxRow = new Object[getTable().getColumnCount()];
    for(ColumnDescriptor col : _columns) {
      idxRow[col.getColumnIndex()] = row.get(col.getName());
    }
    return idxRow;
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
    // see if we have at least one prefix column
    int numCols = 0;
    for(ColumnDescriptor col : _columns) {
      if(!row.containsKey(col.getName())) {
        if(numCols == 0) {
          // can't do it, need at least first column
          return null;
        }
        break;
      }
      ++numCols;
    }

    // fill in the row with either the prefix values or the filler value, as
    // appropriate
    Object[] idxRow = new Object[getTable().getColumnCount()];
    int valIdx = 0;
    for(ColumnDescriptor col : _columns) {
      idxRow[col.getColumnIndex()] =
        ((valIdx < numCols) ? row.get(col.getName()) : filler);
      ++valIdx;
    }
    return idxRow;
  }

  @Override
  public String toString() {
    ToStringBuilder sb = CustomToStringStyle.builder(this)
      .append("dataNumber", _number)
      .append("pageNumber", _rootPageNumber)
      .append("isBackingPrimaryKey", isBackingPrimaryKey())
      .append("isUnique", isUnique())
      .append("ignoreNulls", shouldIgnoreNulls())
      .append("isRequired", isRequired())
      .append("columns", _columns)
      .append("initialized", _initialized);
    if(_initialized) {
      try {
        sb.append("entryCount", getEntryCount());
      } catch(IOException e) {
        throw new RuntimeIOException(e);
      }
    }
    sb.append("pageCache", _pageCache);
    return sb.toString();
  }

  /**
   * Write the given index page out to a buffer
   */
  protected void writeDataPage(DataPage dataPage)
    throws IOException
  {
    if(dataPage.getCompressedEntrySize() > _maxPageEntrySize) {
      throw new IllegalStateException(withErrorContext("data page is too large"));
    }

    ByteBuffer buffer = _indexBufferH.getPageBuffer(getPageChannel());

    writeDataPage(buffer, dataPage, getTable().getTableDefPageNumber(),
                  getFormat());

    getPageChannel().writePage(buffer, dataPage.getPageNumber());
  }

  /**
   * Writes the data page info to the given buffer.
   */
  protected static void writeDataPage(ByteBuffer buffer, DataPage dataPage,
                                      int tdefPageNumber, JetFormat format)
    throws IOException
  {
    buffer.put(dataPage.isLeaf() ?
               PageTypes.INDEX_LEAF :
               PageTypes.INDEX_NODE );  //Page type
    buffer.put((byte) 0x01);  //Unknown
    buffer.putShort((short) 0); //Free space
    buffer.putInt(tdefPageNumber);

    buffer.putInt(0); //Unknown
    buffer.putInt(dataPage.getPrevPageNumber()); //Prev page
    buffer.putInt(dataPage.getNextPageNumber()); //Next page
    buffer.putInt(dataPage.getChildTailPageNumber()); //ChildTail page

    byte[] entryPrefix = dataPage.getEntryPrefix();
    buffer.putShort((short) entryPrefix.length); // entry prefix byte count
    buffer.put((byte) 0); //Unknown

    byte[] entryMask = new byte[format.SIZE_INDEX_ENTRY_MASK];
    // first entry includes the prefix
    int totalSize = entryPrefix.length;
    for(Entry entry : dataPage.getEntries()) {
      totalSize += (entry.size() - entryPrefix.length);
      int idx = totalSize / 8;
      entryMask[idx] |= (1 << (totalSize % 8));
    }
    buffer.put(entryMask);

    // first entry includes the prefix
    buffer.put(entryPrefix);

    for(Entry entry : dataPage.getEntries()) {
      entry.write(buffer, entryPrefix);
    }

    // update free space
    buffer.putShort(2, (short) (format.PAGE_SIZE - buffer.position()));
  }

  /**
   * Reads an index page, populating the correct collection based on the page
   * type (node or leaf).
   */
  protected void readDataPage(DataPage dataPage)
    throws IOException
  {
    ByteBuffer buffer = _indexBufferH.getPageBuffer(getPageChannel());
    getPageChannel().readPage(buffer, dataPage.getPageNumber());

    boolean isLeaf = isLeafPage(buffer);
    dataPage.setLeaf(isLeaf);

    // note, "header" data is in LITTLE_ENDIAN format, entry data is in
    // BIG_ENDIAN format
    int entryPrefixLength = ByteUtil.getUnsignedShort(
        buffer, getFormat().OFFSET_INDEX_COMPRESSED_BYTE_COUNT);
    int entryMaskLength = getFormat().SIZE_INDEX_ENTRY_MASK;
    int entryMaskPos = getFormat().OFFSET_INDEX_ENTRY_MASK;
    int entryPos = entryMaskPos + entryMaskLength;
    int lastStart = 0;
    int totalEntrySize = 0;
    byte[] entryPrefix = null;
    List<Entry> entries = new ArrayList<Entry>();
    TempBufferHolder tmpEntryBufferH =
      TempBufferHolder.newHolder(TempBufferHolder.Type.HARD, true,
                                 ENTRY_BYTE_ORDER);

    Entry prevEntry = FIRST_ENTRY;
    for (int i = 0; i < entryMaskLength; i++) {
      byte entryMask = buffer.get(entryMaskPos + i);
      for (int j = 0; j < 8; j++) {
        if ((entryMask & (1 << j)) != 0) {
          int length = (i * 8) + j - lastStart;
          buffer.position(entryPos + lastStart);

          // determine if we can read straight from the index page (if no
          // entryPrefix).  otherwise, create temp buf with complete entry.
          ByteBuffer curEntryBuffer = buffer;
          int curEntryLen = length;
          if(entryPrefix != null) {
            curEntryBuffer = getTempEntryBuffer(
                buffer, length, entryPrefix, tmpEntryBufferH);
            curEntryLen += entryPrefix.length;
          }
          totalEntrySize += curEntryLen;

          Entry entry = newEntry(curEntryBuffer, curEntryLen, isLeaf);
          if(prevEntry.compareTo(entry) >= 0) {
            throw new IOException(withErrorContext(
                    "Unexpected order in index entries, " +
                    prevEntry + " >= " + entry));
          }

          entries.add(entry);

          if((entries.size() == 1) && (entryPrefixLength > 0)) {
            // read any shared entry prefix
            entryPrefix = new byte[entryPrefixLength];
            buffer.position(entryPos + lastStart);
            buffer.get(entryPrefix);
          }

          lastStart += length;
          prevEntry = entry;
        }
      }
    }

    dataPage.setEntryPrefix(entryPrefix != null ? entryPrefix : EMPTY_PREFIX);
    dataPage.setEntries(entries);
    dataPage.setTotalEntrySize(totalEntrySize);

    int prevPageNumber = buffer.getInt(getFormat().OFFSET_PREV_INDEX_PAGE);
    int nextPageNumber = buffer.getInt(getFormat().OFFSET_NEXT_INDEX_PAGE);
    int childTailPageNumber =
      buffer.getInt(getFormat().OFFSET_CHILD_TAIL_INDEX_PAGE);

    dataPage.setPrevPageNumber(prevPageNumber);
    dataPage.setNextPageNumber(nextPageNumber);
    dataPage.setChildTailPageNumber(childTailPageNumber);
  }

  /**
   * Returns a new Entry of the correct type for the given data and page type.
   */
  private static Entry newEntry(ByteBuffer buffer, int entryLength,
                                boolean isLeaf)
    throws IOException
  {
    if(isLeaf) {
      return new Entry(buffer, entryLength);
    }
    return new NodeEntry(buffer, entryLength);
  }

  /**
   * Returns an entry buffer containing the relevant data for an entry given
   * the valuePrefix.
   */
  private ByteBuffer getTempEntryBuffer(
      ByteBuffer indexPage, int entryLen, byte[] valuePrefix,
      TempBufferHolder tmpEntryBufferH)
  {
    ByteBuffer tmpEntryBuffer = tmpEntryBufferH.getBuffer(
        getPageChannel(), valuePrefix.length + entryLen);

    // combine valuePrefix and rest of entry from indexPage, then prep for
    // reading
    tmpEntryBuffer.put(valuePrefix);
    tmpEntryBuffer.put(indexPage.array(), indexPage.position(), entryLen);
    tmpEntryBuffer.flip();

    return tmpEntryBuffer;
  }

  /**
   * Determines if the given index page is a leaf or node page.
   */
  private boolean isLeafPage(ByteBuffer buffer)
    throws IOException
  {
    byte pageType = buffer.get(0);
    if(pageType == PageTypes.INDEX_LEAF) {
      return true;
    } else if(pageType == PageTypes.INDEX_NODE) {
      return false;
    }
    throw new IOException(withErrorContext("Unexpected page type " + pageType));
  }

  /**
   * Determines the number of {@code null} values for this index from the
   * given row.
   */
  private int countNullValues(Object[] values)
  {
    if(values == null) {
      return _columns.size();
    }

    // annoyingly, the values array could come from different sources, one
    // of which will make it a different size than the other.  we need to
    // handle both situations.
    int nullCount = 0;
    for(ColumnDescriptor col : _columns) {
      Object value = values[col.getColumnIndex()];
      if(col.isNullValue(value)) {
        ++nullCount;
      }
    }

    return nullCount;
  }

  /**
   * Creates the entry bytes for a row of values.
   */
  private byte[] createEntryBytes(Object[] values) throws IOException
  {
    if(values == null) {
      return null;
    }

    if(_entryBuffer == null) {
      _entryBuffer = new ByteStream();
    }
    _entryBuffer.reset();

    for(ColumnDescriptor col : _columns) {

      Object value = values[col.getColumnIndex()];
      if(ColumnImpl.isRawData(value)) {
        // ignore it, we could not parse it
        continue;
      }

      if(value == MIN_VALUE) {
        // null is the "least" value (note the column "ascending" flag is
        // irrelevant here because the entry bytes are _always_ interpreted
        // least to greatest)
        _entryBuffer.write(getNullEntryFlag(true));
        continue;
      }
      if(value == MAX_VALUE) {
        // the opposite null is the "greatest" value (note the column
        // "ascending" flag is irrelevant here because the entry bytes are
        // _always_ interpreted least to greatest)
        _entryBuffer.write(getNullEntryFlag(false));
        continue;
      }

      col.writeValue(value, _entryBuffer);
    }

    return _entryBuffer.toByteArray();
  }

  /**
   * Finds the data page for the given entry.
   */
  protected DataPage findDataPage(Entry entry)
    throws IOException
  {
    return _pageCache.findCacheDataPage(entry);
  }

  /**
   * Gets the data page for the pageNumber.
   */
  protected DataPage getDataPage(int pageNumber)
    throws IOException
  {
    return _pageCache.getCacheDataPage(pageNumber);
  }

  /**
   * Flips the first bit in the byte at the given index.
   */
  private static byte[] flipFirstBitInByte(byte[] value, int index)
  {
    value[index] = (byte)(value[index] ^ 0x80);

    return value;
  }

  /**
   * Flips all the bits in the byte array.
   */
  private static byte[] flipBytes(byte[] value) {
    return flipBytes(value, 0, value.length);
  }

  /**
   * Flips the bits in the specified bytes in the byte array.
   */
  static byte[] flipBytes(byte[] value, int offset, int length) {
    for(int i = offset; i < (offset + length); ++i) {
      value[i] = (byte)(~value[i]);
    }
    return value;
  }

  /**
   * Writes the value of the given column type to a byte array and returns it.
   */
  private static byte[] encodeNumberColumnValue(Object value, ColumnImpl column)
    throws IOException
  {
    // always write in big endian order
    return column.write(value, 0, ENTRY_BYTE_ORDER).array();
  }

  /**
   * Writes a binary value using the general binary entry encoding rules.
   */
  private static void writeGeneralBinaryEntry(byte[] valueBytes, boolean isAsc,
                                              ByteStream bout)
  {
    int dataLen = valueBytes.length;
    int extraLen = (dataLen + 7) / 8;
    int entryLen = ((dataLen + extraLen + 8) / 9) * 9;

    // reserve space for the full entry
    bout.ensureNewCapacity(entryLen);

    // binary data is written in 8 byte segments with a trailing length byte.
    // The length byte is the amount of valid bytes in the segment (where 9
    // indicates that there is more data _after_ this segment).
    byte[] partialEntryBytes = new byte[9];

    // bit twiddling rules:
    // - isAsc  => nothing
    // - !isAsc => flipBytes, _but keep intermediate 09 unflipped_!

    // first, write any intermediate segements
    int segmentLen = dataLen;
    int pos = 0;
    while(segmentLen > 8) {

      System.arraycopy(valueBytes, pos, partialEntryBytes, 0, 8);
      if(!isAsc) {
        // note, we do _not_ flip the length byte for intermediate segments
        flipBytes(partialEntryBytes, 0, 8);
      }

      // we are writing intermediate segments (there is more data after this
      // segment), so the length is always 9.
      partialEntryBytes[8] = (byte)9;

      pos += 8;
      segmentLen -= 8;

      bout.write(partialEntryBytes);
    }

    // write the last segment (with slightly different rules)
    if(segmentLen > 0) {

      System.arraycopy(valueBytes, pos, partialEntryBytes, 0, segmentLen);

      // clear out an intermediate bytes between the real data and the final
      // length byte
      for(int i = segmentLen; i < 8; ++i) {
        partialEntryBytes[i] = 0;
      }

      partialEntryBytes[8] = (byte)segmentLen;

      if(!isAsc) {
        // note, we _do_ flip the last length byte
        flipBytes(partialEntryBytes, 0, 9);
      }

      bout.write(partialEntryBytes);
    }
  }

  /**
   * Creates one of the special index entries.
   */
  private static Entry createSpecialEntry(RowIdImpl rowId) {
    return new Entry((byte[])null, rowId);
  }

  /**
   * Constructs a ColumnDescriptor of the relevant type for the given Column.
   */
  private ColumnDescriptor newColumnDescriptor(ColumnImpl col, byte flags)
    throws IOException
  {
    switch(col.getType()) {
    case TEXT:
    case MEMO:
      ColumnImpl.SortOrder sortOrder = col.getTextSortOrder();
      if(ColumnImpl.GENERAL_SORT_ORDER.equals(sortOrder)) {
        return new GenTextColumnDescriptor(col, flags);
      }
      if(ColumnImpl.GENERAL_LEGACY_SORT_ORDER.equals(sortOrder)) {
        return new GenLegTextColumnDescriptor(col, flags);
      }
      if(ColumnImpl.GENERAL_97_SORT_ORDER.equals(sortOrder)) {
        return new Gen97TextColumnDescriptor(col, flags);
      }
      // unsupported sort order
      setUnsupportedReason("unsupported collating sort order " + sortOrder +
                           " for text index", col);
      return new ReadOnlyColumnDescriptor(col, flags);
    case INT:
    case LONG:
    case MONEY:
    case COMPLEX_TYPE:
    case BIG_INT:
      return new IntegerColumnDescriptor(col, flags);
    case FLOAT:
    case DOUBLE:
    case SHORT_DATE_TIME:
      return new FloatingPointColumnDescriptor(col, flags);
    case NUMERIC:
      return (col.getFormat().LEGACY_NUMERIC_INDEXES ?
              new LegacyFixedPointColumnDescriptor(col, flags) :
              new FixedPointColumnDescriptor(col, flags));
    case BYTE:
      return new ByteColumnDescriptor(col, flags);
    case BOOLEAN:
      return new BooleanColumnDescriptor(col, flags);
    case GUID:
      return new GuidColumnDescriptor(col, flags);
    case BINARY:
      return new BinaryColumnDescriptor(col, flags);
    case EXT_DATE_TIME:
      return new ExtDateColumnDescriptor(col, flags);

    default:
      // we can't modify this index at this point in time
      setUnsupportedReason("unsupported data type " + col.getType() +
                           " for index", col);
      return new ReadOnlyColumnDescriptor(col, flags);
    }
  }

  /**
   * Returns the EntryType based on the given entry info.
   */
  private static EntryType determineEntryType(byte[] entryBytes, RowIdImpl rowId)
  {
    if(entryBytes != null) {
      return ((rowId.getType() == RowIdImpl.Type.NORMAL) ?
              EntryType.NORMAL :
              ((rowId.getType() == RowIdImpl.Type.ALWAYS_FIRST) ?
               EntryType.FIRST_VALID : EntryType.LAST_VALID));
    } else if(!rowId.isValid()) {
      // this is a "special" entry (first/last)
      return ((rowId.getType() == RowIdImpl.Type.ALWAYS_FIRST) ?
              EntryType.ALWAYS_FIRST : EntryType.ALWAYS_LAST);
    }
    throw new IllegalArgumentException("Values was null for valid entry");
  }

  /**
   * Returns the maximum amount of entry data which can be encoded on any
   * index page.
   */
  private static int calcMaxPageEntrySize(JetFormat format)
  {
    // the max data we can fit on a page is the min of the space on the page
    // vs the number of bytes which can be encoded in the entry mask
    int pageDataSize = (format.PAGE_SIZE -
                        (format.OFFSET_INDEX_ENTRY_MASK +
                         format.SIZE_INDEX_ENTRY_MASK));
    int entryMaskSize = (format.SIZE_INDEX_ENTRY_MASK * 8);
    return Math.min(pageDataSize, entryMaskSize);
  }

  String withErrorContext(String msg) {
    return withErrorContext(msg, getTable().getDatabase(), getTable().getName(),
                            getName());
  }

  private static String withErrorContext(String msg, DatabaseImpl db,
                                         String tableName, String idxName) {
    return msg + " (Db=" + db.getName() + ";Table=" + tableName +
      ";Index=" + idxName + ")";
  }

  /**
   * Information about the columns in an index.  Also encodes new index
   * values.
   */
  public static abstract class ColumnDescriptor implements Index.Column
  {
    private final ColumnImpl _column;
    private final byte _flags;

    private ColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      _column = column;
      _flags = flags;
    }

    @Override
    public ColumnImpl getColumn() {
      return _column;
    }

    public byte getFlags() {
      return _flags;
    }

    @Override
    public boolean isAscending() {
      return((getFlags() & ASCENDING_COLUMN_FLAG) != 0);
    }

    @Override
    public int getColumnIndex() {
      return getColumn().getColumnIndex();
    }

    @Override
    public String getName() {
      return getColumn().getName();
    }

    protected boolean isNullValue(Object value) {
      return (value == null);
    }

    protected final void writeValue(Object value, ByteStream bout)
      throws IOException
    {
      if(isNullValue(value)) {
        // write null value
        bout.write(getNullEntryFlag(isAscending()));
        return;
      }

      // write the start flag
      bout.write(getStartEntryFlag(isAscending()));
      // write the rest of the value
      writeNonNullValue(value, bout);
    }

    protected abstract void writeNonNullValue(Object value, ByteStream bout)
      throws IOException;

    @Override
    public String toString() {
      return CustomToStringStyle.builder(this)
        .append("column", getColumn())
        .append("flags", getFlags() + " " + (isAscending() ? "(ASC)" : "(DSC)"))
        .toString();
    }
  }

  /**
   * ColumnDescriptor for integer based columns.
   */
  private static final class IntegerColumnDescriptor extends ColumnDescriptor
  {
    private IntegerColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      byte[] valueBytes = encodeNumberColumnValue(value, getColumn());

      // bit twiddling rules:
      // - isAsc  => flipFirstBit
      // - !isAsc => flipFirstBit, flipBytes

      flipFirstBitInByte(valueBytes, 0);
      if(!isAscending()) {
        flipBytes(valueBytes);
      }

      bout.write(valueBytes);
    }
  }

  /**
   * ColumnDescriptor for floating point based columns.
   */
  private static final class FloatingPointColumnDescriptor
    extends ColumnDescriptor
  {
    private FloatingPointColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      byte[] valueBytes = encodeNumberColumnValue(value, getColumn());

      // determine if the number is negative by testing if the first bit is
      // set
      boolean isNegative = ((valueBytes[0] & 0x80) != 0);

      // bit twiddling rules:
      // isAsc && !isNeg => flipFirstBit
      // isAsc && isNeg => flipBytes
      // !isAsc && !isNeg => flipFirstBit, flipBytes
      // !isAsc && isNeg => nothing

      if(!isNegative) {
        flipFirstBitInByte(valueBytes, 0);
      }
      if(isNegative == isAscending()) {
        flipBytes(valueBytes);
      }

      bout.write(valueBytes);
    }
  }

  /**
   * ColumnDescriptor for fixed point based columns (legacy sort order).
   */
  private static class LegacyFixedPointColumnDescriptor
    extends ColumnDescriptor
  {
    private LegacyFixedPointColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    protected void handleNegationAndOrder(boolean isNegative,
                                          byte[] valueBytes)
    {
      if(isNegative == isAscending()) {
        flipBytes(valueBytes);
      }

      // reverse the sign byte (after any previous byte flipping)
      valueBytes[0] = (isNegative ? (byte)0x00 : (byte)0xFF);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      byte[] valueBytes = encodeNumberColumnValue(value, getColumn());

      // determine if the number is negative by testing if the first bit is
      // set
      boolean isNegative = ((valueBytes[0] & 0x80) != 0);

      // bit twiddling rules:
      // isAsc && !isNeg => setReverseSignByte             => FF 00 00 ...
      // isAsc && isNeg => flipBytes, setReverseSignByte   => 00 FF FF ...
      // !isAsc && !isNeg => flipBytes, setReverseSignByte => FF FF FF ...
      // !isAsc && isNeg => setReverseSignByte             => 00 00 00 ...

      // v2007 bit twiddling rules (old ordering was a bug, MS kb 837148):
      // isAsc && !isNeg => setSignByte 0xFF            => FF 00 00 ...
      // isAsc && isNeg => setSignByte 0xFF, flipBytes  => 00 FF FF ...
      // !isAsc && !isNeg => setSignByte 0xFF           => FF 00 00 ...
      // !isAsc && isNeg => setSignByte 0xFF, flipBytes => 00 FF FF ...
      handleNegationAndOrder(isNegative, valueBytes);

      bout.write(valueBytes);
    }
  }

  /**
   * ColumnDescriptor for new-style fixed point based columns.
   */
  private static final class FixedPointColumnDescriptor
    extends LegacyFixedPointColumnDescriptor
  {
    private FixedPointColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void handleNegationAndOrder(boolean isNegative,
                                          byte[] valueBytes)
    {
      // see notes above in FixedPointColumnDescriptor for bit twiddling rules

      // reverse the sign byte (before any byte flipping)
      valueBytes[0] = (byte)0xFF;

      if(isNegative == isAscending()) {
        flipBytes(valueBytes);
      }
    }
  }

  /**
   * ColumnDescriptor for byte based columns.
   */
  private static final class ByteColumnDescriptor extends ColumnDescriptor
  {
    private ByteColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      byte[] valueBytes = encodeNumberColumnValue(value, getColumn());

      // bit twiddling rules:
      // - isAsc  => nothing
      // - !isAsc => flipBytes
      if(!isAscending()) {
        flipBytes(valueBytes);
      }

      bout.write(valueBytes);
    }
  }

  /**
   * ColumnDescriptor for boolean columns.
   */
  private static final class BooleanColumnDescriptor extends ColumnDescriptor
  {
    private BooleanColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected boolean isNullValue(Object value) {
      // null values are handled as booleans
      return false;
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      bout.write(
          ColumnImpl.toBooleanValue(value) ?
          (isAscending() ? ASC_BOOLEAN_TRUE : DESC_BOOLEAN_TRUE) :
          (isAscending() ? ASC_BOOLEAN_FALSE : DESC_BOOLEAN_FALSE));
    }
  }

  /**
   * ColumnDescriptor for "general legacy" sort order text based columns.
   */
  private static final class GenLegTextColumnDescriptor
    extends ColumnDescriptor
  {
    private GenLegTextColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      GeneralLegacyIndexCodes.GEN_LEG_INSTANCE.writeNonNullIndexTextValue(
          value, bout, isAscending());
    }
  }

  /**
   * ColumnDescriptor for "general" sort order (2010+) text based columns.
   */
  private static final class GenTextColumnDescriptor extends ColumnDescriptor
  {
    private GenTextColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      GeneralIndexCodes.GEN_INSTANCE.writeNonNullIndexTextValue(
          value, bout, isAscending());
    }
  }

  /**
   * ColumnDescriptor for "general 97" sort order text based columns.
   */
  private static final class Gen97TextColumnDescriptor
    extends ColumnDescriptor
  {
    private Gen97TextColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      General97IndexCodes.GEN_97_INSTANCE.writeNonNullIndexTextValue(
          value, bout, isAscending());
    }
  }

  /**
   * ColumnDescriptor for guid columns.
   */
  private static final class GuidColumnDescriptor extends ColumnDescriptor
  {
    private GuidColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      writeGeneralBinaryEntry(
          encodeNumberColumnValue(value, getColumn()), isAscending(),
          bout);
    }
  }


  /**
   * ColumnDescriptor for BINARY columns.
   */
  private static final class BinaryColumnDescriptor extends ColumnDescriptor
  {
    private BinaryColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      writeGeneralBinaryEntry(
          ColumnImpl.toByteArray(value), isAscending(), bout);
    }
  }

  /**
   * ColumnDescriptor for extended date/time based columns.
   */
  private static final class ExtDateColumnDescriptor extends ColumnDescriptor
  {
    private ExtDateColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      byte[] valueBytes = encodeNumberColumnValue(value, getColumn());

      // entry (which is 42 bytes of data) is encoded in blocks of 8 bytes
      // separated by '\t' char

      // note that for desc, all bytes are flipped _except_ separator char
      byte[] trailer = ASC_EXT_DATE_TRAILER;
      if(!isAscending()) {
        flipBytes(valueBytes);
        trailer = DESC_EXT_DATE_TRAILER;
      }

      // first 5 blocks are all value data
      int valIdx = 0;
      for(int i = 0; i < 5; ++i) {
        bout.write(valueBytes, valIdx, 8);
        bout.write((byte)0x09);
        valIdx += 8;
      }

      // last two data bytes and then the trailer
      bout.write(valueBytes, valIdx, 2);
      bout.write(trailer);
    }
  }

  /**
   * ColumnDescriptor for columns which we cannot currently write.
   */
  private final class ReadOnlyColumnDescriptor extends ColumnDescriptor
  {
    private ReadOnlyColumnDescriptor(ColumnImpl column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteStream bout)
      throws IOException
    {
      throw new UnsupportedOperationException(
          "Cannot write indexes of this type due to " + _unsupportedReason);
    }
  }

  /**
   * A single leaf entry in an index (points to a single row)
   */
  public static class Entry implements Comparable<Entry>
  {
    /** page/row on which this row is stored */
    private final RowIdImpl _rowId;
    /** the entry value */
    private final byte[] _entryBytes;
    /** comparable type for the entry */
    private final EntryType _type;

    /**
     * Create a new entry
     * @param entryBytes encoded bytes for this index entry
     * @param rowId rowId in which the row is stored
     * @param type the type of the entry
     */
    private Entry(byte[] entryBytes, RowIdImpl rowId, EntryType type) {
      _rowId = rowId;
      _entryBytes = entryBytes;
      _type = type;
    }

    /**
     * Create a new entry
     * @param entryBytes encoded bytes for this index entry
     * @param rowId rowId in which the row is stored
     */
    private Entry(byte[] entryBytes, RowIdImpl rowId)
    {
      this(entryBytes, rowId, determineEntryType(entryBytes, rowId));
    }

    /**
     * Read an existing entry in from a buffer
     */
    private Entry(ByteBuffer buffer, int entryLen)
      throws IOException
    {
      this(buffer, entryLen, 0);
    }

    /**
     * Read an existing entry in from a buffer
     */
    private Entry(ByteBuffer buffer, int entryLen, int extraTrailingLen)
      throws IOException
    {
      // we need 4 trailing bytes for the rowId, plus whatever the caller
      // wants
      int colEntryLen = entryLen - (4 + extraTrailingLen);

      // read the entry bytes
      _entryBytes = ByteUtil.getBytes(buffer, colEntryLen);

      // read the rowId
      int page = ByteUtil.get3ByteInt(buffer, ENTRY_BYTE_ORDER);
      int row = ByteUtil.getUnsignedByte(buffer);

      _rowId = new RowIdImpl(page, row);
      _type = EntryType.NORMAL;
    }

    public RowIdImpl getRowId() {
      return _rowId;
    }

    public EntryType getType() {
      return _type;
    }

    public Integer getSubPageNumber() {
      throw new UnsupportedOperationException();
    }

    public boolean isLeafEntry() {
      return true;
    }

    public boolean isValid() {
      return(_entryBytes != null);
    }

    protected final byte[] getEntryBytes() {
      return _entryBytes;
    }

    /**
     * Size of this entry in the db.
     */
    protected int size() {
      // need 4 trailing bytes for the rowId
      return _entryBytes.length + 4;
    }

    /**
     * Write this entry into a buffer
     */
    protected void write(ByteBuffer buffer,
                         byte[] prefix)
      throws IOException
    {
      if(prefix.length <= _entryBytes.length) {

        // write entry bytes, not including prefix
        buffer.put(_entryBytes, prefix.length,
                   (_entryBytes.length - prefix.length));
        ByteUtil.put3ByteInt(buffer, getRowId().getPageNumber(),
                             ENTRY_BYTE_ORDER);

      } else if(prefix.length <= (_entryBytes.length + 3)) {

        // the prefix includes part of the page number, write to temp buffer
        // and copy last bytes to output buffer
        ByteBuffer tmp = ByteBuffer.allocate(3);
        ByteUtil.put3ByteInt(tmp, getRowId().getPageNumber(),
                             ENTRY_BYTE_ORDER);
        tmp.flip();
        tmp.position(prefix.length - _entryBytes.length);
        buffer.put(tmp);

      } else {

        // since the row number would never be the same if the page number is
        // the same, nothing past the page number should ever be included in
        // the prefix.
        // FIXME, this could happen if page has only one row...
        throw new IllegalStateException("prefix should never be this long");
      }

      buffer.put((byte)getRowId().getRowNumber());
    }

    protected final ToStringBuilder entryBytesToStringBuilder(
        ToStringBuilder sb) {
      if(isValid()) {
        sb.append("bytes", _entryBytes);
      }
      return sb;
    }

    @Override
    public String toString() {
      return entryBytesToStringBuilder(
          CustomToStringStyle.valueBuilder(this)
          .append("rowId", _rowId))
        .toString();
    }

    @Override
    public int hashCode() {
      return _rowId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return((this == o) ||
             ((o != null) && (getClass() == o.getClass()) &&
              (compareTo((Entry)o) == 0)));
    }

    /**
     * @return {@code true} iff the entryBytes are equal between this
     *         Entry and the given Entry
     */
    public boolean equalsEntryBytes(Entry o) {
      return(BYTE_CODE_COMPARATOR.compare(_entryBytes, o._entryBytes) == 0);
    }

    @Override
    public int compareTo(Entry other) {
      if (this == other) {
        return 0;
      }

      if(isValid() && other.isValid()) {

        // comparing two valid entries.  first, compare by actual byte values
        int entryCmp = BYTE_CODE_COMPARATOR.compare(
            _entryBytes, other._entryBytes);
        if(entryCmp != 0) {
          return entryCmp;
        }

      } else {

        // if the entries are of mixed validity (or both invalid), we defer
        // next to the EntryType
        int typeCmp = _type.compareTo(other._type);
        if(typeCmp != 0) {
          return typeCmp;
        }
      }

      // at this point we let the RowId decide the final result
      return _rowId.compareTo(other.getRowId());
    }

    /**
     * Returns a copy of this entry as a node Entry with the given
     * subPageNumber.
     */
    protected Entry asNodeEntry(Integer subPageNumber) {
      return new NodeEntry(_entryBytes, _rowId, _type, subPageNumber);
    }

  }

  /**
   * A single node entry in an index (points to a sub-page in the index)
   */
  private static final class NodeEntry extends Entry {

    /** index page number of the page to which this node entry refers */
    private final Integer _subPageNumber;

    /**
     * Create a new node entry
     * @param entryBytes encoded bytes for this index entry
     * @param rowId rowId in which the row is stored
     * @param type the type of the entry
     * @param subPageNumber the sub-page to which this node entry refers
     */
    private NodeEntry(byte[] entryBytes, RowIdImpl rowId, EntryType type,
                      Integer subPageNumber) {
      super(entryBytes, rowId, type);
      _subPageNumber = subPageNumber;
    }

    /**
     * Read an existing node entry in from a buffer
     */
    private NodeEntry(ByteBuffer buffer, int entryLen)
      throws IOException
    {
      // we need 4 trailing bytes for the sub-page number
      super(buffer, entryLen, 4);

      _subPageNumber = ByteUtil.getInt(buffer, ENTRY_BYTE_ORDER);
    }

    @Override
    public Integer getSubPageNumber() {
      return _subPageNumber;
    }

    @Override
    public boolean isLeafEntry() {
      return false;
    }

    @Override
    protected int size() {
      // need 4 trailing bytes for the sub-page number
      return super.size() + 4;
    }

    @Override
    protected void write(ByteBuffer buffer, byte[] prefix) throws IOException {
      super.write(buffer, prefix);
      ByteUtil.putInt(buffer, _subPageNumber, ENTRY_BYTE_ORDER);
    }

    @Override
    public boolean equals(Object o) {
      return((this == o) ||
             ((o != null) && (getClass() == o.getClass()) &&
              (compareTo((Entry)o) == 0) &&
              (getSubPageNumber().equals(((Entry)o).getSubPageNumber()))));
    }

    @Override
    public String toString() {
      return entryBytesToStringBuilder(
          CustomToStringStyle.valueBuilder(this)
          .append("rowId", getRowId())
          .append("subPage", _subPageNumber))
        .toString();
    }
  }

  /**
   * Utility class to traverse the entries in the Index.  Remains valid in the
   * face of index entry modifications.
   */
  public final class EntryCursor
  {
    /** handler for moving the page cursor forward */
    private final DirHandler _forwardDirHandler = new ForwardDirHandler();
    /** handler for moving the page cursor backward */
    private final DirHandler _reverseDirHandler = new ReverseDirHandler();
    /** the first (exclusive) row id for this cursor */
    private Position _firstPos;
    /** the last (exclusive) row id for this cursor */
    private Position _lastPos;
    /** the current entry */
    private Position _curPos;
    /** the previous entry */
    private Position _prevPos;
    /** the last read modification count on the Index.  we track this so that
        the cursor can detect updates to the index while traversing and act
        accordingly */
    private int _lastModCount;

    private EntryCursor(Position firstPos, Position lastPos)
    {
      _firstPos = firstPos;
      _lastPos = lastPos;
      _lastModCount = getIndexModCount();
      reset();
    }

    /**
     * Returns the DirHandler for the given direction
     */
    private DirHandler getDirHandler(boolean moveForward) {
      return (moveForward ? _forwardDirHandler : _reverseDirHandler);
    }

    public IndexData getIndexData() {
      return IndexData.this;
    }

    private int getIndexModCount() {
      return IndexData.this._modCount;
    }

    /**
     * Returns the first entry (exclusive) as defined by this cursor.
     */
    public Entry getFirstEntry() {
      return _firstPos.getEntry();
    }

    /**
     * Returns the last entry (exclusive) as defined by this cursor.
     */
    public Entry getLastEntry() {
      return _lastPos.getEntry();
    }

    /**
     * Returns {@code true} if this cursor is up-to-date with respect to its
     * index.
     */
    public boolean isUpToDate() {
      return(getIndexModCount() == _lastModCount);
    }

    public void reset() {
      beforeFirst();
    }

    public void beforeFirst() {
      reset(CursorImpl.MOVE_FORWARD);
    }

    public void afterLast() {
      reset(CursorImpl.MOVE_REVERSE);
    }

    protected void reset(boolean moveForward)
    {
      _curPos = getDirHandler(moveForward).getBeginningPosition();
      _prevPos = _curPos;
    }

    /**
     * Repositions the cursor so that the next row will be the first entry
     * &gt;= the given row.
     */
    public void beforeEntry(Object[] row)
      throws IOException
    {
      restorePosition(new Entry(IndexData.this.createEntryBytes(row),
                                RowIdImpl.FIRST_ROW_ID));
    }

    /**
     * Repositions the cursor so that the previous row will be the first
     * entry &lt;= the given row.
     */
    public void afterEntry(Object[] row)
      throws IOException
    {
      restorePosition(new Entry(IndexData.this.createEntryBytes(row),
                                RowIdImpl.LAST_ROW_ID));
    }

    /**
     * @return valid entry if there was a next entry,
     *         {@code #getLastEntry} otherwise
     */
    public Entry getNextEntry() throws IOException {
      return getAnotherPosition(CursorImpl.MOVE_FORWARD).getEntry();
    }

    /**
     * @return valid entry if there was a next entry,
     *         {@code #getFirstEntry} otherwise
     */
    public Entry getPreviousEntry() throws IOException {
      return getAnotherPosition(CursorImpl.MOVE_REVERSE).getEntry();
    }

    /**
     * Restores a current position for the cursor (current position becomes
     * previous position).
     */
    protected void restorePosition(Entry curEntry)
      throws IOException
    {
      restorePosition(curEntry, _curPos.getEntry());
    }

    /**
     * Restores a current and previous position for the cursor.
     */
    protected void restorePosition(Entry curEntry, Entry prevEntry)
      throws IOException
    {
      if(!_curPos.equalsEntry(curEntry) ||
         !_prevPos.equalsEntry(prevEntry))
      {
        if(!isUpToDate()) {
          updateBounds();
          _lastModCount = getIndexModCount();
        }
        _prevPos = updatePosition(prevEntry);
        _curPos = updatePosition(curEntry);
      } else {
        checkForModification();
      }
    }

    /**
     * Gets another entry in the given direction, returning the new entry.
     */
    private Position getAnotherPosition(boolean moveForward)
      throws IOException
    {
      DirHandler handler = getDirHandler(moveForward);
      if(_curPos.equals(handler.getEndPosition())) {
        if(!isUpToDate()) {
          restorePosition(_prevPos.getEntry());
          // drop through and retry moving to another entry
        } else {
          // at end, no more
          return _curPos;
        }
      }

      checkForModification();

      _prevPos = _curPos;
      _curPos = handler.getAnotherPosition(_curPos);
      return _curPos;
    }

    /**
     * Checks the index for modifications and updates state accordingly.
     */
    private void checkForModification()
      throws IOException
    {
      if(!isUpToDate()) {
        updateBounds();
        _prevPos = updatePosition(_prevPos.getEntry());
        _curPos = updatePosition(_curPos.getEntry());
        _lastModCount = getIndexModCount();
      }
    }

    /**
     * Updates the given position, taking boundaries into account.
     */
    private Position updatePosition(Entry entry)
      throws IOException
    {
      if(!entry.isValid()) {
        // no use searching if "updating" the first/last pos
        if(_firstPos.equalsEntry(entry)) {
          return _firstPos;
        } else if(_lastPos.equalsEntry(entry)) {
          return _lastPos;
        } else {
          throw new IllegalArgumentException(
              withErrorContext("Invalid entry given " + entry));
        }
      }

      Position pos = findEntryPosition(entry);
      if(pos.compareTo(_lastPos) >= 0) {
        return _lastPos;
      } else if(pos.compareTo(_firstPos) <= 0) {
        return _firstPos;
      }
      return pos;
    }

    /**
     * Updates any the boundary info (_firstPos/_lastPos).
     */
    private void updateBounds()
      throws IOException
    {
      _firstPos = findEntryPosition(_firstPos.getEntry());
      _lastPos = findEntryPosition(_lastPos.getEntry());
    }

    @Override
    public String toString() {
      return CustomToStringStyle.valueBuilder(this)
        .append("curPosition", _curPos)
        .append("prevPosition", _prevPos)
        .toString();
    }

    /**
     * Handles moving the cursor in a given direction.  Separates cursor
     * logic from value storage.
     */
    private abstract class DirHandler {
      public abstract Position getAnotherPosition(Position curPos)
        throws IOException;
      public abstract Position getBeginningPosition();
      public abstract Position getEndPosition();
    }

    /**
     * Handles moving the cursor forward.
     */
    private final class ForwardDirHandler extends DirHandler {
      @Override
      public Position getAnotherPosition(Position curPos)
        throws IOException
      {
        Position newPos = getNextPosition(curPos);
        if((newPos == null) || (newPos.compareTo(_lastPos) >= 0)) {
          newPos = _lastPos;
        }
        return newPos;
      }
      @Override
      public Position getBeginningPosition() {
        return _firstPos;
      }
      @Override
      public Position getEndPosition() {
        return _lastPos;
      }
    }

    /**
     * Handles moving the cursor backward.
     */
    private final class ReverseDirHandler extends DirHandler {
      @Override
      public Position getAnotherPosition(Position curPos)
        throws IOException
      {
        Position newPos = getPreviousPosition(curPos);
        if((newPos == null) || (newPos.compareTo(_firstPos) <= 0)) {
          newPos = _firstPos;
        }
        return newPos;
      }
      @Override
      public Position getBeginningPosition() {
        return _lastPos;
      }
      @Override
      public Position getEndPosition() {
        return _firstPos;
      }
    }
  }

  /**
   * Simple value object for maintaining some cursor state.
   */
  private static final class Position implements Comparable<Position> {
    /** the last known page of the given entry */
    private final DataPage _dataPage;
    /** the last known index of the given entry */
    private final int _idx;
    /** the entry at the given index */
    private final Entry _entry;
    /** {@code true} if this entry does not currently exist in the entry list,
        {@code false} otherwise (this is equivalent to adding -0.5 to the
        _idx) */
    private final boolean _between;

    private Position(DataPage dataPage, int idx)
    {
      this(dataPage, idx, dataPage.getEntries().get(idx), false);
    }

    private Position(DataPage dataPage, int idx, Entry entry, boolean between)
    {
      _dataPage = dataPage;
      _idx = idx;
      _entry = entry;
      _between = between;
    }

    DataPage getDataPage() {
      return _dataPage;
    }

    int getIndex() {
      return _idx;
    }

    int getNextIndex() {
      // note, _idx does not need to be advanced if it was pointing at a
      // between position
      return(_between ? _idx : (_idx + 1));
    }

    int getPrevIndex() {
      // note, we ignore the between flag here because the index will be
      // pointing at the correct next index in either the between or
      // non-between case
      return(_idx - 1);
    }

    Entry getEntry() {
      return _entry;
    }

    boolean equalsEntry(Entry entry) {
      return _entry.equals(entry);
    }

    @Override
    public int compareTo(Position other)
    {
      if(this == other) {
        return 0;
      }

      if(_dataPage.equals(other._dataPage)) {
        // "simple" index comparison (handle between-ness)
        int idxCmp = ((_idx < other._idx) ? -1 :
                      ((_idx > other._idx) ? 1 :
                       ((_between == other._between) ? 0 :
                        (_between ? -1 : 1))));
        if(idxCmp != 0) {
          return idxCmp;
        }
      }

      // compare the entries.
      return _entry.compareTo(other._entry);
    }

    @Override
    public int hashCode() {
      return _entry.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return((this == o) ||
             ((o != null) && (getClass() == o.getClass()) &&
              (compareTo((Position)o) == 0)));
    }

    @Override
    public String toString() {
      return CustomToStringStyle.valueBuilder(this)
        .append("page", _dataPage.getPageNumber())
        .append("idx", _idx)
        .append("entry", _entry)
        .append("between", _between)
        .toString();
    }
  }

  /**
   * Object used to maintain state about an Index page.
   */
  protected static abstract class DataPage {

    public abstract int getPageNumber();

    public abstract boolean isLeaf();
    public abstract void setLeaf(boolean isLeaf);

    public abstract int getPrevPageNumber();
    public abstract void setPrevPageNumber(int pageNumber);
    public abstract int getNextPageNumber();
    public abstract void setNextPageNumber(int pageNumber);
    public abstract int getChildTailPageNumber();
    public abstract void setChildTailPageNumber(int pageNumber);

    public abstract int getTotalEntrySize();
    public abstract void setTotalEntrySize(int totalSize);
    public abstract byte[] getEntryPrefix();
    public abstract void setEntryPrefix(byte[] entryPrefix);

    public abstract List<Entry> getEntries();
    public abstract void setEntries(List<Entry> entries);

    public abstract void addEntry(int idx, Entry entry)
      throws IOException;
    public abstract Entry removeEntry(int idx)
      throws IOException;

    public final boolean isEmpty() {
      return getEntries().isEmpty();
    }

    public final int getCompressedEntrySize() {
      // when written to the index page, the entryPrefix bytes will only be
      // written for the first entry, so we subtract the entry prefix size
      // from all the other entries to determine the compressed size
      return getTotalEntrySize() -
        (getEntryPrefix().length * (getEntries().size() - 1));
    }

    public final int findEntry(Entry entry) {
      return Collections.binarySearch(getEntries(), entry);
    }

    @Override
    public final int hashCode() {
      return getPageNumber();
    }

    @Override
    public final boolean equals(Object o) {
      return((this == o) ||
             ((o != null) && (getClass() == o.getClass()) &&
              (getPageNumber() == ((DataPage)o).getPageNumber())));
    }

    @Override
    public final String toString() {
      List<Entry> entries = getEntries();

      String objName =
        (isLeaf() ? "Leaf" : "Node") + "DataPage[" + getPageNumber() +
        "] " + getPrevPageNumber() + ", " + getNextPageNumber() + ", (" +
        getChildTailPageNumber() + ")";
      ToStringBuilder sb = CustomToStringStyle.valueBuilder(objName);

      if((isLeaf() && !entries.isEmpty())) {
        sb.append("entryRange", "[" + entries.get(0) + ", " +
                  entries.get(entries.size() - 1) + "]");
      } else {
        sb.append("entries", entries);
      }
      return sb.toString();
    }
  }

  /**
   * Simple implementation of a DataPage
   */
  private static final class RootDataPage extends DataPage {

    @Override
    public int getPageNumber() { return 0; }

    @Override
    public boolean isLeaf() { return true; }
    @Override
    public void setLeaf(boolean isLeaf) { }

    @Override
    public int getPrevPageNumber() { return 0; }
    @Override
    public void setPrevPageNumber(int pageNumber) { }

    @Override
    public int getNextPageNumber() { return 0; }
    @Override
    public void setNextPageNumber(int pageNumber) { }

    @Override
    public int getChildTailPageNumber() { return 0; }
    @Override
    public void setChildTailPageNumber(int pageNumber) { }

    @Override
    public int getTotalEntrySize() { return 0; }
    @Override
    public void setTotalEntrySize(int totalSize) { }

    @Override
    public byte[] getEntryPrefix() { return EMPTY_PREFIX; }
    @Override
    public void setEntryPrefix(byte[] entryPrefix) { }

    @Override
    public List<Entry> getEntries() { return Collections.emptyList(); }
    @Override
    public void setEntries(List<Entry> entries) { }
    @Override
    public void addEntry(int idx, Entry entry) { }
    @Override
    public Entry removeEntry(int idx) { return null; }
  }

  /**
   * Utility class which maintains information about a pending index update.
   * An instance of this class can be used to complete the change (by calling
   * {@link #commit}) or undo the change (by calling {@link #rollback}).
   */
  public static abstract class PendingChange
  {
    private final PendingChange _next;

    private PendingChange(PendingChange next) {
      _next = next;
    }

    /**
     * Returns the next pending change, if any
     */
    public PendingChange getNext() {
      return _next;
    }

    /**
     * Completes the pending change.
     */
    public abstract void commit() throws IOException;

    /**
     * Undoes the pending change.
     */
    public abstract void rollback() throws IOException;
  }

  /**
   * PendingChange for a row addition.
   */
  private class AddRowPendingChange extends PendingChange
  {
    protected Entry _addEntry;
    protected DataPage _addDataPage;
    protected int _addIdx;
    protected boolean _isDupe;
    protected Entry _oldEntry;

    private AddRowPendingChange(PendingChange next) {
      super(next);
    }

    public void setAddRow(Entry addEntry, DataPage dataPage, int idx,
                          boolean isDupe) {
      _addEntry = addEntry;
      _addDataPage = dataPage;
      _addIdx = idx;
      _isDupe = isDupe;
    }

    public void setOldRow(Entry oldEntry) {
      _oldEntry = oldEntry;
    }

    @Override
    public void commit() throws IOException {
      commitAddRow(_addEntry, _addDataPage, _addIdx, _isDupe, _oldEntry);
    }

    @Override
    public void rollback() throws IOException {
      _addEntry = null;
      _addDataPage = null;
      _addIdx = -1;
    }
  }

  /**
   * PendingChange for a row update (which is essentially a deletion followed
   * by an addition).
   */
  private class UpdateRowPendingChange extends AddRowPendingChange
  {
    private UpdateRowPendingChange(PendingChange next) {
      super(next);
    }

    @Override
    public void rollback() throws IOException {
      super.rollback();
      rollbackDeletedRow(_oldEntry);
    }
  }

}
