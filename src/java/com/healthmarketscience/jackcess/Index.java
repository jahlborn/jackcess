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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.healthmarketscience.jackcess.IndexCodes.*;


/**
 * Access table index
 * @author Tim McCune
 */
public class Index implements Comparable<Index> {
  
  private static final Log LOG = LogFactory.getLog(Index.class);

  /** special entry which is less than any other entry */
  public static final Entry FIRST_ENTRY =
    createSpecialEntry(RowId.FIRST_ROW_ID);
  
  /** special entry which is greater than any other entry */
  public static final Entry LAST_ENTRY =
    createSpecialEntry(RowId.LAST_ROW_ID);
  
  /** index of the first (exclusive) index entry */
  private static final int FIRST_ENTRY_IDX = -1;
  /** index of the last (exclusive) index entry */
  private static final int LAST_ENTRY_IDX = -2;

  /** the first position for a cursor */
  private static final Position FIRST_POSITION =
    new Position(FIRST_ENTRY_IDX, FIRST_ENTRY);
  
  /** the last position for a cursor */
  private static final Position LAST_POSITION =
    new Position(LAST_ENTRY_IDX, LAST_ENTRY);
  
  /** Max number of columns in an index */
  private static final int MAX_COLUMNS = 10;
  
  private static final short COLUMN_UNUSED = -1;

  private static final byte INDEX_NODE_PAGE_TYPE = (byte)0x03;
  private static final byte INDEX_LEAF_PAGE_TYPE = (byte)0x04;

  private static final byte ASCENDING_COLUMN_FLAG = (byte)0x01;

  private static final byte UNIQUE_INDEX_FLAG = (byte)0x01;
  private static final byte IGNORE_NULLS_INDEX_FLAG = (byte)0x02;

  /** index type for primary key indexes */
  private static final byte PRIMARY_KEY_INDEX_TYPE = (byte)1;
  
  /** index type for foreign key indexes */
  private static final byte FOREIGN_KEY_INDEX_TYPE = (byte)2;

  private static final int MAX_TEXT_INDEX_CHAR_LENGTH =
    (JetFormat.TEXT_FIELD_MAX_LENGTH / JetFormat.TEXT_FIELD_UNIT_SIZE);
  
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
  
  static final Comparator<byte[]> BYTE_CODE_COMPARATOR =
    new Comparator<byte[]>() {
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
        
  
  /** owning table */
  private final Table _table;
  /** Page number of the index data */
  private int _pageNumber;
  /** offset within the tableDefinition buffer of the uniqueEntryCount for
      this index */
  private final int _uniqueEntryCountOffset;
  /** The number of unique entries which have been added to this index.  note,
      however, that it is never decremented, only incremented (as observed in
      Access). */
  private int _uniqueEntryCount;
  /** sorted collection of index entries.  this is kept in a list instead of a
      SortedSet because the SortedSet has lame traversal utilities */
  private final List<Entry> _entries = new ArrayList<Entry>();
  /** List of columns and flags */
  private final List<ColumnDescriptor> _columns =
    new ArrayList<ColumnDescriptor>();
  /** 0-based index number */
  private int _indexNumber;
  /** flags for this index */
  private byte _indexFlags;
  /** the type of the index */
  private byte _indexType;
  /** Index name */
  private String _name;
  /** <code>true</code> if the index entries have been initialized,
      <code>false</code> otherwise */
  private boolean _initialized;
  /** modification count for the table, keeps cursors up-to-date */
  private int _modCount;
  /** temp buffer used to writing the index */
  private final TempBufferHolder _indexBufferH =
    TempBufferHolder.newHolder(false, true);
  /** FIXME, for now, we can't write multi-page indexes or indexes using the funky primary key compression scheme */
  boolean _readOnly;
  
  public Index(Table table, int uniqueEntryCount, int uniqueEntryCountOffset) {
    _table  = table;
    _uniqueEntryCount = uniqueEntryCount;
    _uniqueEntryCountOffset = uniqueEntryCountOffset;
  }

  public Table getTable() {
    return _table;
  }
  
  public JetFormat getFormat() {
    return getTable().getFormat();
  }

  public PageChannel getPageChannel() {
    return getTable().getPageChannel();
  }

  public void setIndexNumber(int indexNumber) {
    _indexNumber = indexNumber;
  }

  public int getIndexNumber() {
    return _indexNumber;
  }

  public void setIndexType(byte indexType) {
    _indexType = indexType;
  }

  public byte getIndexFlags() {
    return _indexFlags;
  }
  
  public int getUniqueEntryCount() {
    return _uniqueEntryCount;
  }

  public int getUniqueEntryCountOffset() {
    return _uniqueEntryCountOffset;
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
    return(isPrimaryKey() || ((_indexFlags & UNIQUE_INDEX_FLAG) != 0));
  }
  
  /**
   * Returns the Columns for this index (unmodifiable)
   */
  public List<ColumnDescriptor> getColumns() {
    return Collections.unmodifiableList(_columns);
  }

  /**
   * Returns the number of index entries in the index.  Only called by unit
   * tests.
   * <p>
   * Forces index initialization.
   */
  int getEntryCount()
    throws IOException
  {
    initialize();
    return _entries.size();
  }

  /**
   * Whether or not the complete index state has been read.
   */
  public boolean isInitialized() {
    return _initialized;
  }

  /**
   * Forces initialization of this index (actual parsing of index pages).
   * normally, the index will not be initialized until the entries are
   * actually needed.
   */
  public void initialize() throws IOException {
    if(!_initialized) {
      readIndexEntries();
      _initialized = true;
    }
  }

  /**
   * Writes the current index state to the database.
   * <p>
   * Forces index initialization.
   */
  public void update() throws IOException {
    // make sure we've parsed the entries
    initialize();
    
    if(_readOnly) {
      throw new UnsupportedOperationException(
          "FIXME cannot write indexes of this type yet");
    }
    getPageChannel().writePage(write(), _pageNumber);
  }

  /**
   * Write this index out to a buffer
   */
  private ByteBuffer write() throws IOException {
    ByteBuffer buffer = _indexBufferH.getPageBuffer(getPageChannel());
    buffer.put((byte) 0x04);  //Page type
    buffer.put((byte) 0x01);  //Unknown
    buffer.putShort((short) 0); //Free space
    buffer.putInt(getTable().getTableDefPageNumber());
    buffer.putInt(0); //Prev page
    buffer.putInt(0); //Next page
    buffer.putInt(0); //Leaf page
    buffer.putInt(0); //Unknown
    buffer.put((byte) 0); // compressed byte count
    buffer.put((byte) 0); //Unknown
    buffer.put((byte) 0); //Unknown
    byte[] entryMask = new byte[getFormat().SIZE_INDEX_ENTRY_MASK];
    int totalSize = 0;
    for(Entry entry : _entries) {
      int size = entry.size();
      totalSize += size;
      int idx = totalSize  / 8;
      if(idx >= entryMask.length) {
        throw new UnsupportedOperationException(
            "FIXME cannot write large index yet");
      }
      entryMask[idx] |= (1 << (totalSize % 8));
    }
    buffer.put(entryMask);
    for(Entry entry : _entries) {
      entry.write(buffer);
    }
    buffer.putShort(2, (short) (getFormat().PAGE_SIZE - buffer.position()));
    return buffer;
  }
  
  /**
   * Read the index info from a tableBuffer
   * @param tableBuffer table definition buffer to read from initial info
   * @param availableColumns Columns that this index may use
   */
  public void read(ByteBuffer tableBuffer, List<Column> availableColumns)
    throws IOException
  {
    for (int i = 0; i < MAX_COLUMNS; i++) {
      short columnNumber = tableBuffer.getShort();
      byte colFlags = tableBuffer.get();
      if (columnNumber != COLUMN_UNUSED) {
        // find the desired column by column number (which is not necessarily
        // the same as the column index)
        Column idxCol = null;
        for(Column col : availableColumns) {
          if(col.getColumnNumber() == columnNumber) {
            idxCol = col;
            break;
          }
        }
        if(idxCol == null) {
          throw new IOException("Could not find column with number "
                                + columnNumber + " for index " + getName());
        }
        _columns.add(newColumnDescriptor(idxCol, colFlags));
      }
    }
    tableBuffer.getInt(); //Forward past Unknown
    _pageNumber = tableBuffer.getInt();
    tableBuffer.getInt(); //Forward past Unknown
    _indexFlags = tableBuffer.get();
    tableBuffer.position(tableBuffer.position() + 5);  //Forward past other stuff
  }

  /**
   * Reads the actual index entries.
   */
  private void readIndexEntries()
    throws IOException
  {
    ByteBuffer indexPage = getPageChannel().createPageBuffer();

    // find first leaf page
    int leafPageNumber = _pageNumber;
    while(true) {
      getPageChannel().readPage(indexPage, leafPageNumber);

      if(indexPage.get(0) == INDEX_NODE_PAGE_TYPE) {
        // FIXME we can't modify this index at this point in time
        _readOnly = true;

        // found another node page
        leafPageNumber = readNodePage(indexPage);
      } else {
        // found first leaf
        indexPage.rewind();
        break;
      }
    }

    // read all leaf pages.  since we read all the entries in sort order, we
    // can insert them directly into the _entries list
    while(true) {

      leafPageNumber = readLeafPage(indexPage, _entries);
      if(leafPageNumber != 0) {
        // FIXME we can't modify this index at this point in time
        _readOnly = true;
        
        // found another one 
        getPageChannel().readPage(indexPage, leafPageNumber);
        
      } else {
        // all done
        break;
      }
    }

    // check the entry order, just to be safe
    for(int i = 0; i < (_entries.size() - 1); ++i) {
      Entry e1 = _entries.get(i);
      Entry e2 = _entries.get(i + 1);
      if(e1.compareTo(e2) > 0) {
        throw new IOException("Unexpected order in index entries, " +
                              e1 + " is greater than " + e2);
      }
    }
  }

  /**
   * Reads the first entry off of an index node page and returns the next page
   * number.
   */
  private int readNodePage(ByteBuffer nodePage)
    throws IOException
  {
    if(nodePage.get(0) != INDEX_NODE_PAGE_TYPE) {
      throw new IOException("expected index node page, found " +
                            nodePage.get(0));
    }
    
    List<NodeEntry> nodeEntries = new ArrayList<NodeEntry>();
    readIndexPage(nodePage, false, null, nodeEntries);

    // grab the first entry
    // FIXME, need to parse all...?
    return nodeEntries.get(0).getSubPageNumber();
  }

  /**
   * Reads an index leaf page.
   * @return the next leaf page number, 0 if none
   */
  private int readLeafPage(ByteBuffer leafPage, Collection<Entry> entries)
    throws IOException
  {
    if(leafPage.get(0) != INDEX_LEAF_PAGE_TYPE) {
      throw new IOException("expected index leaf page, found " +
                            leafPage.get(0));
    }
    
    // note, "header" data is in LITTLE_ENDIAN format, entry data is in
    // BIG_ENDIAN format

    int nextLeafPage = leafPage.getInt(getFormat().OFFSET_NEXT_INDEX_LEAF_PAGE);
    readIndexPage(leafPage, true, entries, null);

    return nextLeafPage;
  }

  /**
   * Reads an index page, populating the correct collection based on the page
   * type (node or leaf).
   */
  private void readIndexPage(ByteBuffer indexPage, boolean isLeaf,
                             Collection<Entry> entries,
                             Collection<NodeEntry> nodeEntries)
    throws IOException
  {
    // note, "header" data is in LITTLE_ENDIAN format, entry data is in
    // BIG_ENDIAN format
    int numCompressedBytes = ByteUtil.getUnsignedByte(
        indexPage, getFormat().OFFSET_INDEX_COMPRESSED_BYTE_COUNT);
    int entryMaskLength = getFormat().SIZE_INDEX_ENTRY_MASK;
    int entryMaskPos = getFormat().OFFSET_INDEX_ENTRY_MASK;
    int entryPos = entryMaskPos + getFormat().SIZE_INDEX_ENTRY_MASK;
    int lastStart = 0;
    byte[] valuePrefix = null;
    boolean firstEntry = true;
    TempBufferHolder tmpEntryBufferH =
      TempBufferHolder.newHolder(true, true, ByteOrder.BIG_ENDIAN);

    for (int i = 0; i < entryMaskLength; i++) {
      byte entryMask = indexPage.get(entryMaskPos + i);
      for (int j = 0; j < 8; j++) {
        if ((entryMask & (1 << j)) != 0) {
          int length = (i * 8) + j - lastStart;
          indexPage.position(entryPos + lastStart);

          // determine if we can read straight from the index page (if no
          // valuePrefix).  otherwise, create temp buf with complete entry.
          ByteBuffer curEntryBuffer = indexPage;
          int curEntryLen = length;
          if(valuePrefix != null) {
            curEntryBuffer = getTempEntryBuffer(
                indexPage, length, valuePrefix, tmpEntryBufferH);
            curEntryLen += valuePrefix.length;
          }
          
          if(isLeaf) {
            entries.add(new Entry(curEntryBuffer, curEntryLen));
          } else {
            nodeEntries.add(new NodeEntry(curEntryBuffer, curEntryLen));
          }

          // read any shared "compressed" bytes
          if(firstEntry) {
            firstEntry = false;
            if(numCompressedBytes > 0) {
              // FIXME we can't modify this index at this point in time
              _readOnly = true;

              valuePrefix = new byte[numCompressedBytes];
              indexPage.position(entryPos + lastStart);
              indexPage.get(valuePrefix);
            }
          }

          lastStart += length;          
        }
      }
    }
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
    int nullCount = countNullValues(row);
    boolean isNullEntry = (nullCount == _columns.size());
    if(shouldIgnoreNulls() && isNullEntry) {
      // nothing to do
      return;
    }
    if(isPrimaryKey() && (nullCount > 0)) {
      throw new IOException("Null value found in row " + Arrays.asList(row)
                            + " for primary key index " + this);
    }
    
    // make sure we've parsed the entries
    initialize();

    Entry newEntry = new Entry(createEntryBytes(row), rowId);
    if(addEntry(newEntry, isNullEntry, row)) {
      ++_modCount;
    } else {
      LOG.warn("Added duplicate index entry " + newEntry + " for row: " +
               Arrays.asList(row));
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
  public void deleteRow(Object[] row, RowId rowId)
    throws IOException
  {
    int nullCount = countNullValues(row);
    if(shouldIgnoreNulls() && (nullCount == _columns.size())) {
      // nothing to do
      return;
    }
    
    // make sure we've parsed the entries
    initialize();

    Entry oldEntry = new Entry(createEntryBytes(row), rowId);
    if(removeEntry(oldEntry)) {
      ++_modCount;
    } else {
      LOG.warn("Failed removing index entry " + oldEntry + " for row: " +
               Arrays.asList(row));
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
    Position startPos = FIRST_POSITION;
    byte[] startEntryBytes = null;
    if(startRow != null) {
      startEntryBytes = createEntryBytes(startRow);
      Entry startEntry = new Entry(startEntryBytes,
                                   (startInclusive ?
                                    RowId.FIRST_ROW_ID : RowId.LAST_ROW_ID));
      startPos = new Position(FIRST_ENTRY_IDX, startEntry);
    }
    Position endPos = LAST_POSITION;
    if(endRow != null) {
      // reuse startEntryBytes if startRow and endRow are same array.  this is
      // common for "lookup" code
      byte[] endEntryBytes = ((startRow == endRow) ?
                              startEntryBytes :
                              createEntryBytes(endRow));
      Entry endEntry = new Entry(endEntryBytes,
                                 (endInclusive ?
                                  RowId.LAST_ROW_ID : RowId.FIRST_ROW_ID));
      endPos = new Position(LAST_ENTRY_IDX, endEntry);
    }
    return new EntryCursor(startPos, endPos);
  }
  
  /**
   * Finds the index of given entry in the _entries list.
   * @return the index if found, (-<insertion_point> - 1) if not found
   */
  private int findEntry(Entry entry) {
    return Collections.binarySearch(_entries, entry);
  }

  /**
   * Returns the valid insertion point for an index indicating a missing
   * entry.
   */
  private static int missingIndexToInsertionPoint(int idx) {
    return -(idx + 1);
  }
  
  /**
   * Adds an entry to the _entries list, maintaining the order.
   */
  private boolean addEntry(Entry newEntry, boolean isNullEntry, Object[] row)
    throws IOException
  {
    int idx = findEntry(newEntry);
    if(idx < 0) {
      // this is a new entry
      idx = missingIndexToInsertionPoint(idx);

      // determine if the addition of this entry would break the uniqueness
      // constraint.  See isUnique() for some notes about uniqueness as
      // defined by Access.
      boolean isDupeEntry =
        (((idx > 0) &&
          newEntry.equalsEntryBytes(_entries.get(idx - 1))) ||
          ((idx < _entries.size()) &&
           newEntry.equalsEntryBytes(_entries.get(idx))));
      if(isUnique() && !isNullEntry && isDupeEntry)
      {
        throw new IOException(
            "New row " + Arrays.asList(row) +
            " violates uniqueness constraint for index " + this);
      }

      if(!isDupeEntry) {
        ++_uniqueEntryCount;
      }
      
      _entries.add(idx, newEntry);
      return true;
    }
    return false;
  }

  /**
   * Removes an entry from the _entries list, maintaining the order.  Will
   * search by RowId if entry is not found in case a partial entry was
   * provided.
   */
  private boolean removeEntry(Entry oldEntry)
  {
    int idx = findEntry(oldEntry);
    boolean removed = false;
    if(idx < 0) {
      // the caller may have only read some of the row data, if this is the
      // case, just search for the page/row numbers
      for(Iterator<Entry> iter = _entries.iterator(); iter.hasNext(); ) {
        Entry entry = iter.next();
        if(entry.getRowId().equals(oldEntry.getRowId())) {
          iter.remove();
          removed = true;
          break;
        }
      }
    } else {
      // found it!
      _entries.remove(idx);
      removed = true;
    }
    
    return removed;
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
      throw new IllegalArgumentException(
          "Wrong number of column values given " + values.length +
          ", expected " + _columns.size());
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

  @Override
  public String toString() {
    StringBuilder rtn = new StringBuilder();
    rtn.append("\tName: (" + _table.getName() + ") " + _name);
    rtn.append("\n\tNumber: " + _indexNumber);
    rtn.append("\n\tPage number: " + _pageNumber);
    rtn.append("\n\tIs Primary Key: " + isPrimaryKey());
    rtn.append("\n\tColumns: " + _columns);
    rtn.append("\n\tInitialized: " + _initialized);
    rtn.append("\n\tEntries: " + _entries);
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
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    
    // annoyingly, the values array could come from different sources, one
    // of which will make it a different size than the other.  we need to
    // handle both situations.
    for(ColumnDescriptor col : _columns) {
      Object value = values[col.getColumnIndex()];
      col.writeValue(value, bout);
    }
    
    return bout.toByteArray();
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
    for(int i = 0; i < value.length; ++i) {
      value[i] = (byte)(~value[i]);
    } 
    return value;
  }

  /**
   * Writes the value of the given column type to a byte array and returns it.
   */
  private static byte[] encodeNumberColumnValue(Object value, Column column)
    throws IOException
  {
    // always write in big endian order
    return column.write(value, 0, ByteOrder.BIG_ENDIAN).array();
  }    

  /**
   * Converts an index value for a text column into the entry value (which
   * is based on a variety of nifty codes).
   */
  private static void writeNonNullIndexTextValue(
      Object value, ByteArrayOutputStream bout, boolean isAscending)
    throws IOException
  {
    // first, convert to string
    String str = Column.toCharSequence(value).toString();

    // all text columns (including memos) are only indexed up to the max
    // number of chars in a VARCHAR column
    if(str.length() > MAX_TEXT_INDEX_CHAR_LENGTH) {
      str = str.substring(0, MAX_TEXT_INDEX_CHAR_LENGTH);
    }
    
    ByteArrayOutputStream tmpBout = bout;
    if(!isAscending) {
      // we need to accumulate the bytes in a temp array in order to negate
      // them before writing them to the final array
      tmpBout = new ByteArrayOutputStream();
    }
    
    // now, convert each character to a "code" of one or more bytes
    List<ExtraCodes> unprintableCodes = null;
    List<ExtraCodes> internationalCodes = null;
    int charOffset = 0;
    for(int i = 0; i < str.length(); ++i) {
      char c = str.charAt(i);
      Character cKey = c;

      byte[] bytes = CODES.get(cKey);
      if(bytes != null) {
        // simple case, write the codes we found
        tmpBout.write(bytes);
        ++charOffset;
        continue;
      }

      bytes = UNPRINTABLE_CODES.get(cKey);
      if(bytes != null) {
        // we do not write anything to tmpBout
        if(bytes.length > 0) {
          if(unprintableCodes == null) {
            unprintableCodes = new LinkedList<ExtraCodes>();
          }
          
          // keep track of the extra codes for later
          unprintableCodes.add(new ExtraCodes(charOffset, bytes));
        }

        // note, we do _not_ increment the charOffset for unprintable chars
        continue;
      }

      InternationalCodes inatCodes = INTERNATIONAL_CODES.get(cKey);
      if(inatCodes != null) {

        // we write the "inline" portion of the international codes
        // immediately, and queue the extra codes for later
        tmpBout.write(inatCodes._inlineCodes);

        if(internationalCodes == null) {
          internationalCodes = new LinkedList<ExtraCodes>();
        }

        // keep track of the extra codes for later
        internationalCodes.add(new ExtraCodes(charOffset,
                                              inatCodes._extraCodes));

        ++charOffset;
        continue;
      }

      // bummer, out of luck
      throw new IOException("unmapped string index value " + c);
    }

    // write end text flag
    tmpBout.write(END_TEXT);

    boolean hasExtraText = ((unprintableCodes != null) ||
                            (internationalCodes != null));
    if(hasExtraText) {

      // we write all the international extra bytes first
      if(internationalCodes != null) {

        // we write a placeholder char for each non-international char before
        // the extra chars for the international char
        charOffset = 0;
        Iterator<ExtraCodes> iter = internationalCodes.iterator();
        while(iter.hasNext()) {
          ExtraCodes extraCodes = iter.next();
          while(charOffset < extraCodes._charOffset) {
            tmpBout.write(INTERNATIONAL_EXTRA_PLACEHOLDER);
            ++charOffset;
          }
          tmpBout.write(extraCodes._extraCodes);
          ++charOffset;
        }
      }

      // then we write all the unprintable extra bytes
      if(unprintableCodes != null) {

        // write a single prefix for all unprintable chars
        tmpBout.write(UNPRINTABLE_COMMON_PREFIX);
        
        // we write a whacky combo of bytes for each unprintable char which
        // includes a funky offset and extra char itself
        Iterator<ExtraCodes> iter = unprintableCodes.iterator();
        while(iter.hasNext()) {
          ExtraCodes extraCodes = iter.next();
          int offset =
            (UNPRINTABLE_COUNT_START +
             (UNPRINTABLE_COUNT_MULTIPLIER * extraCodes._charOffset))
            | UNPRINTABLE_OFFSET_FLAGS;

          // write offset as big-endian short
          tmpBout.write((offset >> 8) & 0xFF);
          tmpBout.write(offset & 0xFF);
          
          tmpBout.write(UNPRINTABLE_MIDFIX);
          tmpBout.write(extraCodes._extraCodes);
        }
      }

    }

    // handle descending order by inverting the bytes
    if(!isAscending) {

      // we actually write the end byte before flipping the bytes, and write
      // another one after flipping
      tmpBout.write(END_EXTRA_TEXT);
      
      // we actually wrote into a temporary array so that we can invert the
      // bytes before writing them to the final array
      bout.write(flipBytes(tmpBout.toByteArray()));

    }

    // write end extra text
    bout.write(END_EXTRA_TEXT);    
  }

  /**
   * Creates one of the special index entries.
   */
  private static Entry createSpecialEntry(RowId rowId) {
    try {
      return new Entry((byte[])null, rowId);
    } catch(IOException e) {
      // should never happen
      throw new IllegalStateException(e);
    }
  }

  /**
   * Constructs a ColumnDescriptor of the relevant type for the given Column.
   */
  private ColumnDescriptor newColumnDescriptor(Column col, byte flags)
    throws IOException
  {
    switch(col.getType()) {
    case TEXT:
    case MEMO:
      return new TextColumnDescriptor(col, flags);
    case INT:
    case LONG:
    case MONEY:
      return new IntegerColumnDescriptor(col, flags);
    case FLOAT:
    case DOUBLE:
    case SHORT_DATE_TIME:
      return new FloatingPointColumnDescriptor(col, flags);
    case NUMERIC:
      return new FixedPointColumnDescriptor(col, flags);
    case BYTE:
      return new ByteColumnDescriptor(col, flags);
    case BOOLEAN:
      return new BooleanColumnDescriptor(col, flags);

    default:
      // FIXME we can't modify this index at this point in time
      _readOnly = true;
      return new ReadOnlyColumnDescriptor(col, flags);
    }
  }

  
  /**
   * Information about the columns in an index.  Also encodes new index
   * values.
   */
  public static abstract class ColumnDescriptor
  {
    private final Column _column;
    private final byte _flags;

    private ColumnDescriptor(Column column, byte flags)
      throws IOException
    {
      _column = column;
      _flags = flags;
    }

    public Column getColumn() {
      return _column;
    }

    public byte getFlags() {
      return _flags;
    }

    public boolean isAscending() {
      return((getFlags() & ASCENDING_COLUMN_FLAG) != 0);
    }
    
    public int getColumnIndex() {
      return getColumn().getColumnIndex();
    }
    
    public String getName() {
      return getColumn().getName();
    }

    protected boolean isNullValue(Object value) {
      return (value == null);
    }
    
    protected final void writeValue(Object value, ByteArrayOutputStream bout)
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

    protected abstract void writeNonNullValue(
        Object value, ByteArrayOutputStream bout)
      throws IOException; 
    
    @Override
    public String toString() {
      return "ColumnDescriptor " + getColumn() + "\nflags: " + getFlags();
    }
  }

  /**
   * ColumnDescriptor for integer based columns.
   */
  private static final class IntegerColumnDescriptor extends ColumnDescriptor
  {
    private IntegerColumnDescriptor(Column column, byte flags)
      throws IOException
    {
      super(column, flags);
    }
    
    @Override
    protected void writeNonNullValue(
        Object value, ByteArrayOutputStream bout)
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
    private FloatingPointColumnDescriptor(Column column, byte flags)
      throws IOException
    {
      super(column, flags);
    }
    
    @Override
    protected void writeNonNullValue(
        Object value, ByteArrayOutputStream bout)
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
   * ColumnDescriptor for fixed point based columns.
   */
  private static final class FixedPointColumnDescriptor
    extends ColumnDescriptor
  {
    private FixedPointColumnDescriptor(Column column, byte flags)
      throws IOException
    {
      super(column, flags);
    }
    
    @Override
    protected void writeNonNullValue(
        Object value, ByteArrayOutputStream bout)
      throws IOException
    {
      byte[] valueBytes = encodeNumberColumnValue(value, getColumn());
      
      // determine if the number is negative by testing if the first bit is
      // set
      boolean isNegative = ((valueBytes[0] & 0x80) != 0);

      // bit twiddling rules:
      // isAsc && !isNeg => setReverseSignByte
      // isAsc && isNeg => flipBytes, setReverseSignByte
      // !isAsc && !isNeg => flipBytes, setReverseSignByte
      // !isAsc && isNeg => setReverseSignByte
      
      if(isNegative == isAscending()) {
        flipBytes(valueBytes);
      }

      // reverse the sign byte (after any previous byte flipping)
      valueBytes[0] = (isNegative ? (byte)0x00 : (byte)0xFF);
      
      bout.write(valueBytes);
    }    
  }
  
  /**
   * ColumnDescriptor for byte based columns.
   */
  private static final class ByteColumnDescriptor extends ColumnDescriptor
  {
    private ByteColumnDescriptor(Column column, byte flags)
      throws IOException
    {
      super(column, flags);
    }
    
    @Override
    protected void writeNonNullValue(
        Object value, ByteArrayOutputStream bout)
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
    private BooleanColumnDescriptor(Column column, byte flags)
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
    protected void writeNonNullValue(Object value, ByteArrayOutputStream bout)
      throws IOException
    {
      bout.write(
          Column.toBooleanValue(value) ?
          (isAscending() ? ASC_BOOLEAN_TRUE : DESC_BOOLEAN_TRUE) :
          (isAscending() ? ASC_BOOLEAN_FALSE : DESC_BOOLEAN_FALSE));
    }
  }
  
  /**
   * ColumnDescriptor for text based columns.
   */
  private static final class TextColumnDescriptor extends ColumnDescriptor
  {
    private TextColumnDescriptor(Column column, byte flags)
      throws IOException
    {
      super(column, flags);
    }
    
    @Override
    protected void writeNonNullValue(
        Object value, ByteArrayOutputStream bout)
      throws IOException
    {
      writeNonNullIndexTextValue(value, bout, isAscending());
    }    
  }

  /**
   * ColumnDescriptor for columns which we cannot currently write.
   */
  private static final class ReadOnlyColumnDescriptor extends ColumnDescriptor
  {
    private ReadOnlyColumnDescriptor(Column column, byte flags)
      throws IOException
    {
      super(column, flags);
    }

    @Override
    protected void writeNonNullValue(Object value, ByteArrayOutputStream bout)
      throws IOException
    {
      throw new UnsupportedOperationException("should not be called");
    }
  }
    
  /**
   * A single leaf entry in an index (points to a single row)
   */
  public static class Entry implements Comparable<Entry>
  {
    /** page/row on which this row is stored */
    private final RowId _rowId;
    /** the entry value */
    private final byte[] _entryBytes;
    /** comparable type for the entry */
    private final EntryType _type;
    
    /**
     * Create a new entry
     * @param entryBytes encoded bytes for this index entry
     * @param rowId rowId in which the row is stored
     */
    private Entry(byte[] entryBytes, RowId rowId)
      throws IOException
    {
      _rowId = rowId;
      _entryBytes = entryBytes;
      if(_entryBytes != null) {
        _type = ((_rowId.getType() == RowId.Type.NORMAL) ?
                 EntryType.NORMAL :
                 ((_rowId.getType() == RowId.Type.ALWAYS_FIRST) ?
                  EntryType.FIRST_VALID : EntryType.LAST_VALID));
      } else if(!_rowId.isValid()) {
        // this is a "special" entry (first/last)
        _type = ((_rowId.getType() == RowId.Type.ALWAYS_FIRST) ?
                 EntryType.ALWAYS_FIRST : EntryType.ALWAYS_LAST);
      } else {
        throw new IllegalArgumentException("Values was null for valid entry");
      }
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
      _entryBytes = new byte[colEntryLen];
      buffer.get(_entryBytes);

      // read the rowId
      int page = ByteUtil.get3ByteInt(buffer, ByteOrder.BIG_ENDIAN);
      int row = ByteUtil.getUnsignedByte(buffer);
      
      _rowId = new RowId(page, row);
      _type = EntryType.NORMAL;
    }
    
    public RowId getRowId() {
      return _rowId;
    }

    public EntryType getType() {
      return _type;
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
    protected void write(ByteBuffer buffer) throws IOException {
      buffer.put(_entryBytes);
      ByteUtil.put3ByteInt(buffer, getRowId().getPageNumber(),
                           ByteOrder.BIG_ENDIAN);
      buffer.put((byte)getRowId().getRowNumber());
    }

    protected final String entryBytesToString() {
      return (isValid() ? ", Bytes = " + ByteUtil.toHexString(
                  ByteBuffer.wrap(_entryBytes), _entryBytes.length) :
              "");
    }
    
    @Override
    public String toString() {
      return "RowId = " + _rowId + entryBytesToString() + "\n";
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
    
  }

  /**
   * A single node entry in an index (points to a sub-page in the index)
   */
  private final class NodeEntry extends Entry {

    /** index page number of the page to which this node entry refers */
    private final int _subPageNumber;

    /**
     * Read an existing node entry in from a buffer
     */
    private NodeEntry(ByteBuffer buffer, int entryLen)
      throws IOException
    {
      // we need 4 trailing bytes for the sub-page number
      super(buffer, entryLen, 4);

      _subPageNumber = ByteUtil.getInt(buffer, ByteOrder.BIG_ENDIAN);
    }
    
    public int getSubPageNumber() {
      return _subPageNumber;
    }

    @Override
    protected int size() {
      // need 4 trailing bytes for the sub-page number
      return super.size() + 4;
    }
    
    @Override
    protected void write(ByteBuffer buffer) throws IOException {
      super.write(buffer);
      ByteUtil.putInt(buffer, _subPageNumber, ByteOrder.BIG_ENDIAN);
    }
    
    @Override
    public String toString() {
      return ("Node RowId = " + getRowId() +
              ", SubPage = " + _subPageNumber + entryBytesToString() + "\n");
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
    private final Position _firstPos;
    /** the last (exclusive) row id for this cursor */
    private final Position _lastPos;
    /** the first valid index for this cursor */
    private int _minIndex;
    /** the last valid index for this cursor */
    private int _maxIndex;
    /** the current entry */
    private Position _curPos;
    /** the previous entry */
    private Position _prevPos;
    /** the last read modification count on the Index.  we track this so that
        the cursor can detect updates to the index while traversing and act
        accordingly */
    private int _lastModCount;

    private EntryCursor(Position firstPos, Position lastPos) {
      _firstPos = firstPos;
      _lastPos = lastPos;
      // force bounds to be updated
      _lastModCount = Index.this._modCount - 1;
      reset();
    }

    public Index getIndex() {
      return Index.this;
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
     * Returns the DirHandler for the given direction
     */
    private DirHandler getDirHandler(boolean moveForward) {
      return (moveForward ? _forwardDirHandler : _reverseDirHandler);
    }

    /**
     * Returns {@code true} if this cursor is up-to-date with respect to its
     * index.
     */
    public boolean isUpToDate() {
      return(Index.this._modCount == _lastModCount);
    }
        
    public void reset() {
      beforeFirst();
    }

    public void beforeFirst() {
      reset(true);
    }

    public void afterLast() {
      reset(false);
    }

    protected void reset(boolean moveForward) {
      _curPos = getDirHandler(moveForward).getBeginningPosition();
      _prevPos = _curPos;
      if(!isUpToDate()) {
        // update bounds
        updateBounds();
        _lastModCount = Index.this._modCount;
      }
    }

    /**
     * Repositions the cursor so that the next row will be the first entry
     * >= the given row.
     */
    public void beforeEntry(Object[] row)
      throws IOException
    {
      restorePosition(
          new Entry(Index.this.createEntryBytes(row), RowId.FIRST_ROW_ID));
    }
    
    /**
     * Repositions the cursor so that the previous row will be the first
     * entry <= the given row.
     */
    public void afterEntry(Object[] row)
      throws IOException
    {
      restorePosition(
          new Entry(Index.this.createEntryBytes(row), RowId.LAST_ROW_ID));
    }
    
    /**
     * @return valid entry if there was a next entry,
     *         {@code #getLastEntry} otherwise
     */
    public Entry getNextEntry() {
      return getAnotherEntry(true);
    }

    /**
     * @return valid entry if there was a next entry,
     *         {@code #getFirstEntry} otherwise
     */
    public Entry getPreviousEntry() {
      return getAnotherEntry(false);
    }

    /**
     * Restores a current position for the cursor (current position becomes
     * previous position).
     */
    private void restorePosition(Entry curEntry) {
      restorePosition(curEntry, _curPos.getEntry());
    }
    
    /**
     * Restores a current and previous position for the cursor.
     */
    protected void restorePosition(Entry curEntry, Entry prevEntry)
    {
      if(!curEntry.equals(_curPos.getEntry()) ||
         !prevEntry.equals(_prevPos.getEntry()))
      {
        _prevPos = updatePosition(prevEntry);
        _curPos = updatePosition(curEntry);
        if(!isUpToDate()) {
          updateBounds();
          _lastModCount = Index.this._modCount;
        }
      } else {
        checkForModification();
      }
    }

    /**
     * Checks the index for modifications and updates state accordingly.
     */
    private void checkForModification() {
      if(!isUpToDate()) {
        _prevPos = updatePosition(_prevPos.getEntry());
        _curPos = updatePosition(_curPos.getEntry());
        updateBounds();
        _lastModCount = Index.this._modCount;
      }
    }

    private void updateBounds() {
      int idx = findEntry(_firstPos.getEntry());
      if(idx < 0) {
        idx = missingIndexToInsertionPoint(idx);
      }
      _minIndex = idx;

      idx = findEntry(_lastPos.getEntry());
      if(idx < 0) {
        idx = missingIndexToInsertionPoint(idx) - 1;
      }
      _maxIndex = idx;
    }
    
    /**
     * Gets an up-to-date position for the given entry.
     */
    private Position updatePosition(Entry entry) {
      if(entry.isValid()) {
        
        // find the new position for this entry
        int curIdx = findEntry(entry);
        boolean between = false;
        if(curIdx < 0) {
          // given entry was not found exactly.  our current position is now
          // really between two indexes, but we cannot support that as an
          // integer value so we set a flag instead
          curIdx = missingIndexToInsertionPoint(curIdx);
          between = true;
        }

        if(curIdx < _minIndex) {
          curIdx = _minIndex;
          between = true;
        } else if(curIdx > _maxIndex) {
          curIdx = _maxIndex + 1;
          between = true;
        }
        
        return new Position(curIdx, entry, between);
        
      } else if(entry.equals(_firstPos.getEntry())) {
        return _firstPos;
      } else if(entry.equals(_lastPos.getEntry())) {
        return _lastPos;
      } else {
        throw new IllegalArgumentException("Invalid entry given: " + entry);
      }
    }
    
    /**
     * Gets another entry in the given direction, returning the new entry.
     */
    private Entry getAnotherEntry(boolean moveForward) {
      DirHandler handler = getDirHandler(moveForward);
      if(_curPos.equals(handler.getEndPosition())) {
        if(!isUpToDate()) {
          restorePosition(_prevPos.getEntry());
          // drop through and retry moving to another entry
        } else {
          // at end, no more
          return _curPos.getEntry();
        }
      }

      checkForModification();

      _prevPos = _curPos;
      _curPos = handler.getAnotherPosition(_curPos.getIndex(),
                                           _curPos.isBetween());
      return _curPos.getEntry();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " CurPosition " + _curPos +
        ", PrevPosition " + _prevPos;
    }
    
    /**
     * Handles moving the cursor in a given direction.  Separates cursor
     * logic from value storage.
     */
    private abstract class DirHandler {
      public abstract Position getAnotherPosition(int curIdx, boolean between);
      public abstract Position getBeginningPosition();
      public abstract Position getEndPosition();
      protected final Position newPosition(int curIdx) {
        return new Position(curIdx, _entries.get(curIdx));
      }
      protected final Position newForwardPosition(int curIdx) {
        return((curIdx <= _maxIndex) ?
               newPosition(curIdx) : _lastPos);
      }
      protected final Position newReversePosition(int curIdx) {
        return ((curIdx >= _minIndex) ?
                newPosition(curIdx) : _firstPos);
      }
    }
        
    /**
     * Handles moving the cursor forward.
     */
    private final class ForwardDirHandler extends DirHandler {
      @Override
      public Position getAnotherPosition(int curIdx, boolean between) {
        // note, curIdx does not need to be advanced if it was pointing at a
        // between position
        if(!between) {
          curIdx = ((curIdx == getBeginningPosition().getIndex()) ?
                    _minIndex : (curIdx + 1));
        }
        return newForwardPosition(curIdx);
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
      public Position getAnotherPosition(int curIdx, boolean between) {
        // note, we ignore the between flag here because the index will be
        // pointing at the correct next index in either the between or
        // non-between case
        curIdx = ((curIdx == getBeginningPosition().getIndex()) ?
                  _maxIndex : (curIdx - 1));
        return newReversePosition(curIdx);
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
  private static class Position {
    /** the last known index of the given entry */
    private final int _idx;
    /** the entry at the given index */
    private final Entry _entry;
    /** {@code true} if this entry does not currently exist in the entry list,
        {@code false} otherwise */
    private final boolean _between;

    private Position(int idx, Entry entry) {
      this(idx, entry, false);
    }
    
    private Position(int idx, Entry entry, boolean between) {
      _idx = idx;
      _entry = entry;
      _between = between;
    }

    public int getIndex() {
      return _idx;
    }

    public Entry getEntry() {
      return _entry;
    }

    public boolean isBetween() {
      return _between;
    }

    @Override
    public int hashCode() {
      return _entry.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
      return((this == o) ||
             ((o != null) && (getClass() == o.getClass()) &&
              (_idx == ((Position)o)._idx) &&
              _entry.equals(((Position)o)._entry) &&
              (_between == ((Position)o)._between)));
    }

    @Override
    public String toString() {
      return "Idx = " + _idx + ", Entry = " + _entry + ", Between = " +
        _between;
    }
  }

  private static final class ExtraCodes {
    public final int _charOffset;
    public final byte[] _extraCodes;

    private ExtraCodes(int charOffset, byte[] extraCodes) {
      _charOffset = charOffset;
      _extraCodes = extraCodes;
    }
  }
  
}
