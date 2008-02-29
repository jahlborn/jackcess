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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



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
          return ((ByteUtil.toUnsignedInt(left[pos]) <
                   ByteUtil.toUnsignedInt(right[pos])) ? -1 : 1);
        }
        return ((left.length < right.length) ? -1 :
                ((left.length > right.length) ? 1 : 0));
      }
    };
        
  
  /**
   * Map of character to byte[] that Access uses in indexes (not ASCII)
   *    (Character -> byte[]) as codes to order text
   */
  private static final Map<Character, byte[]> CODES =
    new HashMap<Character, byte[]>();
  /**
   * Map of character to byte[] that Access uses in indexes (not ASCII)
   *    (Character -> byte[]), in the extended portion
   */
  private static final Map<Character, byte[]> CODES_EXT =
    new HashMap<Character, byte[]>();
  static {

    CODES.put('^', new byte[]{(byte)43, (byte)2});
    CODES.put('_', new byte[]{(byte)43, (byte)3});
    CODES.put('`', new byte[]{(byte)43, (byte)7});
    CODES.put('{', new byte[]{(byte)43, (byte)9});
    CODES.put('|', new byte[]{(byte)43, (byte)11});
    CODES.put('}', new byte[]{(byte)43, (byte)13});
    CODES.put('~', new byte[]{(byte)43, (byte)15});
    
    CODES.put('\t', new byte[]{(byte)8, (byte)3});
    CODES.put('\r', new byte[]{(byte)8, (byte)4});
    CODES.put('\n', new byte[]{(byte)8, (byte)7});
        
    CODES.put(' ', new byte[]{(byte)7});
    CODES.put('!', new byte[]{(byte)9});
    CODES.put('"', new byte[]{(byte)10});
    CODES.put('#', new byte[]{(byte)12});
    CODES.put('$', new byte[]{(byte)14});
    CODES.put('%', new byte[]{(byte)16});
    CODES.put('&', new byte[]{(byte)18});
    CODES.put('(', new byte[]{(byte)20});
    CODES.put(')', new byte[]{(byte)22});
    CODES.put('*', new byte[]{(byte)24});
    CODES.put(',', new byte[]{(byte)26});
    CODES.put('.', new byte[]{(byte)28});
    CODES.put('/', new byte[]{(byte)30});
    CODES.put(':', new byte[]{(byte)32});
    CODES.put(';', new byte[]{(byte)34});
    CODES.put('?', new byte[]{(byte)36});
    CODES.put('@', new byte[]{(byte)38});    
    CODES.put('[', new byte[]{(byte)39});
    CODES.put('\\', new byte[]{(byte)41});
    CODES.put(']', new byte[]{(byte)42});
    CODES.put('+', new byte[]{(byte)44});
    CODES.put('<', new byte[]{(byte)46});
    CODES.put('=', new byte[]{(byte)48});
    CODES.put('>', new byte[]{(byte)50});
    CODES.put('0', new byte[]{(byte)54});
    CODES.put('1', new byte[]{(byte)56});
    CODES.put('2', new byte[]{(byte)58});
    CODES.put('3', new byte[]{(byte)60});
    CODES.put('4', new byte[]{(byte)62});
    CODES.put('5', new byte[]{(byte)64});
    CODES.put('6', new byte[]{(byte)66});
    CODES.put('7', new byte[]{(byte)68});
    CODES.put('8', new byte[]{(byte)70});
    CODES.put('9', new byte[]{(byte)72});
    CODES.put('A', new byte[]{(byte)74});
    CODES.put('B', new byte[]{(byte)76});
    CODES.put('C', new byte[]{(byte)77});
    CODES.put('D', new byte[]{(byte)79});
    CODES.put('E', new byte[]{(byte)81});
    CODES.put('F', new byte[]{(byte)83});
    CODES.put('G', new byte[]{(byte)85});
    CODES.put('H', new byte[]{(byte)87});
    CODES.put('I', new byte[]{(byte)89});
    CODES.put('J', new byte[]{(byte)91});
    CODES.put('K', new byte[]{(byte)92});
    CODES.put('L', new byte[]{(byte)94});
    CODES.put('M', new byte[]{(byte)96});
    CODES.put('N', new byte[]{(byte)98});
    CODES.put('O', new byte[]{(byte)100});
    CODES.put('P', new byte[]{(byte)102});
    CODES.put('Q', new byte[]{(byte)104});
    CODES.put('R', new byte[]{(byte)105});
    CODES.put('S', new byte[]{(byte)107});
    CODES.put('T', new byte[]{(byte)109});
    CODES.put('U', new byte[]{(byte)111});
    CODES.put('V', new byte[]{(byte)113});
    CODES.put('W', new byte[]{(byte)115});
    CODES.put('X', new byte[]{(byte)117});
    CODES.put('Y', new byte[]{(byte)118});
    CODES.put('Z', new byte[]{(byte)120});

    CODES_EXT.put('\'', new byte[]{(byte)6, (byte)128});
    CODES_EXT.put('-', new byte[]{(byte)6, (byte)130});
  }

  /** owning table */
  private final Table _table;
  /** Page number of the index data */
  private int _pageNumber;
  /** Number of rows in the index
      NOTE: this does not actually seem to be the row count, unclear what the
      value means*/
  private int _rowCount;
  /** sorted collection of index entries.  this is kept in a list instead of a
      SortedSet because the SortedSet has lame traversal utilities */
  private final List<Entry> _entries = new ArrayList<Entry>();
  /** Map of columns to flags */
  private final Map<Column, Byte> _columns = new LinkedHashMap<Column, Byte>();
  /** 0-based index number */
  private int _indexNumber;
  /** Index name */
  private String _name;
  /** is this index a primary key */
  private boolean _primaryKey;
  /** <code>true</code> if the index entries have been initialized,
      <code>false</code> otherwise */
  private boolean _initialized;
  /** modification count for the table, keeps cursors up-to-date */
  private int _modCount;
  /** FIXME, for now, we can't write multi-page indexes or indexes using the funky primary key compression scheme */
  boolean _readOnly;
  
  public Index(Table table) {
    _table  = table;
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
  
  public void setRowCount(int rowCount) {
    _rowCount = rowCount;
  }

  public int getRowCount() {
    return _rowCount;
  }

  /**
   * Note, there may still be some issues around the name of an index, this
   * information may not be correct.  I've done a variety of testing comparing
   * the index name to what ms access shows, and i think the data is being
   * parsed correctly, but sometimes access comes up with a completely
   * different index name, hence my lack of confidence in this method.  (of
   * course, access could also just be doing some monkeying under the
   * hood...).
   */
  public String getName() {
    return _name;
  }
  
  public void setName(String name) {
    _name = name;
  }

  public boolean isPrimaryKey() {
    return _primaryKey;
  }

  public void setPrimaryKey(boolean newPrimaryKey) {
    _primaryKey = newPrimaryKey;
  }

  /**
   * Returns the Columns for this index (unmodifiable)
   */
  public Collection<Column> getColumns() {
    return Collections.unmodifiableCollection(_columns.keySet());
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
    ByteBuffer buffer = getPageChannel().createPageBuffer();
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
      Byte flags = Byte.valueOf(tableBuffer.get());
      if (columnNumber != COLUMN_UNUSED) {
        _columns.put(availableColumns.get(columnNumber), flags);
      }
    }
    tableBuffer.getInt(); //Forward past Unknown
    _pageNumber = tableBuffer.getInt();
    tableBuffer.position(tableBuffer.position() + 10);  //Forward past other stuff
  }

  /**
   * Reads the actual index entries.
   */
  private void readIndexEntries()
    throws IOException
  {
    // use sorted set initially to do the bulk of the sorting
    SortedSet<Entry> tmpEntries = new TreeSet<Entry>();
    
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

    // read all leaf pages
    while(true) {

      leafPageNumber = readLeafPage(indexPage, tmpEntries);
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

    // dump all the entries (sorted) into the actual _entries list
    _entries.addAll(tmpEntries);
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
    int numCompressedBytes = indexPage.get(
        getFormat().OFFSET_INDEX_COMPRESSED_BYTE_COUNT);
    int entryMaskLength = getFormat().SIZE_INDEX_ENTRY_MASK;
    int entryMaskPos = getFormat().OFFSET_INDEX_ENTRY_MASK;
    int entryPos = entryMaskPos + getFormat().SIZE_INDEX_ENTRY_MASK;
    int lastStart = 0;
    byte[] valuePrefix = null;
    boolean firstEntry = true;
    ByteBuffer tmpEntryBuffer = null;

    for (int i = 0; i < entryMaskLength; i++) {
      byte entryMask = indexPage.get(entryMaskPos + i);
      for (int j = 0; j < 8; j++) {
        if ((entryMask & (1 << j)) != 0) {
          int length = (i * 8) + j - lastStart;
          indexPage.position(entryPos + lastStart);
          int startReadPos = indexPage.position();

          // determine if we can read straight from the index page (if no
          // valuePrefix).  otherwise, create temp buf with complete entry.
          ByteBuffer curEntryBuffer = indexPage;
          int curEntryLen = length;
          if(valuePrefix != null) {
            tmpEntryBuffer = getTempEntryBuffer(
                indexPage, length, valuePrefix, tmpEntryBuffer);
            curEntryBuffer = tmpEntryBuffer;
            curEntryLen += valuePrefix.length;
          }
          
          if(isLeaf) {
            entries.add(new Entry(curEntryBuffer, curEntryLen, _columns));
          } else {
            nodeEntries.add(new NodeEntry(
                                curEntryBuffer, curEntryLen, _columns));
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
      ByteBuffer tmpEntryBuffer)
  {
    int totalLen = entryLen + valuePrefix.length;
    if((tmpEntryBuffer == null) || (tmpEntryBuffer.capacity() < totalLen)) {
      tmpEntryBuffer = ByteBuffer.allocate(totalLen);
    } else {
      tmpEntryBuffer.clear();
    }

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
   * @param pageNumber Page number on which the row is stored
   * @param rowNumber Row number at which the row is stored
   */
  public void addRow(Object[] row, RowId rowId)
    throws IOException
  {
    // make sure we've parsed the entries
    initialize();

    Entry newEntry = new Entry(row, rowId, _columns);
    if(addEntry(newEntry)) {
      ++_rowCount;
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
   * @param pageNumber Page number on which the row is removed
   * @param rowNumber Row number at which the row is removed
   */
  public void deleteRow(Object[] row, RowId rowId)
    throws IOException
  {
    // make sure we've parsed the entries
    initialize();

    Entry oldEntry = new Entry(row, rowId, _columns);
    if(removeEntry(oldEntry)) {
      --_rowCount;
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
    if(startRow != null) {
      Entry startEntry = new Entry(startRow,
                                   (startInclusive ?
                                    RowId.FIRST_ROW_ID : RowId.LAST_ROW_ID),
                                   _columns);
      startPos = new Position(FIRST_ENTRY_IDX, startEntry);
    }
    Position endPos = LAST_POSITION;
    if(endRow != null) {
      Entry endEntry = new Entry(endRow,
                                 (endInclusive ?
                                  RowId.LAST_ROW_ID : RowId.FIRST_ROW_ID),
                                 _columns);
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
  private boolean addEntry(Entry newEntry) {
    int idx = findEntry(newEntry);
    if(idx < 0) {
      // this is a new entry
      idx = missingIndexToInsertionPoint(idx);
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
    Object[] idxRow = new Object[getTable().getMaxColumnCount()];
    for(Column col : _columns.keySet()) {
      idxRow[col.getColumnNumber()] = values[valIdx++];
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
    for(Column col : _columns.keySet()) {
      if(!row.containsKey(col.getName())) {
        return null;
      }
    }

    Object[] idxRow = new Object[getTable().getMaxColumnCount()];
    for(Column col : _columns.keySet()) {
      idxRow[col.getColumnNumber()] = row.get(col.getName());
    }
    return idxRow;
  }  

  @Override
  public String toString() {
    StringBuilder rtn = new StringBuilder();
    rtn.append("\tName: " + _name);
    rtn.append("\n\tNumber: " + _indexNumber);
    rtn.append("\n\tPage number: " + _pageNumber);
    rtn.append("\n\tIs Primary Key: " + _primaryKey);
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

  private static void checkColumnType(Column col)
    throws IOException
  {
    if(col.isVariableLength() && !isTextualColumn(col)) {
      throw new IOException("unsupported index column type: " +
                            col.getType());
    }
  }      

  private static boolean isTextualColumn(Column col) {
    return((col.getType() == DataType.TEXT) ||
           (col.getType() == DataType.MEMO));
  }

  // FIXME
//   private static boolean isFloatingPointColumn(Column col) {
//     return((col.getType() == DataType.FLOAT) ||
//            (col.getType() == DataType.DOUBLE));
//   }

  /**
   * Converts an index value for a fixed column into the index bytes
   */
  // FIXME
//   private static void toIndexFixedValue(
//       Entry.FixedEntryColumn entryCol,
//       Object value,
//       byte flags)
//     throws IOException
//   {
//     if(value == null) {
//       // nothing more to do
//       return;
//     }

//     Column column = entryCol._column;
    
// //     if (value instanceof Integer) {
// //       value = Integer.valueOf((int) (((Integer) value).longValue() -
// //                                  ((long) Integer.MAX_VALUE + 1L)));
// //     } else if (value instanceof Short) {
// //       value = Short.valueOf((short) (((Short) value).longValue() -
// //                                  ((long) Integer.MAX_VALUE + 1L)));
// //     }

//     byte[] value = column.write(value, 0, ByteOrder.BIG_ENDIAN);
    
//     if(isFloatingPointColumn(column)) {
//       if(((Number)value).doubleValue() < 0) {
//         // invert all the bits
//         for(int i = 0; i < value.length; ++i) {
//           value[i] = (byte)~value[i];
//         }
//       }
//     } else {
//       // invert the highest bit
//       value[0] = (byte)((value[0] ^ 0x80) & 0xFF);
//     }
    
    
//   }
  
  /**
   * Converts an index value for a text column into the value which
   * is based on a variety of nifty codes.
   */
  private static void toIndexTextValue(
      Entry.TextEntryColumn entryCol,
      Object value,
      byte flags)
    throws IOException
  {
    if(value == null) {
      // nothing more to do
      return;
    }

    // first, convert to uppercase string (all text characters are uppercase)
    String str = Column.toCharSequence(value).toString().toUpperCase();

    // now, convert each character to a "code" of one or more bytes
    ByteArrayOutputStream bout = new ByteArrayOutputStream(str.length());
    ByteArrayOutputStream boutExt = null;
    for(int i = 0; i < str.length(); ++i) {
      char c = str.charAt(i);

      byte[] bytes = CODES.get(c);
      if(bytes != null) {
        bout.write(bytes);
      } else {
        bytes = CODES_EXT.get(c);
        if(bytes != null) {
          // add extra chars
          if(boutExt == null) {
            boutExt = new ByteArrayOutputStream(7);
            // setup funky extra bytes
            boutExt.write(1);
            boutExt.write(1);
            boutExt.write(1);
          }

          // FIXME, complete me..
          
          // no clue where this comes from...
          int offset = 7 + (i * 4);
          boutExt.write((byte)0x80);
          boutExt.write((byte)offset);
          boutExt.write(bytes);
            
        } else {
          throw new IOException("unmapped string index value");
        }
      }
      
    }

    entryCol._valueBytes = bout.toByteArray();
    if(boutExt != null) {
      entryCol._extraBytes = boutExt.toByteArray();
    }
  }

  /**
   * Creates one of the special index entries.
   */
  private static Entry createSpecialEntry(RowId rowId) {
    try {
      return new Entry(null, rowId, null);
    } catch(IOException e) {
      // should never happen
      throw new IllegalStateException(e);
    }
  }
  
  /**
   * A single leaf entry in an index (points to a single row)
   */
  public static class Entry implements Comparable<Entry>
  {
    
    /** page/row on which this row is stored */
    private final RowId _rowId;
    /** Columns that are indexed */
    private final List<EntryColumn> _entryColumns;
    
    /**
     * Create a new entry
     * @param values Indexed row values
     * @param page Page number on which the row is stored
     * @param rowNumber Row number at which the row is stored
     */
    private Entry(Object[] values, RowId rowId,
                  Map<Column, Byte> columns)
      throws IOException
    {
      _rowId = rowId;
      if(values != null) {
        _entryColumns = new ArrayList<EntryColumn>();
        for(Map.Entry<Column, Byte> entry : columns.entrySet()) {
          Column col = entry.getKey();
          Byte flags = entry.getValue();
          Object value = values[col.getColumnNumber()];
          _entryColumns.add(newEntryColumn(col).initFromValue(value, flags));
        }
      } else {
        if(!_rowId.isValid()) {
          // this is a "special" entry (first/last)
          _entryColumns = null;
        } else {
          throw new IllegalArgumentException("Values was null");
        }
      }
    }

    /**
     * Read an existing entry in from a buffer
     */
    private Entry(ByteBuffer buffer, int entryLen, 
                  Map<Column, Byte> columns)
      throws IOException
    {
      this(buffer, entryLen, columns, 0);
    }
    
    /**
     * Read an existing entry in from a buffer
     */
    private Entry(ByteBuffer buffer, int entryLen, 
                  Map<Column, Byte> columns, int extraTrailingLen)
      throws IOException
    {
      // we need 4 trailing bytes for the rowId, plus whatever the caller
      // wants
      int trailingByteLen = 4 + extraTrailingLen;

      int colEntryLen = entryLen - trailingByteLen;
      
      _entryColumns = new ArrayList<EntryColumn>();
      for(Map.Entry<Column, Byte> entry : columns.entrySet()) {
        Column col = entry.getKey();
        Byte flags = entry.getValue();
        int startCurEntryPos = buffer.position();
        _entryColumns.add(newEntryColumn(col)
                          .initFromBuffer(buffer, flags, colEntryLen));
        int curEntryLen = buffer.position() - startCurEntryPos;
        if(curEntryLen > colEntryLen) {
          throw new IOException("could not parse entry column, expected " +
                                colEntryLen + ", read " + curEntryLen);
        }
        colEntryLen -= curEntryLen;
      }
      int page = ByteUtil.get3ByteInt(buffer, ByteOrder.BIG_ENDIAN);
      int row = buffer.get();
      _rowId = new RowId(page, row);
    }

    /**
     * Instantiate the correct EntryColumn for the given column type
     */
    private EntryColumn newEntryColumn(Column col) throws IOException
    {
      if(isTextualColumn(col)) {
        return new TextEntryColumn(col);
      }
      return new FixedEntryColumn(col);
    }
    
    protected List<EntryColumn> getEntryColumns() {
      return _entryColumns;
    }

    public RowId getRowId() {
      return _rowId;
    }

    public boolean isValid() {
      return(_entryColumns != null);
    }
    
    /**
     * Size of this entry in the db.
     */
    protected int size() {
      int rtn = 4;
      for(EntryColumn entryCol : _entryColumns) {
        rtn += entryCol.size();
      }
      return rtn;
    }
    
    /**
     * Write this entry into a buffer
     */
    protected void write(ByteBuffer buffer) throws IOException {
      for(EntryColumn entryCol : _entryColumns) {
        entryCol.write(buffer);
      }
      int page = getRowId().getPageNumber();
      buffer.put((byte) (page >>> 16));
      buffer.put((byte) (page >>> 8));
      buffer.put((byte) page);
      buffer.put((byte)getRowId().getRowNumber());
    }
    
    @Override
    public String toString() {
      return ("RowId = " + _rowId + ", Columns = " + _entryColumns + "\n");
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
    
    public int compareTo(Entry other) {
      if (this == other) {
        return 0;
      }

      // note, if the one or both of the entries are not valid, they are
      // "special" entries, which are handled below
      if(isValid() && other.isValid()) {

        // comparing two normal entries
        Iterator<EntryColumn> myIter = _entryColumns.iterator();
        Iterator<EntryColumn> otherIter = other.getEntryColumns().iterator();
        while (myIter.hasNext()) {
          if (!otherIter.hasNext()) {
            throw new IllegalArgumentException(
                "Trying to compare index entries with a different number of entry columns");
          }
          EntryColumn myCol = myIter.next();
          EntryColumn otherCol = otherIter.next();
          int i = myCol.compareTo(otherCol);
          if (i != 0) {
            return i;
          }
        }

        // if entry columns are equal, sort by rowIds
        return _rowId.compareTo(other.getRowId());
      }

      // this is the odd case where mixed entries are being compared.  if both
      // entries are invalid or the rowIds are not equal, then use the rowId
      // comparison.
      int rowCmp = _rowId.compareTo(other.getRowId());
      if((isValid() == other.isValid()) || (rowCmp != 0)) {
        return rowCmp;
      }

      // at this point, the rowId's are equal, but the validity is not.  this
      // will happen when a "special" entry is compared to something created
      // by EntryCursor.afterEntry or EntryCursor.beforeEntry.  in this case,
      // the FIRST_ENTRY is always least and the LAST_ENTRY is always
      // greatest.
      int cmp = 0;
      Entry invalid = null;
      if(!isValid()) {
        cmp = -1;
        invalid = this;
      } else {
        cmp = 1;
        invalid = other;
      }
      return (cmp * (invalid.equals(FIRST_ENTRY) ? 1 : -1));
    }
    

    /**
     * A single column value within an index Entry; encapsulates column
     * definition and column value.
     */
    private abstract class EntryColumn implements Comparable<EntryColumn>
    {
      /** Column definition */
      protected Column _column;
    
      protected EntryColumn(Column col) throws IOException {
        checkColumnType(col);
        _column = col;
      }

      public int size() {
        int size = 1;
        if (!isNullValue()) {
          size += nonNullSize();
        }
        return size;
      }

      /**
       * Initialize using a new value
       */
      protected abstract EntryColumn initFromValue(Object value,
                                                   byte flags)
        throws IOException;

      /**
       * Initialize from a buffer
       */
      protected abstract EntryColumn initFromBuffer(ByteBuffer buffer,
                                                    byte flags,
                                                    int colEntryLen)
        throws IOException;

      protected abstract boolean isNullValue();
      
      /**
       * Write this entry column to a buffer
       */
      public void write(ByteBuffer buffer) throws IOException
      {
        if(isNullValue()) {
          buffer.put((byte)0);
        } else {
          buffer.put((byte) 0x7F);
          writeNonNullValue(buffer);
        }
      }

      /**
       * Write this non-null entry column to a buffer
       */
      protected abstract void writeNonNullValue(ByteBuffer buffer)
          throws IOException;
      
      protected abstract int nonNullSize();

      public abstract int compareTo(EntryColumn other);
    }

    /**
     * A single fixed column value within an index Entry; encapsulates column
     * definition and column value.
     */
    private final class FixedEntryColumn extends EntryColumn
    {
      /** Column value */
      private Comparable _value;
    
      private FixedEntryColumn(Column col) throws IOException {
        super(col);
        if(isTextualColumn(col)) {
          throw new IOException("must be fixed column");
        }
      }

      /**
       * Initialize using a new value
       */
      @Override
      protected EntryColumn initFromValue(Object value, byte flags)
        throws IOException
      {
        _value = (Comparable)value;
      
        return this;
      }

      /**
       * Initialize from a buffer
       */
      @Override
      protected EntryColumn initFromBuffer(ByteBuffer buffer,
                                           byte flags,
                                           int colEntryLen)
        throws IOException
      {
        // FIXME, eventually take colEntryLen into account
        
        byte flag = buffer.get();
        // FIXME, reverse is 0x80, reverse null is 0xFF
        if ((flag != (byte) 0) && (flag != (byte)0xFF)) {
          byte[] data = new byte[_column.getType().getFixedSize()];
          buffer.get(data);
          _value = (Comparable) _column.read(data, ByteOrder.BIG_ENDIAN);
          
          //ints and shorts are stored in index as value + 2147483648
          if (_value instanceof Integer) {
            _value = Integer.valueOf((int) (((Integer) _value).longValue() +
                                        (long) Integer.MAX_VALUE + 1L)); 
          } else if (_value instanceof Short) {
            _value = Short.valueOf((short) (((Short) _value).longValue() +
                                        (long) Integer.MAX_VALUE + 1L));
          }
        }
        
        return this;
      }

      @Override
      protected boolean isNullValue() {
        return(_value == null);
      }
      
      /**
       * Write this entry column to a buffer
       */
      @Override
      protected void writeNonNullValue(ByteBuffer buffer) throws IOException {
        Comparable value = _value;
        if (value instanceof Integer) {
          value = Integer.valueOf((int) (((Integer) value).longValue() -
                                     ((long) Integer.MAX_VALUE + 1L)));
        } else if (value instanceof Short) {
          value = Short.valueOf((short) (((Short) value).longValue() -
                                     ((long) Integer.MAX_VALUE + 1L)));
        }
        buffer.put(_column.write(value, 0, ByteOrder.BIG_ENDIAN));
      }
    
      @Override
      protected int nonNullSize() {
        return _column.getType().getFixedSize();
      }

      @Override
      public String toString() {
        return String.valueOf(_value);
      }
        
      @Override
      public int compareTo(EntryColumn other) {
        return new CompareToBuilder()
          .append(_value, ((FixedEntryColumn)other)._value)
          .toComparison();
      }
    }

  
    /**
     * A single textual column value within an index Entry; encapsulates
     * column definition and column value.
     */
    private final class TextEntryColumn extends EntryColumn
    {
      /** the string byte codes */
      private byte[] _valueBytes;
      /** extra column bytes */
      private byte[] _extraBytes;
      /** whether or not the trailing bytes were found */
      private boolean _hasTrailingBytes;
    
      private TextEntryColumn(Column col) throws IOException {
        super(col);
        if(!isTextualColumn(col)) {
          throw new IOException("must be textual column");
        }
      }

      /**
       * Initialize using a new value
       */
      @Override
      protected EntryColumn initFromValue(Object value,
                                          byte flags)
        throws IOException
      {
        // convert string to byte array
        toIndexTextValue(this, value, flags);
      
        return this;
      }

      /**
       * Initialize from a buffer
       */
      @Override
      protected EntryColumn initFromBuffer(ByteBuffer buffer,
                                           byte flags,
                                           int colEntryLen)
        throws IOException
      {
        // can't read more than colEntryLen
        int maxPos = buffer.position() + colEntryLen;

        byte flag = buffer.get();
        // FIXME, reverse is 0x80, reverse null is 0xFF
        // end flag is FE, post extra bytes is FF 00
        // extra bytes are inverted, so are normal bytes
        if ((flag != (byte) 0) && (flag != (byte)0xFF)) {

          int endPos = buffer.position();
          _hasTrailingBytes = true;
          while(buffer.get(endPos) != (byte) 1) {
            if(endPos == maxPos) {
              _hasTrailingBytes = false;
              break;
            }
            ++endPos;
          }

          // read index bytes
          int numPrefixBytes = 0;
          int dataOffset = 0;
          _valueBytes = new byte[endPos - buffer.position()];
          buffer.get(_valueBytes);

          if(_hasTrailingBytes) {
            
            // read end codes byte
            buffer.get();
          
            //Forward past 0x00 (in some cases, there is more data here, which
            //we don't currently understand)
            byte endByte = buffer.get();
            if(endByte != (byte)0x00) {
              endPos = buffer.position() - 1;
              buffer.position(endPos);
              while(buffer.get(endPos) != (byte)0x00) {
                ++endPos;
              }
              _extraBytes = new byte[endPos - buffer.position()];
              buffer.get(_extraBytes);

              // re-get endByte
              buffer.get();
            }
          }
        }

        return this;
      }

      @Override
      protected boolean isNullValue() {
        return(_valueBytes == null);
      }
      
      /**
       * Write this entry column to a buffer
       */
      @Override
      protected void writeNonNullValue(ByteBuffer buffer) throws IOException {
        buffer.put(_valueBytes);
        if(_hasTrailingBytes) {
          buffer.put((byte) 1);
          if(_extraBytes != null) {
            buffer.put(_extraBytes);
          }
          buffer.put((byte) 0);
        }
      }

      @Override
      protected int nonNullSize() {
        int rtn = _valueBytes.length;
        if(_hasTrailingBytes) {
          rtn += 2;
          if(_extraBytes != null) {
            rtn += _extraBytes.length;
          }
        }
        return rtn;
      }

      @Override
      public String toString() {
        if(_valueBytes == null) {
          return String.valueOf(_valueBytes);
        }

        String rtn = ByteUtil.toHexString(ByteBuffer.wrap(_valueBytes),
                                          _valueBytes.length);
        if(_extraBytes != null) {
          rtn += " (" + ByteUtil.toHexString(ByteBuffer.wrap(_extraBytes),
                                             _extraBytes.length) + ")";
        }
        
        return rtn;
      }
        
      @Override
      public int compareTo(EntryColumn other) {
        TextEntryColumn textOther = (TextEntryColumn)other;
        int rtn = BYTE_CODE_COMPARATOR.compare(
            _valueBytes, textOther._valueBytes);
        if(rtn != 0) {
          return rtn;
        }
        if(_hasTrailingBytes != textOther._hasTrailingBytes) {
          return(_hasTrailingBytes ? 1 : -1);
        }
        return BYTE_CODE_COMPARATOR.compare(
            _extraBytes, textOther._extraBytes);
      }
    
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
    private NodeEntry(ByteBuffer buffer, int entryLen,
                      Map<Column, Byte> columns)
      throws IOException
    {
      // we need 4 trailing bytes for the sub-page number
      super(buffer, entryLen, columns, 4);

      _subPageNumber = ByteUtil.getInt(buffer, ByteOrder.BIG_ENDIAN);
    }
    
    public int getSubPageNumber() {
      return _subPageNumber;
    }

    @Override
    public String toString() {
      return ("Node RowId = " + getRowId() +
              ", SubPage = " + _subPageNumber +
              ", Columns = " + getEntryColumns() + "\n");
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
      restorePosition(new Entry(row, RowId.FIRST_ROW_ID, _columns));
    }
    
    /**
     * Repositions the cursor so that the previous row will be the first
     * entry <= the given row.
     */
    public void afterEntry(Object[] row)
      throws IOException
    {
      restorePosition(new Entry(row, RowId.LAST_ROW_ID, _columns));
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
  
}
