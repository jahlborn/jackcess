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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.lang.builder.CompareToBuilder;

/**
 * Access table index
 * @author Tim McCune
 */
public class Index implements Comparable<Index> {
  
  /** Max number of columns in an index */
  private static final int MAX_COLUMNS = 10;
  
  private static final short COLUMN_UNUSED = -1;
  
  /**
   * Map of characters to bytes that Access uses in indexes (not ASCII)
   *    (Character -> Byte)
   */
  private static final BidiMap CODES = new DualHashBidiMap();
  static {
    //These values are prefixed with a '43'
    CODES.put('^', (byte) 2);
    CODES.put('_', (byte) 3);
    CODES.put('{', (byte) 9);
    CODES.put('|', (byte) 11);
    CODES.put('}', (byte) 13);
    CODES.put('~', (byte) 15);
    
    //These values aren't.
    CODES.put(' ', (byte) 7);
    CODES.put('#', (byte) 12);
    CODES.put('$', (byte) 14);
    CODES.put('%', (byte) 16);
    CODES.put('&', (byte) 18);
    CODES.put('(', (byte) 20);
    CODES.put(')', (byte) 22);
    CODES.put('*', (byte) 24);
    CODES.put(',', (byte) 26);
    CODES.put('/', (byte) 30);
    CODES.put(':', (byte) 32);
    CODES.put(';', (byte) 34);
    CODES.put('?', (byte) 36);
    CODES.put('@', (byte) 38);
    CODES.put('+', (byte) 44);
    CODES.put('<', (byte) 46);
    CODES.put('=', (byte) 48);
    CODES.put('>', (byte) 50);
    CODES.put('0', (byte) 54);
    CODES.put('1', (byte) 56);
    CODES.put('2', (byte) 58);
    CODES.put('3', (byte) 60);
    CODES.put('4', (byte) 62);
    CODES.put('5', (byte) 64);
    CODES.put('6', (byte) 66);
    CODES.put('7', (byte) 68);
    CODES.put('8', (byte) 70);
    CODES.put('9', (byte) 72);
    CODES.put('A', (byte) 74);
    CODES.put('B', (byte) 76);
    CODES.put('C', (byte) 77);
    CODES.put('D', (byte) 79);
    CODES.put('E', (byte) 81);
    CODES.put('F', (byte) 83);
    CODES.put('G', (byte) 85);
    CODES.put('H', (byte) 87);
    CODES.put('I', (byte) 89);
    CODES.put('J', (byte) 91);
    CODES.put('K', (byte) 92);
    CODES.put('L', (byte) 94);
    CODES.put('M', (byte) 96);
    CODES.put('N', (byte) 98);
    CODES.put('O', (byte) 100);
    CODES.put('P', (byte) 102);
    CODES.put('Q', (byte) 104);
    CODES.put('R', (byte) 105);
    CODES.put('S', (byte) 107);
    CODES.put('T', (byte) 109);
    CODES.put('U', (byte) 111);
    CODES.put('V', (byte) 113);
    CODES.put('W', (byte) 115);
    CODES.put('X', (byte) 117);
    CODES.put('Y', (byte) 118);
    CODES.put('Z', (byte) 120);
  }
  
  /** Page number of the index data */
  private int _pageNumber;
  private int _parentPageNumber;
  /** Number of rows in the index */
  private int _rowCount;
  private JetFormat _format;
  private SortedSet<Entry> _entries = new TreeSet<Entry>();
  /** Map of columns to order */
  private Map<Column, Byte> _columns = new LinkedHashMap<Column, Byte>();
  private PageChannel _pageChannel;
  /** 0-based index number */
  private int _indexNumber;
  /** Index name */
  private String _name;
  /** is this index a primary key */
  private boolean _primaryKey;
  
  public Index(int parentPageNumber, PageChannel channel, JetFormat format) {
    _parentPageNumber = parentPageNumber;
    _pageChannel = channel;
    _format = format;
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
  
  public void update() throws IOException {
    _pageChannel.writePage(write(), _pageNumber);
  }

  /**
   * Write this index out to a buffer
   */
  public ByteBuffer write() throws IOException {
    ByteBuffer buffer = _pageChannel.createPageBuffer();
    buffer.put((byte) 0x04);  //Page type
    buffer.put((byte) 0x01);  //Unknown
    buffer.putShort((short) 0); //Free space
    buffer.putInt(_parentPageNumber);
    buffer.putInt(0); //Prev page
    buffer.putInt(0); //Next page
    buffer.putInt(0); //Leaf page
    buffer.putInt(0); //Unknown
    buffer.put((byte) 0); //Unknown
    buffer.put((byte) 0); //Unknown
    buffer.put((byte) 0); //Unknown
    byte[] entryMask = new byte[_format.SIZE_INDEX_ENTRY_MASK];
    int totalSize = 0;
    Iterator iter = _entries.iterator();
    while (iter.hasNext()) {
      Entry entry = (Entry) iter.next();
      int size = entry.size();
      totalSize += size;
      int idx = totalSize  / 8;
      entryMask[idx] |= (1 << (totalSize % 8));
    }
    buffer.put(entryMask);
    iter = _entries.iterator();
    while (iter.hasNext()) {
      Entry entry = (Entry) iter.next();
      entry.write(buffer);
    }
    buffer.putShort(2, (short) (_format.PAGE_SIZE - buffer.position()));
    return buffer;
  }
  
  /**
   * Read this index in from a buffer
   * @param buffer Buffer to read from
   * @param availableColumns Columns that this index may use
   */
  public void read(ByteBuffer buffer, List<Column> availableColumns)
  throws IOException
  {
    for (int i = 0; i < MAX_COLUMNS; i++) {
      short columnNumber = buffer.getShort();
      Byte order = new Byte(buffer.get());
      if (columnNumber != COLUMN_UNUSED) {
        _columns.put(availableColumns.get(columnNumber), order);
      }
    }
    buffer.getInt(); //Forward past Unknown
    _pageNumber = buffer.getInt();
    buffer.position(buffer.position() + 10);  //Forward past other stuff
    ByteBuffer indexPage = _pageChannel.createPageBuffer();
    _pageChannel.readPage(indexPage, _pageNumber);
    indexPage.position(_format.OFFSET_INDEX_ENTRY_MASK);
    byte[] entryMask = new byte[_format.SIZE_INDEX_ENTRY_MASK];
    indexPage.get(entryMask);
    int lastStart = 0;
    for (int i = 0; i < entryMask.length; i++) {
      for (int j = 0; j < 8; j++) {
        if ((entryMask[i] & (1 << j)) != 0) {
          int length = i * 8 + j - lastStart;
          _entries.add(new Entry(indexPage));
          lastStart += length;
        }
      }
    }
  }
  
  /**
   * Add a row to this index
   * @param row Row to add
   * @param pageNumber Page number on which the row is stored
   * @param rowNumber Row number at which the row is stored
   */
  public void addRow(Object[] row, int pageNumber, byte rowNumber) {
    _entries.add(new Entry(row, pageNumber, rowNumber));
  }
  
  public String toString() {
    StringBuilder rtn = new StringBuilder();
    rtn.append("\tName: " + _name);
    rtn.append("\n\tNumber: " + _indexNumber);
    rtn.append("\n\tPage number: " + _pageNumber);
    rtn.append("\n\tIs Primary Key: " + _primaryKey);
    rtn.append("\n\tColumns: " + _columns);
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
   * A single entry in an index (points to a single row)
   */
  private class Entry implements Comparable<Entry> {
    
    /** Page number on which the row is stored */
    private int _page;
    /** Row number at which the row is stored */
    private byte _row;
    /** Columns that are indexed */
    private List<EntryColumn> _entryColumns = new ArrayList<EntryColumn>();
    
    /**
     * Create a new entry
     * @param values Indexed row values
     * @param page Page number on which the row is stored
     * @param rowNumber Row number at which the row is stored
     */
    public Entry(Object[] values, int page, byte rowNumber) {
      _page = page;
      _row = rowNumber;
      Iterator iter = _columns.keySet().iterator();
      while (iter.hasNext()) {
        Column col = (Column) iter.next();
        Object value = values[col.getColumnNumber()];
        _entryColumns.add(new EntryColumn(col, (Comparable) value));
      }
    }
    
    /**
     * Read an existing entry in from a buffer
     */
    public Entry(ByteBuffer buffer) throws IOException {
      Iterator iter = _columns.keySet().iterator();
      while (iter.hasNext()) {
        _entryColumns.add(new EntryColumn((Column) iter.next(), buffer));
      }
      //3-byte int in big endian order!  Gotta love those kooky MS programmers. :)
      _page = (((int) buffer.get()) & 0xFF) << 16;
      _page += (((int) buffer.get()) & 0xFF) << 8;
      _page += (int) buffer.get();
      _row = buffer.get();
    }
    
    public List getEntryColumns() {
      return _entryColumns;
    }
    
    public int getPage() {
      return _page;
    }
    
    public byte getRow() {
      return _row;
    }
    
    public int size() {
      int rtn = 5;
      Iterator iter = _entryColumns.iterator();
      while (iter.hasNext()) {
        rtn += ((EntryColumn) iter.next()).size();
      }
      return rtn;
    }
    
    /**
     * Write this entry into a buffer
     */
    public void write(ByteBuffer buffer) throws IOException {
      Iterator iter = _entryColumns.iterator();
      while (iter.hasNext()) {
        ((EntryColumn) iter.next()).write(buffer);
      }
      buffer.put((byte) (_page >>> 16));
      buffer.put((byte) (_page >>> 8));
      buffer.put((byte) _page);
      buffer.put(_row);
    }
    
    public String toString() {
      return ("Page = " + _page + ", Row = " + _row + ", Columns = " + _entryColumns + "\n");
    }
    
    public int compareTo(Entry other) {
      if (this == other) {
        return 0;
      }
      Iterator myIter = _entryColumns.iterator();
      Iterator otherIter = other.getEntryColumns().iterator();
      while (myIter.hasNext()) {
        if (!otherIter.hasNext()) {
          throw new IllegalArgumentException(
              "Trying to compare index entries with a different number of entry columns");
        }
        EntryColumn myCol = (EntryColumn) myIter.next();
        EntryColumn otherCol = (EntryColumn) otherIter.next();
        int i = myCol.compareTo(otherCol);
        if (i != 0) {
          return i;
        }
      }
      return new CompareToBuilder().append(_page, other.getPage())
          .append(_row, other.getRow()).toComparison();
    }
    
  }
  
  /**
   * A single column value within an index Entry; encapsulates column
   * definition and column value.
   */
  private class EntryColumn implements Comparable<EntryColumn> {
    
    /** Column definition */
    private Column _column;
    /** Column value */
    private Comparable _value;
    /** extra column bytes */
    private byte[] _extraBytes;
    
    /**
     * Create a new EntryColumn
     */
    public EntryColumn(Column col, Comparable value) {
      _column = col;
      _value = value;
      if(_column.getType() == DataType.TEXT) {
        // index strings are stored as uppercase
        _value = ((_value != null) ? _value.toString().toUpperCase() : null);
      }
    }
    
    /**
     * Read in an existing EntryColumn from a buffer
     */
    public EntryColumn(Column col, ByteBuffer buffer) throws IOException {
      _column = col;
      byte flag = buffer.get();
      if (flag != (byte) 0) {
        if (col.getType() == DataType.TEXT) {
          StringBuilder sb = new StringBuilder();
          byte b;
          while ( (b = buffer.get()) != (byte) 1) {
            if ((int) b == 43) {
              b = buffer.get();
            }
            Character c = (Character) CODES.getKey(new Byte(b));
            if (c != null) {
              sb.append(c.charValue());
            }
          }
          //Forward past 0x00 (in some cases, there is more data here, which
          //we don't currently understand)
          byte endByte = buffer.get();
          if(endByte != 0x00) {
            int startPos = buffer.position() - 1;
            int endPos = buffer.position();
            while(buffer.get(endPos) != 0x00) {
              ++endPos;
            }
            _extraBytes = new byte[endPos - startPos];
            buffer.position(startPos);
            buffer.get(_extraBytes);
            buffer.get();
          }
          _value = sb.toString();
        } else {
          byte[] data = new byte[col.getType().getSize()];
          buffer.get(data);
          _value = (Comparable) col.read(data, ByteOrder.BIG_ENDIAN);
          //ints and shorts are stored in index as value + 2147483648
          if (_value instanceof Integer) {
            _value = new Integer((int) (((Integer) _value).longValue() + (long) Integer.MAX_VALUE + 1L)); 
          } else if (_value instanceof Short) {
            _value = new Short((short) (((Short) _value).longValue() + (long) Integer.MAX_VALUE + 1L));
          }
        }
      }
    }
    
    public Comparable getValue() {
      return _value;
    }
    
    /**
     * Write this entry column to a buffer
     */
    public void write(ByteBuffer buffer) throws IOException {
      buffer.put((byte) 0x7F);
      if (_column.getType() == DataType.TEXT) {
        String s = (String) _value;
        for (int i = 0; i < s.length(); i++) {
          Byte b = (Byte) CODES.get(new Character(s.charAt(i)));
          
          if (b == null) {
            throw new IOException("Unmapped index value: " + s.charAt(i));
          } else {
            byte bv = b.byteValue();
            //WTF is this?  No idea why it's this way, but it is. :)
            if (bv == (byte) 2 || bv == (byte) 3 || bv == (byte) 9 || bv == (byte) 11 ||
                bv == (byte) 13 || bv == (byte) 15)
            {
              buffer.put((byte) 43);  //Ah, the magic 43.
            }
            buffer.put(b.byteValue());
            if (s.equals("_")) {
              buffer.put((byte) 3);
            }
          }
        }
        buffer.put((byte) 1);
        if(_extraBytes != null) {
          buffer.put(_extraBytes);
        }
        buffer.put((byte) 0);
      } else {
        Comparable value = _value;
        if (value instanceof Integer) {
          value = new Integer((int) (((Integer) value).longValue() - ((long) Integer.MAX_VALUE + 1L)));
        } else if (value instanceof Short) {
          value = new Short((short) (((Short) value).longValue() - ((long) Integer.MAX_VALUE + 1L)));
        }
        buffer.put(_column.write(value, ByteOrder.BIG_ENDIAN));
      }
    }
    
    public int size() {
      if (_value == null) {
        return 0;
      } else if (_value instanceof String) {
        int rtn = 3;
        String s = (String) _value;
        for (int i = 0; i < s.length(); i++) {
          rtn++;
          if (s.charAt(i) == '^' || s.charAt(i) == '_' || s.charAt(i) == '{' ||
              s.charAt(i) == '|' || s.charAt(i) == '}' || s.charAt(i) == '~')
          {
            // account for the magic 43
            rtn++;
          }
        }
        if(_extraBytes != null) {
          rtn += _extraBytes.length;
        }
        return rtn;
      } else {
        return _column.getType().getSize();
      }
    }
    
    public String toString() {
      return String.valueOf(_value);
    }
    
    public int compareTo(EntryColumn other) {
      return new CompareToBuilder().append(_value, other.getValue())
          .toComparison();
    }
  }
  
}
