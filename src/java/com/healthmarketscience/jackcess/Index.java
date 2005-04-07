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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.lang.builder.CompareToBuilder;

/**
 * Access table index
 * @author Tim McCune
 */
public class Index implements Comparable {
  
  /** Max number of columns in an index */
  private static final int MAX_COLUMNS = 10;
  
  private static final short COLUMN_UNUSED = -1;
  
  /**
   * Map of characters to bytes that Access uses in indexes (not ASCII)
   *    (Character -> Byte)
   */
  private static BidiMap CODES = new DualHashBidiMap();
  static {
    //These values are prefixed with a '43'
    CODES.put(new Character('^'), new Byte((byte) 2));
    CODES.put(new Character('_'), new Byte((byte) 3));
    CODES.put(new Character('{'), new Byte((byte) 9));
    CODES.put(new Character('|'), new Byte((byte) 11));
    CODES.put(new Character('}'), new Byte((byte) 13));
    CODES.put(new Character('~'), new Byte((byte) 15));
    
    //These values aren't.
    CODES.put(new Character(' '), new Byte((byte) 7));
    CODES.put(new Character('#'), new Byte((byte) 12));
    CODES.put(new Character('$'), new Byte((byte) 14));
    CODES.put(new Character('%'), new Byte((byte) 16));
    CODES.put(new Character('&'), new Byte((byte) 18));
    CODES.put(new Character('('), new Byte((byte) 20));
    CODES.put(new Character(')'), new Byte((byte) 22));
    CODES.put(new Character('*'), new Byte((byte) 24));
    CODES.put(new Character(','), new Byte((byte) 26));
    CODES.put(new Character('/'), new Byte((byte) 30));
    CODES.put(new Character(':'), new Byte((byte) 32));
    CODES.put(new Character(';'), new Byte((byte) 34));
    CODES.put(new Character('?'), new Byte((byte) 36));
    CODES.put(new Character('@'), new Byte((byte) 38));
    CODES.put(new Character('+'), new Byte((byte) 44));
    CODES.put(new Character('<'), new Byte((byte) 46));
    CODES.put(new Character('='), new Byte((byte) 48));
    CODES.put(new Character('>'), new Byte((byte) 50));
    CODES.put(new Character('0'), new Byte((byte) 54));
    CODES.put(new Character('1'), new Byte((byte) 56));
    CODES.put(new Character('2'), new Byte((byte) 58));
    CODES.put(new Character('3'), new Byte((byte) 60));
    CODES.put(new Character('4'), new Byte((byte) 62));
    CODES.put(new Character('5'), new Byte((byte) 64));
    CODES.put(new Character('6'), new Byte((byte) 66));
    CODES.put(new Character('7'), new Byte((byte) 68));
    CODES.put(new Character('8'), new Byte((byte) 70));
    CODES.put(new Character('9'), new Byte((byte) 72));
    CODES.put(new Character('A'), new Byte((byte) 74));
    CODES.put(new Character('B'), new Byte((byte) 76));
    CODES.put(new Character('C'), new Byte((byte) 77));
    CODES.put(new Character('D'), new Byte((byte) 79));
    CODES.put(new Character('E'), new Byte((byte) 81));
    CODES.put(new Character('F'), new Byte((byte) 83));
    CODES.put(new Character('G'), new Byte((byte) 85));
    CODES.put(new Character('H'), new Byte((byte) 87));
    CODES.put(new Character('I'), new Byte((byte) 89));
    CODES.put(new Character('J'), new Byte((byte) 91));
    CODES.put(new Character('K'), new Byte((byte) 92));
    CODES.put(new Character('L'), new Byte((byte) 94));
    CODES.put(new Character('M'), new Byte((byte) 96));
    CODES.put(new Character('N'), new Byte((byte) 98));
    CODES.put(new Character('O'), new Byte((byte) 100));
    CODES.put(new Character('P'), new Byte((byte) 102));
    CODES.put(new Character('Q'), new Byte((byte) 104));
    CODES.put(new Character('R'), new Byte((byte) 105));
    CODES.put(new Character('S'), new Byte((byte) 107));
    CODES.put(new Character('T'), new Byte((byte) 109));
    CODES.put(new Character('U'), new Byte((byte) 111));
    CODES.put(new Character('V'), new Byte((byte) 113));
    CODES.put(new Character('W'), new Byte((byte) 115));
    CODES.put(new Character('X'), new Byte((byte) 117));
    CODES.put(new Character('Y'), new Byte((byte) 118));
    CODES.put(new Character('Z'), new Byte((byte) 120));
  }
  
  /** Page number of the index data */
  private int _pageNumber;
  private int _parentPageNumber;
  /** Number of rows in the index */
  private int _rowCount;
  private JetFormat _format;
  private List _allColumns;
  private SortedSet _entries = new TreeSet();
  /** Map of columns to order (Column -> Byte) */
  private Map _columns = new LinkedHashMap();
  private PageChannel _pageChannel;
  /** 0-based index number */
  private int _indexNumber;
  /** Index name */
  private String _name;
  
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
  public void read(ByteBuffer buffer, List availableColumns)
  throws IOException
  {
    _allColumns = availableColumns;
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
    StringBuffer rtn = new StringBuffer();
    rtn.append("\tName: " + _name);
    rtn.append("\n\tNumber: " + _indexNumber);
    rtn.append("\n\tPage number: " + _pageNumber);
    rtn.append("\n\tColumns: " + _columns);
    rtn.append("\n\tEntries: " + _entries);
    rtn.append("\n\n");
    return rtn.toString();
  }
  
  public int compareTo(Object obj) {
    Index other = (Index) obj;
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
  private class Entry implements Comparable {
    
    /** Page number on which the row is stored */
    private int _page;
    /** Row number at which the row is stored */
    private byte _row;
    /** Columns that are indexed */
    private List _entryColumns = new ArrayList();
    
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
    
    public int compareTo(Object obj) {
      if (this == obj) {
        return 0;
      }
      Entry other = (Entry) obj;
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
  private class EntryColumn implements Comparable {
    
    /** Column definition */
    private Column _column;
    /** Column value */
    private Comparable _value;
    
    /**
     * Create a new EntryColumn
     */
    public EntryColumn(Column col, Comparable value) {
      _column = col;
      _value = value;
    }
    
    /**
     * Read in an existing EntryColumn from a buffer
     */
    public EntryColumn(Column col, ByteBuffer buffer) throws IOException {
      _column = col;
      byte flag = buffer.get();
      if (flag != (byte) 0) {
        if (col.getType() == DataTypes.TEXT) {
          StringBuffer sb = new StringBuffer();
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
          buffer.get(); //Forward past 0x00
          _value = sb.toString();
        } else {
          byte[] data = new byte[col.size()];
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
      if (_column.getType() == DataTypes.TEXT) {
        String s = (String) _value;
        for (int i = 0; i < s.length(); i++) {
          Byte b = (Byte) CODES.get(new Character(Character.toUpperCase(s.charAt(i))));
          
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
              s.charAt(i) == '|' || s.charAt(i) == '}' || s.charAt(i) == '-')
          {
            rtn++;
          }
        }
        return rtn;
      } else {
        return _column.size();
      }
    }
    
    public String toString() {
      return String.valueOf(_value);
    }
    
    public int compareTo(Object obj) {
      return new CompareToBuilder().append(_value, ((EntryColumn) obj).getValue())
          .toComparison();
    }
  }
  
}
