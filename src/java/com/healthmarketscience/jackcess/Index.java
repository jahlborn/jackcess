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

/**
 * Access table index
 * @author Tim McCune
 */
public class Index implements Comparable<Index> {
  
  /** Max number of columns in an index */
  private static final int MAX_COLUMNS = 10;
  
  private static final short COLUMN_UNUSED = -1;

  private static final int NEW_ENTRY_COLUMN_INDEX = -1;

  private static final byte REVERSE_ORDER_FLAG = (byte)0x01;
  
  private static final Comparator<byte[]> BYTE_CODE_COMPARATOR =
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
          return ((left[pos] < right[pos]) ? -1 : 1);
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
  
  /** Page number of the index data */
  private int _pageNumber;
  private int _parentPageNumber;
  /** Number of rows in the index */
  private int _rowCount;
  private JetFormat _format;
  private SortedSet<Entry> _entries = new TreeSet<Entry>();
  /** Map of columns to flags */
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
      Byte flags = new Byte(buffer.get());
      if (columnNumber != COLUMN_UNUSED) {
        _columns.put(availableColumns.get(columnNumber), flags);
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
    int nextEntryIndex = 0;
    for (int i = 0; i < entryMask.length; i++) {
      for (int j = 0; j < 8; j++) {
        if ((entryMask[i] & (1 << j)) != 0) {
          int length = i * 8 + j - lastStart;
          Entry e = new Entry(indexPage, nextEntryIndex++);
          _entries.add(e);
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
  public void addRow(Object[] row, int pageNumber, byte rowNumber)
    throws IOException
  {
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

  private static boolean isFloatingPointColumn(Column col) {
    return((col.getType() == DataType.FLOAT) ||
           (col.getType() == DataType.DOUBLE));
  }

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
// //       value = new Integer((int) (((Integer) value).longValue() -
// //                                  ((long) Integer.MAX_VALUE + 1L)));
// //     } else if (value instanceof Short) {
// //       value = new Short((short) (((Short) value).longValue() -
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
    public Entry(Object[] values, int page, byte rowNumber) throws IOException
    {
      _page = page;
      _row = rowNumber;
      for(Map.Entry<Column, Byte> entry : _columns.entrySet()) {
        Column col = entry.getKey();
        Byte flags = entry.getValue();
        Object value = values[col.getColumnNumber()];
        _entryColumns.add(newEntryColumn(col).initFromValue(value, flags));
      }
    }
    
    /**
     * Read an existing entry in from a buffer
     */
    public Entry(ByteBuffer buffer, int nextEntryIndex) throws IOException {
      for(Map.Entry<Column, Byte> entry : _columns.entrySet()) {
        Column col = entry.getKey();
        Byte flags = entry.getValue();
        _entryColumns.add(newEntryColumn(col)
                          .initFromBuffer(buffer, nextEntryIndex, flags));
      }
      // 3-byte int in big endian order!  Gotta love those kooky MS
      // programmers. :)
      _page = ByteUtil.get3ByteInt(buffer, ByteOrder.BIG_ENDIAN);
      _row = buffer.get();
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
      int rtn = 4;
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
                                                    int entryIndex,
                                                    byte flags)
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
    private class FixedEntryColumn extends EntryColumn
    {
      /** Column value */
      private Comparable _value;
    
      public FixedEntryColumn(Column col) throws IOException {
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
                                           int entryIndex,
                                           byte flags)
        throws IOException
      {
        byte flag = buffer.get();
        // FIXME, reverse is 0x80, reverse null is 0xFF
        if (flag != (byte) 0) {
          byte[] data = new byte[_column.getType().getFixedSize()];          
          buffer.get(data);
          _value = (Comparable) _column.read(data, ByteOrder.BIG_ENDIAN);
          
          //ints and shorts are stored in index as value + 2147483648
          if (_value instanceof Integer) {
            _value = new Integer((int) (((Integer) _value).longValue() +
                                        (long) Integer.MAX_VALUE + 1L)); 
          } else if (_value instanceof Short) {
            _value = new Short((short) (((Short) _value).longValue() +
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
        buffer.put((byte) 0x7F);
        Comparable value = _value;
        if (value instanceof Integer) {
          value = new Integer((int) (((Integer) value).longValue() -
                                     ((long) Integer.MAX_VALUE + 1L)));
        } else if (value instanceof Short) {
          value = new Short((short) (((Short) value).longValue() -
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
    private class TextEntryColumn extends EntryColumn
    {
      /** the string byte codes */
      private byte[] _valueBytes;
      /** extra column bytes */
      private byte[] _extraBytes;
    
      public TextEntryColumn(Column col) throws IOException {
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
                                           int entryIndex,
                                           byte flags)
        throws IOException
      {
        byte flag = buffer.get();
        // FIXME, reverse is 0x80, reverse null is 0xFF
        // end flag is FE, post extra bytes is FF 00
        // extra bytes are inverted, so are normal bytes
        if (flag != (byte) 0) {

          int endPos = buffer.position();
          while(buffer.get(endPos) != (byte) 1) {
            ++endPos;
          }

          // read index bytes
          _valueBytes = new byte[endPos - buffer.position()];
          buffer.get(_valueBytes);

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
        buffer.put((byte) 0x7F);
        buffer.put(_valueBytes);
        buffer.put((byte) 1);
        if(_extraBytes != null) {
          buffer.put(_extraBytes);
        }
        buffer.put((byte) 0);
      }

      @Override
      protected int nonNullSize() {
        int rtn = _valueBytes.length + 2;
        if(_extraBytes != null) {
          rtn += _extraBytes.length;
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
        return BYTE_CODE_COMPARATOR.compare(
            _extraBytes, textOther._extraBytes);
      }
    
    }
    
  }
  
}
