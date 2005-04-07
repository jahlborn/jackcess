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
import java.nio.CharBuffer;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import com.healthmarketscience.jackcess.scsu.EndOfInputException;
import com.healthmarketscience.jackcess.scsu.Expand;
import com.healthmarketscience.jackcess.scsu.IllegalInputException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Access database column definition
 * @author Tim McCune
 */
public class Column implements Comparable {
  
  private static final Log LOG = LogFactory.getLog(Column.class);
  
  /**
   * Access starts counting dates at Jan 1, 1900.  Java starts counting
   * at Jan 1, 1970.  This is the # of days between them for conversion.
   */
  private static final double DAYS_BETWEEN_EPOCH_AND_1900 = 25569d;
  /**
   * Access stores numeric dates in days.  Java stores them in milliseconds.
   */
  private static final double MILLISECONDS_PER_DAY = 86400000d;
  
  /**
   * Long value (LVAL) type that indicates that the value is stored on the same page
   */
  private static final short LONG_VALUE_TYPE_THIS_PAGE = (short) 0x8000;
  /**
   * Long value (LVAL) type that indicates that the value is stored on another page
   */
  private static final short LONG_VALUE_TYPE_OTHER_PAGE = (short) 0x4000;
  /**
   * Long value (LVAL) type that indicates that the value is stored on multiple other pages
   */
  private static final short LONG_VALUE_TYPE_OTHER_PAGES = (short) 0x0;
  
  /** For text columns, whether or not they are compressed */ 
  private boolean _compressedUnicode = false;
  /** Whether or not the column is of variable length */
  private boolean _variableLength;
  /** Numeric precision */
  private byte _precision;
  /** Numeric scale */
  private byte _scale;
  /** Data type */
  private byte _type;
  /** Format that the containing database is in */
  private JetFormat _format;
  /** Used to read in LVAL pages */
  private PageChannel _pageChannel;
  /** Maximum column length */
  private short _columnLength;
  /** 0-based column number */
  private short _columnNumber;
  /** Column name */
  private String _name;
  
  public Column() {
    this(JetFormat.VERSION_4);
  }
  
  public Column(JetFormat format) {
    _format = format;
  }
  
  /**
   * Read a column definition in from a buffer
   * @param buffer Buffer containing column definition
   * @param offset Offset in the buffer at which the column definition starts
   * @param format Format that the containing database is in
   */
  public Column(ByteBuffer buffer, int offset, PageChannel pageChannel, JetFormat format) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Column def block:\n" + ByteUtil.toHexString(buffer, offset, 25));
    }
    _pageChannel = pageChannel;
    _format = format;
    setType(buffer.get(offset + format.OFFSET_COLUMN_TYPE));
    _columnNumber = buffer.getShort(offset + format.OFFSET_COLUMN_NUMBER);
    _columnLength = buffer.getShort(offset + format.OFFSET_COLUMN_LENGTH);
    if (_type == DataTypes.NUMERIC) {
      _precision = buffer.get(offset + format.OFFSET_COLUMN_PRECISION);
      _scale = buffer.get(offset + format.OFFSET_COLUMN_SCALE);
    }
    _variableLength = ((buffer.get(offset + format.OFFSET_COLUMN_VARIABLE)
        & 1) != 1);
    _compressedUnicode = ((buffer.get(offset +
        format.OFFSET_COLUMN_COMPRESSED_UNICODE) & 1) == 1);
  }
  
  public String getName() {
    return _name;
  }
  public void setName(String name) {
    _name = name;
  }
  
  public boolean isVariableLength() {
    return _variableLength;
  }
  public void setVariableLength(boolean variableLength) {
    _variableLength = variableLength;
  }
  
  public short getColumnNumber() {
    return _columnNumber;
  }
  
  /**
   * Also sets the length and the variable length flag, inferred from the type
   */
  public void setType(byte type) {
    _type = type;
    setLength((short) size());
    switch (type) {
      case DataTypes.BOOLEAN:
      case DataTypes.BYTE:
      case DataTypes.INT:
      case DataTypes.LONG:
      case DataTypes.DOUBLE:
      case DataTypes.FLOAT:
      case DataTypes.SHORT_DATE_TIME:
        setVariableLength(false);
        break;
      case DataTypes.BINARY:
      case DataTypes.TEXT:
        setVariableLength(true);
        break;
    }
  }
  public byte getType() {
    return _type;
  }
  
  public int getSQLType() throws SQLException {
    return DataTypes.toSQLType(_type);      
  }
  
  public void setSQLType(int type) throws SQLException {
    setType(DataTypes.fromSQLType(type));
  }
  
  public boolean isCompressedUnicode() {
    return _compressedUnicode;
  }
  
  public byte getPrecision() {
    return _precision;
  }
  
  public byte getScale() {
    return _scale;
  }
  
  public void setLength(short length) {
    _columnLength = length;
  }
  public short getLength() {
    return _columnLength;
  }
  
  /**
   * Deserialize a raw byte value for this column into an Object
   * @param data The raw byte value
   * @return The deserialized Object
   */
  public Object read(byte[] data) throws IOException {
    return read(data, ByteOrder.LITTLE_ENDIAN);
  }
  
  /**
   * Deserialize a raw byte value for this column into an Object
   * @param data The raw byte value
   * @param order Byte order in which the raw value is stored
   * @return The deserialized Object
   */  
  public Object read(byte[] data, ByteOrder order) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.order(order);
    switch (_type) {
      case DataTypes.BOOLEAN:
        throw new IOException("Tried to read a boolean from data instead of null mask.");
      case DataTypes.BYTE:
        return new Byte(buffer.get());
      case DataTypes.INT:
        return new Short(buffer.getShort());
      case DataTypes.LONG:
        return new Integer(buffer.getInt());
      case DataTypes.DOUBLE:
        return new Double(buffer.getDouble());
      case DataTypes.FLOAT:
        return new Float(buffer.getFloat());
      case DataTypes.SHORT_DATE_TIME:
        long time = (long) ((buffer.getDouble() - DAYS_BETWEEN_EPOCH_AND_1900) *
            MILLISECONDS_PER_DAY);
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.setTimeInMillis(time);
        //Not sure why we're off by 1...
        cal.add(Calendar.DATE, 1);
        return cal.getTime();
      case DataTypes.BINARY:
        return data;
      case DataTypes.TEXT:
        if (_compressedUnicode) {
          try {
            String rtn = new Expand().expand(data);
            //SCSU expander isn't handling the UTF-8-looking 2-byte combo that
            //prepends some of these strings.  Rather than dig into that code,
            //I'm just stripping them off here.  However, this is probably not
            //a great idea.
            if (rtn.length() > 2 && (int) rtn.charAt(0) == 255 &&
                (int) rtn.charAt(1) == 254)
            {
              rtn = rtn.substring(2);
            }
            //It also isn't handling short strings.
            if (rtn.length() > 1 && (int) rtn.charAt(1) == 0) {
              char[] fixed = new char[rtn.length() / 2];
              for (int i = 0; i < fixed.length; i ++) {
                fixed[i] = rtn.charAt(i * 2);
              }
              rtn = new String(fixed);
            }
            return rtn;
          } catch (IllegalInputException e) {
            throw new IOException("Can't expand text column");
          } catch (EndOfInputException e) {
            throw new IOException("Can't expand text column");
          }
        } else {
          return _format.CHARSET.decode(ByteBuffer.wrap(data)).toString();
        }
      case DataTypes.MONEY:
        //XXX
        return null;
      case DataTypes.OLE:
        if (data.length > 0) {
          return getLongValue(data);
        } else {
          return null;
        }
      case DataTypes.MEMO:
        if (data.length > 0) {
          return _format.CHARSET.decode(ByteBuffer.wrap(getLongValue(data))).toString();
        } else {
          return null;
        }
      case DataTypes.NUMERIC:
        //XXX
        return null;
      case DataTypes.UNKNOWN_0D:
      case DataTypes.GUID:
        return null;
      default:
        throw new IOException("Unrecognized data type: " + _type);
    }
  }
  
  /**
   * @param lvalDefinition Column value that points to an LVAL record
   * @return The LVAL data
   */
  private byte[] getLongValue(byte[] lvalDefinition) throws IOException {
    ByteBuffer def = ByteBuffer.wrap(lvalDefinition);
    def.order(ByteOrder.LITTLE_ENDIAN);
    short length = def.getShort();
    byte[] rtn = new byte[length];
    short type = def.getShort();
    switch (type) {
      case LONG_VALUE_TYPE_OTHER_PAGE:
        if (lvalDefinition.length != _format.SIZE_LONG_VALUE_DEF) {
          throw new IOException("Expected " + _format.SIZE_LONG_VALUE_DEF +
              " bytes in long value definition, but found " + lvalDefinition.length);
        }
        byte rowNum = def.get();
        int pageNum = ByteUtil.get3ByteInt(def, def.position());
        ByteBuffer lvalPage = _pageChannel.createPageBuffer();
        _pageChannel.readPage(lvalPage, pageNum);
        short offset = lvalPage.getShort(14 +
            rowNum * _format.SIZE_ROW_LOCATION);
        lvalPage.position(offset);
        lvalPage.get(rtn);
        break;
      case LONG_VALUE_TYPE_THIS_PAGE:
        def.getLong();  //Skip over lval_dp and unknown
        def.get(rtn);
      case LONG_VALUE_TYPE_OTHER_PAGES:
        //XXX
        return null;
      default:
        throw new IOException("Unrecognized long value type: " + type);
    }
    return rtn;
  }
  
  /**
   * Write an LVAL column into a ByteBuffer inline (LONG_VALUE_TYPE_THIS_PAGE)
   * @param value Value of the LVAL column
   * @return A buffer containing the LVAL definition and the column value
   */
  public ByteBuffer writeLongValue(byte[] value) throws IOException {
    ByteBuffer def = ByteBuffer.allocate(_format.SIZE_LONG_VALUE_DEF + value.length);
    def.order(ByteOrder.LITTLE_ENDIAN);
    def.putShort((short) value.length);
    def.putShort(LONG_VALUE_TYPE_THIS_PAGE);
    def.putInt(0);
    def.putInt(0);  //Unknown
    def.put(value);
    def.flip();
    return def;    
  }
  
  /**
   * Write an LVAL column into a ByteBuffer on another page
   *    (LONG_VALUE_TYPE_OTHER_PAGE)
   * @param value Value of the LVAL column
   * @return A buffer containing the LVAL definition
   */
  public ByteBuffer writeLongValueInNewPage(byte[] value) throws IOException {
    ByteBuffer lvalPage = _pageChannel.createPageBuffer();
    lvalPage.put(PageTypes.DATA); //Page type
    lvalPage.put((byte) 1); //Unknown
    lvalPage.putShort((short) (_format.PAGE_SIZE -
        _format.OFFSET_LVAL_ROW_LOCATION_BLOCK - _format.SIZE_ROW_LOCATION -
        value.length)); //Free space
    lvalPage.put((byte) 'L');
    lvalPage.put((byte) 'V');
    lvalPage.put((byte) 'A');
    lvalPage.put((byte) 'L');
    int offset = _format.PAGE_SIZE - value.length;
    lvalPage.position(14);
    lvalPage.putShort((short) offset);
    lvalPage.position(offset);
    lvalPage.put(value);
    ByteBuffer def = ByteBuffer.allocate(_format.SIZE_LONG_VALUE_DEF);
    def.order(ByteOrder.LITTLE_ENDIAN);
    def.putShort((short) value.length);
    def.putShort(LONG_VALUE_TYPE_OTHER_PAGE);
    def.put((byte) 0); //Row number
    def.put(ByteUtil.to3ByteInt(_pageChannel.writeNewPage(lvalPage)));  //Page #
    def.putInt(0);  //Unknown
    def.flip();
    return def;    
  }
  
  /**
   * Serialize an Object into a raw byte value for this column in little endian order
   * @param obj Object to serialize
   * @return A buffer containing the bytes
   */
  public ByteBuffer write(Object obj) throws IOException {
    return write(obj, ByteOrder.LITTLE_ENDIAN);
  }
  
  /**
   * Serialize an Object into a raw byte value for this column
   * @param obj Object to serialize
   * @param order Order in which to serialize
   * @return A buffer containing the bytes
   */
  public ByteBuffer write(Object obj, ByteOrder order) throws IOException {
    int size = size();
    if (_type == DataTypes.OLE || _type == DataTypes.MEMO) {
      size += ((byte[]) obj).length;
    }
    if (_type == DataTypes.TEXT) {
      size = getLength();
    }
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.order(order);
    switch (_type) {
      case DataTypes.BOOLEAN:
        break;
      case DataTypes.BYTE:
        buffer.put(((Byte) obj).byteValue());
        break;
      case DataTypes.INT:
        buffer.putShort(((Short) obj).shortValue());
        break;
      case DataTypes.LONG:
        buffer.putInt(((Integer) obj).intValue());
        break;
      case DataTypes.DOUBLE:
        buffer.putDouble(((Double) obj).doubleValue());
        break;
      case DataTypes.FLOAT:
        buffer.putFloat(((Float) obj).floatValue());
        break;
      case DataTypes.SHORT_DATE_TIME:
        Calendar cal = Calendar.getInstance();
        cal.setTime((Date) obj);
        long ms = cal.getTimeInMillis();
        ms += (long) TimeZone.getDefault().getOffset(ms);
        buffer.putDouble((double) ms / MILLISECONDS_PER_DAY +
            DAYS_BETWEEN_EPOCH_AND_1900);
        break;
      case DataTypes.BINARY:
        buffer.put((byte[]) obj);
        break;
      case DataTypes.TEXT:
        CharSequence text = (CharSequence) obj;
        int maxChars = size / 2;
        if (text.length() > maxChars) {
          text = text.subSequence(0, maxChars);
        }
        buffer.put(encodeText(text));
        break;
      case DataTypes.OLE:
        buffer.put(writeLongValue((byte[]) obj));
        break;
      case DataTypes.MEMO:
        buffer.put(writeLongValue(encodeText((CharSequence) obj).array()));
        break;
      default:
        throw new IOException("Unsupported data type: " + _type);
    }
    buffer.flip();
    return buffer;
  }
  
  /**
   * @param text Text to encode
   * @return A buffer with the text encoded
   */
  private ByteBuffer encodeText(CharSequence text) {
    return _format.CHARSET.encode(CharBuffer.wrap(text));
  }
  
  /**
   * @return Number of bytes that should be read for this column
   *    (applies to fixed-width columns)
   */
  public int size() {
    switch (_type) {
      case DataTypes.BOOLEAN:
        return 0;
      case DataTypes.BYTE:
        return 1;
      case DataTypes.INT:
        return 2;
      case DataTypes.LONG:
        return 4;
      case DataTypes.MONEY:
      case DataTypes.DOUBLE:
        return 8;
      case DataTypes.FLOAT:
        return 4;
      case DataTypes.SHORT_DATE_TIME:
        return 8;
      case DataTypes.BINARY:
        return 255;
      case DataTypes.TEXT:
        return 50 * 2;
      case DataTypes.OLE:
        return _format.SIZE_LONG_VALUE_DEF;
      case DataTypes.MEMO:
        return _format.SIZE_LONG_VALUE_DEF;
      case DataTypes.NUMERIC:
        throw new IllegalArgumentException("FIX ME");
      case DataTypes.UNKNOWN_0D:
      case DataTypes.GUID:
        throw new IllegalArgumentException("FIX ME");
      default:
        throw new IllegalArgumentException("Unrecognized data type: " + _type);
    }
  }
  
  public String toString() {
    StringBuffer rtn = new StringBuffer();
    rtn.append("\tName: " + _name);
    rtn.append("\n\tType: 0x" + Integer.toHexString((int)_type));
    rtn.append("\n\tNumber: " + _columnNumber);
    rtn.append("\n\tLength: " + _columnLength);
    rtn.append("\n\tVariable length: " + _variableLength);
    rtn.append("\n\tCompressed Unicode: " + _compressedUnicode);
    rtn.append("\n\n");
    return rtn.toString();
  }
  
  public int compareTo(Object obj) {
    Column other = (Column) obj;
    if (_columnNumber > other.getColumnNumber()) {
      return 1;
    } else if (_columnNumber < other.getColumnNumber()) {
      return -1;
    } else {
      return 0;
    }
  }
  
  /**
   * @param columns A list of columns in a table definition
   * @return The number of variable length columns found in the list
   */
  public static short countVariableLength(List columns) {
    short rtn = 0;
    Iterator iter = columns.iterator();
    while (iter.hasNext()) {
      Column col = (Column) iter.next();
      if (col.isVariableLength()) {
        rtn++;
      }
    }
    return rtn;
  }
  
}
