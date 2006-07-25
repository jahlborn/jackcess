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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.scsu.EndOfInputException;
import com.healthmarketscience.jackcess.scsu.Expand;
import com.healthmarketscience.jackcess.scsu.IllegalInputException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Access database column definition
 * @author Tim McCune
 */
public class Column implements Comparable<Column> {
  
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

  private static final Pattern GUID_PATTERN = Pattern.compile("\\s*[{]([\\p{XDigit}]{4})-([\\p{XDigit}]{2})-([\\p{XDigit}]{2})-([\\p{XDigit}]{2})-([\\p{XDigit}]{6})[}]\\s*");

  /** default precision value for new numeric columns */
  public static final byte DEFAULT_PRECISION = 18;
  /** default scale value for new numeric columns */
  public static final byte DEFAULT_SCALE = 18;
  
  /** For text columns, whether or not they are compressed */ 
  private boolean _compressedUnicode = false;
  /** Whether or not the column is of variable length */
  private boolean _variableLength;
  /** Numeric precision */
  private byte _precision = DEFAULT_PRECISION;
  /** Numeric scale */
  private byte _scale = DEFAULT_SCALE;
  /** Data type */
  private DataType _type;
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
  /** the offset of the fixed data in the row */
  private int _fixedDataOffset;
  /** the index of the variable length data in the var len offset table */
  private int _varLenTableIndex;
  
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
  public Column(ByteBuffer buffer, int offset, PageChannel pageChannel, JetFormat format)
  throws SQLException
  {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Column def block:\n" + ByteUtil.toHexString(buffer, offset, 25));
    }
    _pageChannel = pageChannel;
    _format = format;
    setType(DataType.fromByte(buffer.get(offset + format.OFFSET_COLUMN_TYPE)));
    _columnNumber = buffer.getShort(offset + format.OFFSET_COLUMN_NUMBER);
    _columnLength = buffer.getShort(offset + format.OFFSET_COLUMN_LENGTH);
    if (_type == DataType.NUMERIC) {
      _precision = buffer.get(offset + format.OFFSET_COLUMN_PRECISION);
      _scale = buffer.get(offset + format.OFFSET_COLUMN_SCALE);
    }
    _variableLength = ((buffer.get(offset + format.OFFSET_COLUMN_VARIABLE)
        & 1) != 1);
    _compressedUnicode = ((buffer.get(offset +
        format.OFFSET_COLUMN_COMPRESSED_UNICODE) & 1) == 1);

    if(_variableLength) {
      _varLenTableIndex = buffer.getShort(offset + format.OFFSET_COLUMN_VARIABLE_TABLE_INDEX);
    } else {
      _fixedDataOffset = buffer.getShort(offset + format.OFFSET_COLUMN_FIXED_DATA_OFFSET);
    }
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
  public void setType(DataType type) {
    _type = type;
    setLength((short) size());
		setVariableLength(type.isVariableLength());
  }
  public DataType getType() {
    return _type;
  }
  
  public int getSQLType() throws SQLException {
    return _type.getSQLType();
  }
  
  public void setSQLType(int type) throws SQLException {
    setType(DataType.fromSQLType(type));
  }
  
  public boolean isCompressedUnicode() {
    return _compressedUnicode;
  }
  
  public byte getPrecision() {
    return _precision;
  }
  
  public void setPrecision(byte newPrecision) {
    if((newPrecision < 1) || (newPrecision > 28)) {
      throw new IllegalArgumentException("Precision must be from 1 to 28 inclusive");
    }
    _precision = newPrecision;
  }
  
  public byte getScale() {
    return _scale;
  }

  public void setScale(byte newScale) {
    if((newScale < 1) || (newScale > 28)) {
      throw new IllegalArgumentException("Scale must be from 0 to 28 inclusive");
    }
    _scale = newScale;
  }
  
  public void setLength(short length) {
    _columnLength = length;
  }
  public short getLength() {
    return _columnLength;
  }

  public int getVarLenTableIndex() {
    return _varLenTableIndex;
  }

  public int getFixedDataOffset() {
    return _fixedDataOffset;
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
    if (_type == DataType.BOOLEAN) {
      throw new IOException("Tried to read a boolean from data instead of null mask.");
    } else if (_type == DataType.BYTE) {
      return new Byte(buffer.get());
    } else if (_type == DataType.INT) {
      return new Short(buffer.getShort());
    } else if (_type == DataType.LONG) {
      return new Integer(buffer.getInt());
    } else if (_type == DataType.DOUBLE) {
      return new Double(buffer.getDouble());
    } else if (_type == DataType.FLOAT) {
      return new Float(buffer.getFloat());
    } else if (_type == DataType.SHORT_DATE_TIME) {
      return readDateValue(buffer);
    } else if (_type == DataType.BINARY) {
      return data;
    } else if (_type == DataType.TEXT) {
      if (_compressedUnicode) {
        try {
          return fixString(new Expand().expand(data));
        } catch (IllegalInputException e) {
          throw new IOException("Can't expand text column");
        } catch (EndOfInputException e) {
          throw new IOException("Can't expand text column");
        }
      } else {
        return decodeText(data);
      }
    } else if (_type == DataType.MONEY) {
      return readCurrencyValue(data);
    } else if (_type == DataType.OLE) {
      if (data.length > 0) {
        return readLongBinaryValue(data, null);
      } else {
        return null;
      }
    } else if (_type == DataType.MEMO) {
      if (data.length > 0) {
        return readLongStringValue(data);
      } else {
        return null;
      }
    } else if (_type == DataType.NUMERIC) {
      return readNumericValue(buffer);
    } else if (_type == DataType.GUID) {
      return readGUIDValue(buffer);
    } else if (_type == DataType.UNKNOWN_0D) {
      return null;
    } else {
      throw new IOException("Unrecognized data type: " + _type);
    }
  }

  /**
   * @param lvalDefinition Column value that points to an LVAL record
   * @param outType optional 1 element array for returning the
   *                <code>LONG_VALUE_TYPE_*</code>
   * @return The LVAL data
   */
  private byte[] readLongBinaryValue(byte[] lvalDefinition, short[] outType)
    throws IOException
  {
    ByteBuffer def = ByteBuffer.wrap(lvalDefinition);
    def.order(ByteOrder.LITTLE_ENDIAN);
    short length = def.getShort();
    // bail out gracefully here as we don't understand the format
    if (length < 0)
    {
       return null;
    }
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
        def.getInt();  //Skip over lval_dp
        def.getInt();  //Skip over unknown
        def.get(rtn);
        break;
      case LONG_VALUE_TYPE_OTHER_PAGES:
        //XXX
        return null;
      default:
        throw new IOException("Unrecognized long value type: " + type);
    }
    if(outType != null) {
      // return parsed type as well
      outType[0] = type;
    }
    return rtn;
  }
  
  /**
   * @param lvalDefinition Column value that points to an LVAL record
   * @return The LVAL data
   */
  private String readLongStringValue(byte[] lvalDefinition)
    throws IOException
  {
    short[] type = new short[1];
    byte[] binData = readLongBinaryValue(lvalDefinition, type);
    if(binData == null) {
      return null;
    }
    String result = null;
    switch (type[0]) {
      case LONG_VALUE_TYPE_OTHER_PAGE:
        result = decodeText(binData);
        break;
      case LONG_VALUE_TYPE_THIS_PAGE:
        result = fixString(new String(binData));
        break;
      case LONG_VALUE_TYPE_OTHER_PAGES:
        //XXX
        return null;
      default:
        throw new IOException("Unrecognized long value type: " + type);
    }
    return result;
  }

  /**
   * Decodes "Currency" values.
   * 
   * @param lvalDefinition Column value that points to an LVAL record
   * @return BigDecimal representing the monetary value
   * @throws IOException if the value cannot be parsed 
   */
  private BigDecimal readCurrencyValue(byte[] lvalDefinition)
    throws IOException
  {
    if(lvalDefinition.length != 8) {
      throw new IOException("Invalid money value.");
    }
    
    ByteBuffer def = ByteBuffer.wrap(lvalDefinition);
    def.order(ByteOrder.LITTLE_ENDIAN);
    return new BigDecimal(BigInteger.valueOf(def.getLong(0)), 4);
  }

  /**
   * Writes "Currency" values.
   */
  private void writeCurrencyValue(ByteBuffer buffer, Object value)
  {
    BigDecimal decVal = toBigDecimal(value);

    // adjust scale (this will throw if number has too many decimal places)
    decVal = decVal.setScale(4);
    
    // now, remove scale and convert to long (this will throw if the value is
    // too big)
    buffer.putLong(decVal.movePointRight(4).longValueExact());
  }

  /**
   * Decodes a NUMERIC field.
   */
  private BigDecimal readNumericValue(ByteBuffer buffer)
  {
    boolean negate = (buffer.get() != 0);

    byte[] tmpArr = new byte[16];
    buffer.get(tmpArr);

    if(buffer.order() != ByteOrder.BIG_ENDIAN) {
      fixNumericByteOrder(tmpArr);
    }

    BigInteger intVal = new BigInteger(tmpArr);
    if(negate) {
      intVal = intVal.negate();
    }
    return new BigDecimal(intVal, getScale());
  }

  /**
   * Writes a numeric value.
   */
  private void writeNumericValue(ByteBuffer buffer, Object value)
    throws IOException
  {
    BigDecimal decVal = toBigDecimal(value);

    boolean negative = (decVal.compareTo(BigDecimal.ZERO) < 0);
    if(negative) {
      decVal = decVal.negate();
    }

    // write sign byte
    buffer.put(negative ? (byte)1 : (byte)0);

    // adjust scale according to this column type (this will throw if number
    // has too many decimal places)
    decVal = decVal.setScale(getScale());

    // check precision
    if(decVal.precision() > getPrecision()) {
      throw new IOException("Numeric value is too big for specified precision "
                            + getPrecision() + ": " + decVal);
    }
    
    // convert to unscaled BigInteger, big-endian bytes
    byte[] intValBytes = decVal.unscaledValue().toByteArray();
    if(intValBytes.length > 16) {
      throw new IOException("Too many bytes for valid BigInteger?");
    }
    if(intValBytes.length < 16) {
      byte[] tmpBytes = new byte[16];
      System.arraycopy(intValBytes, 0, tmpBytes, (16 - intValBytes.length),
                       intValBytes.length);
      intValBytes = tmpBytes;
    }
    if(buffer.order() != ByteOrder.BIG_ENDIAN) {
      fixNumericByteOrder(intValBytes);
    }
    buffer.put(intValBytes);
  }

  /**
   * Decodes a date value.
   */
  private Date readDateValue(ByteBuffer buffer)
  {
    // seems access stores dates in the local timezone.  guess you just hope
    // you read it in the same timezone in which it was written!
    double dval = buffer.getDouble();
    dval *= MILLISECONDS_PER_DAY;
    dval -= (DAYS_BETWEEN_EPOCH_AND_1900 * MILLISECONDS_PER_DAY);
    long time = (long)dval;
    TimeZone tz = TimeZone.getDefault();
    Date date = new Date(time - tz.getRawOffset());
    if (tz.inDaylightTime(date))
    {
      date = new Date(date.getTime() - tz.getDSTSavings());
    }
    return date;
  }

  /**
   * Writes a date value.
   */
  private void writeDateValue(ByteBuffer buffer, Object value)
  {
    if(value == null) {
      buffer.putDouble(0d);
    } else {
      // seems access stores dates in the local timezone.  guess you just
      // hope you read it in the same timezone in which it was written!
      Calendar cal = Calendar.getInstance();
      cal.setTime((Date) value);
      long ms = cal.getTimeInMillis();
      ms += (long) TimeZone.getDefault().getOffset(ms);
      buffer.putDouble((double) ms / MILLISECONDS_PER_DAY +
                       DAYS_BETWEEN_EPOCH_AND_1900);
    }
  }

  /**
   * Decodes a GUID value.
   */
  private String readGUIDValue(ByteBuffer buffer)
  {
    StringBuilder sb = new StringBuilder(22);
    sb.append("{");
    sb.append(ByteUtil.toHexString(buffer, 0, 4,
                                   false));
    sb.append("-");
    sb.append(ByteUtil.toHexString(buffer, 4, 2,
                                   false));
    sb.append("-");
    sb.append(ByteUtil.toHexString(buffer, 6, 2,
                                   false));
    sb.append("-");
    sb.append(ByteUtil.toHexString(buffer, 8, 2,
                                   false));
    sb.append("-");
    sb.append(ByteUtil.toHexString(buffer, 10, 6,
                                   false));
    sb.append("}");
    return (sb.toString());
  }

  /**
   * Writes a GUID value.
   */
  private void writeGUIDValue(ByteBuffer buffer, Object value)
    throws IOException
  {
    Matcher m = GUID_PATTERN.matcher((CharSequence)value);
    if(m.matches()) {
      ByteUtil.writeHexString(buffer, m.group(1));
      ByteUtil.writeHexString(buffer, m.group(2));
      ByteUtil.writeHexString(buffer, m.group(3));
      ByteUtil.writeHexString(buffer, m.group(4));
      ByteUtil.writeHexString(buffer, m.group(5));
    } else {
      throw new IOException("Invalid GUID: " + value);
    }
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
    if (_type == DataType.OLE) {
      size += ((byte[]) obj).length;
    } else if(_type == DataType.MEMO) {
      byte[] encodedData = encodeText((CharSequence) obj).array();
      size += encodedData.length;
      obj = encodedData;
    } else if(_type == DataType.TEXT) {
      size = getLength();
    }
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.order(order);
    if (obj instanceof Boolean) {
      obj = ((Boolean) obj) ? 1 : 0;
    }
    if (_type == DataType.BOOLEAN) {
      //Do nothing
    } else if (_type == DataType.BYTE) {
      buffer.put(obj != null ? ((Number) obj).byteValue() : (byte) 0);
    } else if (_type == DataType.INT) {
      buffer.putShort(obj != null ? ((Number) obj).shortValue() : (short) 0);
    } else if (_type == DataType.LONG) {
      buffer.putInt(obj != null ? ((Number) obj).intValue() : 0);
    } else if (_type == DataType.DOUBLE) {
      buffer.putDouble(obj != null ? ((Number) obj).doubleValue() : (double) 0);
    } else if (_type == DataType.FLOAT) {
      buffer.putFloat(obj != null ? ((Number) obj).floatValue() : (float) 0);
    } else if (_type == DataType.SHORT_DATE_TIME) {
      writeDateValue(buffer, obj);
    } else if (_type == DataType.BINARY) {
      buffer.put((byte[]) obj);
    } else if (_type == DataType.TEXT) {
      CharSequence text = (CharSequence) obj;
      int maxChars = size / 2;
      if (text.length() > maxChars) {
        text = text.subSequence(0, maxChars);
      }
      buffer.put(encodeText(text));
    } else if (_type == DataType.MONEY) {
      writeCurrencyValue(buffer, obj);
    } else if (_type == DataType.OLE) {
      buffer.put(writeLongValue((byte[]) obj));
    } else if (_type == DataType.MEMO) {
      buffer.put(writeLongValue((byte[]) obj));
    } else if (_type == DataType.NUMERIC) {
      writeNumericValue(buffer, obj);
    } else if (_type == DataType.GUID) {
      writeGUIDValue(buffer, obj);
    } else {
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
   * @param textBytes bytes of text to decode
   * @return the decoded string
   */
  private String decodeText(byte[] textBytes) {
    return _format.CHARSET.decode(ByteBuffer.wrap(textBytes)).toString();
  }

  /**
   * Mucks with a string to handle some weird edge cases.
   * @param str string to fix
   * @return new string with whacky cases fixed
   */
  private String fixString(String str) {
    // There is a UTF-8-looking 2-byte combo that prepends some of these
    // strings.  Rather than dig into that code, I'm just stripping them off
    // here.  However, this is probably not a great idea.
    if (str.length() > 2 && (int) str.charAt(0) == 255 &&
        (int) str.charAt(1) == 254)
    {
      str = str.substring(2);
    }
    // It also isn't handling short strings.
    if (str.length() > 1 && (int) str.charAt(1) == 0) {
      char[] fixed = new char[str.length() / 2];
      for (int i = 0; i < fixed.length; i ++) {
        fixed[i] = str.charAt(i * 2);
      }
      str = new String(fixed);
    }
    return str;
  }
  
  /**
   * @return Number of bytes that should be read for this column
   *    (applies to fixed-width columns)
   */
  public int size() {
    if (_type == DataType.BOOLEAN) {
      return 0;
    } else if (_type == DataType.BYTE) {
      return 1;
    } else if (_type == DataType.INT) {
      return 2;
    } else if (_type == DataType.LONG) {
      return 4;
    } else if (_type == DataType.MONEY || _type == DataType.DOUBLE) {
      return 8;
    } else if (_type == DataType.FLOAT) {
      return 4;
    } else if (_type == DataType.SHORT_DATE_TIME) {
      return 8;
    } else if (_type == DataType.BINARY) {
      return 255;
    } else if (_type == DataType.TEXT) {
      return 50 * 2;
    } else if (_type == DataType.OLE) {
      return _format.SIZE_LONG_VALUE_DEF;
    } else if (_type == DataType.MEMO) {
      return _format.SIZE_LONG_VALUE_DEF;
    } else if (_type == DataType.NUMERIC) {
      return 17;
    } else if (_type == DataType.GUID) {
      return 16; 
    } else if (_type == DataType.UNKNOWN_0D) {
      throw new IllegalArgumentException("FIX ME");
    } else {
      throw new IllegalArgumentException("Unrecognized data type: " + _type);
    }
  }
  
  public String toString() {
    StringBuilder rtn = new StringBuilder();
    rtn.append("\tName: " + _name);
    rtn.append("\n\tType: 0x" + Integer.toHexString((int)_type.getValue()));
    rtn.append("\n\tNumber: " + _columnNumber);
    rtn.append("\n\tLength: " + _columnLength);
    rtn.append("\n\tVariable length: " + _variableLength);
    rtn.append("\n\tCompressed Unicode: " + _compressedUnicode);
    rtn.append("\n\n");
    return rtn.toString();
  }
  
  public int compareTo(Column other) {
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

  /**
   * @return an appropriate BigDecimal representation of the given object.
   *         <code>null</code> is returned as 0 and Numbers are converted
   *         using their double representation.
   */
  private static BigDecimal toBigDecimal(Object value)
  {
    if(value == null) {
      return BigDecimal.ZERO;
    } else if(value instanceof BigDecimal) {
      return (BigDecimal)value;
    } else {
      return new BigDecimal(((Number)value).doubleValue());
    }
  }

  /**
   * Swaps the bytes of the given numeric in place.
   */
  private static void fixNumericByteOrder(byte[] bytes)
  {
    // fix endianness of each 4 byte segment
    for(int i = 0; i < 4; ++i) {
      int idx = i * 4;
      byte b = bytes[idx + 0];
      bytes[idx + 0] = bytes[idx + 3];
      bytes[idx + 3] = b;
      b = bytes[idx + 1];
      bytes[idx + 1] = bytes[idx + 2];
      bytes[idx + 2] = b;
    }
  }
  
}
