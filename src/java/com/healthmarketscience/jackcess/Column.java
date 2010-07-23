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
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.scsu.Compress;
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
   * Meaningless placeholder object for inserting values in an autonumber
   * column.  it is not required that this value be used (any passed in value
   * is ignored), but using this placeholder may make code more obvious.
   */
  public static final Object AUTO_NUMBER = "<AUTO_NUMBER>";
  
  /**
   * Meaningless placeholder object for updating rows which indicates that a
   * given column should keep its existing value.
   */
  public static final Object KEEP_VALUE = "<KEEP_VALUE>";
  
  /**
   * Access stores numeric dates in days.  Java stores them in milliseconds.
   */
  private static final double MILLISECONDS_PER_DAY =
    (24L * 60L * 60L * 1000L);

  /**
   * Access starts counting dates at Jan 1, 1900.  Java starts counting
   * at Jan 1, 1970.  This is the # of millis between them for conversion.
   */
  private static final long MILLIS_BETWEEN_EPOCH_AND_1900 =
    25569L * (long)MILLISECONDS_PER_DAY;
  
  /**
   * Long value (LVAL) type that indicates that the value is stored on the same page
   */
  private static final byte LONG_VALUE_TYPE_THIS_PAGE = (byte) 0x80;
  /**
   * Long value (LVAL) type that indicates that the value is stored on another page
   */
  private static final byte LONG_VALUE_TYPE_OTHER_PAGE = (byte) 0x40;
  /**
   * Long value (LVAL) type that indicates that the value is stored on multiple other pages
   */
  private static final byte LONG_VALUE_TYPE_OTHER_PAGES = (byte) 0x00;

  /** mask for the fixed len bit */
  public static final byte FIXED_LEN_FLAG_MASK = (byte)0x01;
  
  /** mask for the auto number bit */
  public static final byte AUTO_NUMBER_FLAG_MASK = (byte)0x04;
  
  /** mask for the auto number guid bit */
  public static final byte AUTO_NUMBER_GUID_FLAG_MASK = (byte)0x40;
  
  /** mask for the unknown bit */
  public static final byte UNKNOWN_FLAG_MASK = (byte)0x02;

  /** pattern matching textual guid strings (allows for optional surrounding
      '{' and '}') */
  private static final Pattern GUID_PATTERN = Pattern.compile("\\s*[{]?([\\p{XDigit}]{8})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{12})[}]?\\s*");

  /** header used to indicate unicode text compression */
  private static final byte[] TEXT_COMPRESSION_HEADER = 
  { (byte)0xFF, (byte)0XFE };

  /** owning table */
  private final Table _table;
  /** For text columns, whether or not they are compressed */ 
  private boolean _compressedUnicode = false;
  /** Whether or not the column is of variable length */
  private boolean _variableLength;
  /** Whether or not the column is an autonumber column */
  private boolean _autoNumber;
  /** Numeric precision */
  private byte _precision;
  /** Numeric scale */
  private byte _scale;
  /** Data type */
  private DataType _type;
  /** Maximum column length */
  private short _columnLength;
  /** 0-based column number */
  private short _columnNumber;
  /** index of the data for this column within a list of row data */
  private int _columnIndex;
  /** Column name */
  private String _name;
  /** the offset of the fixed data in the row */
  private int _fixedDataOffset;
  /** the index of the variable length data in the var len offset table */
  private int _varLenTableIndex;
  /** the auto number generator for this column (if autonumber column) */
  private AutoNumberGenerator _autoNumberGenerator;
  
  public Column() {
    this(JetFormat.VERSION_4);
  }
  
  public Column(JetFormat format) {
    _table = null;
  }

  /**
   * Only used by unit tests
   */
  Column(boolean testing, Table table) {
    if(!testing) {
      throw new IllegalArgumentException();
    }
    _table = table;
  }
    
  /**
   * Read a column definition in from a buffer
   * @param table owning table
   * @param buffer Buffer containing column definition
   * @param offset Offset in the buffer at which the column definition starts
   */
  public Column(Table table, ByteBuffer buffer, int offset)
    throws IOException
  {
    _table = table;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Column def block:\n" + ByteUtil.toHexString(buffer, offset, 25));
    }
    setType(DataType.fromByte(buffer.get(offset + getFormat().OFFSET_COLUMN_TYPE)));
    _columnNumber = buffer.getShort(offset + getFormat().OFFSET_COLUMN_NUMBER);
    _columnLength = buffer.getShort(offset + getFormat().OFFSET_COLUMN_LENGTH);
    if (_type.getHasScalePrecision()) {
      _precision = buffer.get(offset + getFormat().OFFSET_COLUMN_PRECISION);
      _scale = buffer.get(offset + getFormat().OFFSET_COLUMN_SCALE);
    }
    byte flags = buffer.get(offset + getFormat().OFFSET_COLUMN_FLAGS);
    _variableLength = ((flags & FIXED_LEN_FLAG_MASK) == 0);
    _autoNumber = ((flags & (AUTO_NUMBER_FLAG_MASK | AUTO_NUMBER_GUID_FLAG_MASK)) != 0);
    setAutoNumberGenerator();
        
    _compressedUnicode = ((buffer.get(offset +
        getFormat().OFFSET_COLUMN_COMPRESSED_UNICODE) & 1) == 1);

    if(_variableLength) {
      _varLenTableIndex = buffer.getShort(offset + getFormat().OFFSET_COLUMN_VARIABLE_TABLE_INDEX);
    } else {
      _fixedDataOffset = buffer.getShort(offset + getFormat().OFFSET_COLUMN_FIXED_DATA_OFFSET);
    }
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
  
  public boolean isAutoNumber() {
    return _autoNumber;
  }

  public void setAutoNumber(boolean autoNumber) {
    _autoNumber = autoNumber;
    setAutoNumberGenerator();
  }

  public short getColumnNumber() {
    return _columnNumber;
  }

  public void setColumnNumber(short newColumnNumber) {
    _columnNumber = newColumnNumber;
  }

  public int getColumnIndex() {
    return _columnIndex;
  }

  public void setColumnIndex(int newColumnIndex) {
    _columnIndex = newColumnIndex;
  }
  
  /**
   * Also sets the length and the variable length flag, inferred from the
   * type.  For types with scale/precision, sets the scale and precision to
   * default values.
   */
  public void setType(DataType type) {
    _type = type;
    if(!type.isVariableLength()) {
      setLength((short)type.getFixedSize());
    } else if(!type.isLongValue()) {
      setLength((short)type.getDefaultSize());
    }
    setVariableLength(type.isVariableLength());
    if(type.getHasScalePrecision()) {
      setScale((byte)type.getDefaultScale());
      setPrecision((byte)type.getDefaultPrecision());
    }
  }
  public DataType getType() {
    return _type;
  }
  
  public int getSQLType() throws SQLException {
    return _type.getSQLType();
  }
  
  public void setSQLType(int type) throws SQLException {
    setSQLType(type, 0);
  }
  
  public void setSQLType(int type, int lengthInUnits) throws SQLException {
    setType(DataType.fromSQLType(type, lengthInUnits));
  }
  
  public boolean isCompressedUnicode() {
    return _compressedUnicode;
  }

  public void setCompressedUnicode(boolean newCompessedUnicode) {
    _compressedUnicode = newCompessedUnicode;
  }

  public byte getPrecision() {
    return _precision;
  }
  
  public void setPrecision(byte newPrecision) {
    _precision = newPrecision;
  }
  
  public byte getScale() {
    return _scale;
  }

  public void setScale(byte newScale) {
    _scale = newScale;
  }
  
  public void setLength(short length) {
    _columnLength = length;
  }
  public short getLength() {
    return _columnLength;
  }

  public void setLengthInUnits(short unitLength) {
    setLength((short)getType().fromUnitSize(unitLength));
  }
  public short getLengthInUnits() {
    return (short)getType().toUnitSize(getLength());
  }

  public void setVarLenTableIndex(int idx) {
    _varLenTableIndex = idx;
  }
  
  public int getVarLenTableIndex() {
    return _varLenTableIndex;
  }

  public void setFixedDataOffset(int newOffset) {
    _fixedDataOffset = newOffset;
  }
  
  public int getFixedDataOffset() {
    return _fixedDataOffset;
  }

  protected Charset getCharset() {
    return getTable().getDatabase().getCharset();
  }

  protected TimeZone getTimeZone() {
    return getTable().getDatabase().getTimeZone();
  }

  private void setAutoNumberGenerator()
  {
    if(!_autoNumber || (_type == null)) {
      _autoNumberGenerator = null;
      return;
    }

    if((_autoNumberGenerator != null) && 
       (_autoNumberGenerator.getType() == _type)) {
      // keep existing
      return;
    }

    switch(_type) {
    case LONG:
      _autoNumberGenerator = new LongAutoNumberGenerator();
      break;
    case GUID:
      _autoNumberGenerator = new GuidAutoNumberGenerator();
      break;
    default:
      throw new RuntimeException("Unexpected autoNumber column type " + _type);
    }
  }

  /**
   * Returns the AutoNumberGenerator for this column if this is an autonumber
   * column, {@code null} otherwise.
   */
  public AutoNumberGenerator getAutoNumberGenerator() {
    return _autoNumberGenerator;
  }

  /**
   * Checks that this column definition is valid.
   *
   * @throws IllegalArgumentException if this column definition is invalid.
   */
  public void validate(JetFormat format) {
    if(getType() == null) {
      throw new IllegalArgumentException("must have type");
    }
    Database.validateIdentifierName(getName(), format.MAX_COLUMN_NAME_LENGTH,
                                    "column");
    
    if(isVariableLength() != getType().isVariableLength()) {
      throw new IllegalArgumentException("invalid variable length setting");
    }

    if(!isVariableLength()) {
      if(getLength() != getType().getFixedSize()) {
        if(getLength() < getType().getFixedSize()) {
          throw new IllegalArgumentException("invalid fixed length size");
        }
        LOG.warn("Column length " + getLength() + 
                 " longer than expected fixed size " + 
                 getType().getFixedSize());
      }
    } else if(!getType().isLongValue()) {
      if(!getType().isValidSize(getLength())) {
        throw new IllegalArgumentException("var length out of range");
      }
    }

    if(getType().getHasScalePrecision()) {
      if(!getType().isValidScale(getScale())) {
        throw new IllegalArgumentException(
            "Scale must be from " + getType().getMinScale() + " to " +
            getType().getMaxScale() + " inclusive");
      }
      if(!getType().isValidPrecision(getPrecision())) {
        throw new IllegalArgumentException(
            "Precision must be from " + getType().getMinPrecision() + " to " +
            getType().getMaxPrecision() + " inclusive");
      }
    }

    if(isAutoNumber()) {
      if((getType() != DataType.LONG) && (getType() != DataType.GUID)) {
        throw new IllegalArgumentException(
            "Auto number column must be long integer or guid");
      }
    }

    if(isCompressedUnicode()) {
      if((getType() != DataType.TEXT) &&
         (getType() != DataType.MEMO)) {
        throw new IllegalArgumentException(
            "Only textual columns allow unicode compression (text/memo)");
      }
    }
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
      return Byte.valueOf(buffer.get());
    } else if (_type == DataType.INT) {
      return Short.valueOf(buffer.getShort());
    } else if (_type == DataType.LONG) {
      return Integer.valueOf(buffer.getInt());
    } else if (_type == DataType.DOUBLE) {
      return Double.valueOf(buffer.getDouble());
    } else if (_type == DataType.FLOAT) {
      return Float.valueOf(buffer.getFloat());
    } else if (_type == DataType.SHORT_DATE_TIME) {
      return readDateValue(buffer);
    } else if (_type == DataType.BINARY) {
      return data;
    } else if (_type == DataType.TEXT) {
      return decodeTextValue(data);
    } else if (_type == DataType.MONEY) {
      return readCurrencyValue(buffer);
    } else if (_type == DataType.OLE) {
      if (data.length > 0) {
        return readLongValue(data);
      }
      return null;
    } else if (_type == DataType.MEMO) {
      if (data.length > 0) {
        return readLongStringValue(data);
      }
      return null;
    } else if (_type == DataType.NUMERIC) {
      return readNumericValue(buffer);
    } else if (_type == DataType.GUID) {
      return readGUIDValue(buffer, order);
    } else if ((_type == DataType.UNKNOWN_0D) || 
               (_type == DataType.UNKNOWN_11)) {
      // treat like "binary" data
      return data;
    } else {
      throw new IOException("Unrecognized data type: " + _type);
    }
  }

  /**
   * @param lvalDefinition Column value that points to an LVAL record
   * @return The LVAL data
   */
  private byte[] readLongValue(byte[] lvalDefinition)
    throws IOException
  {
    ByteBuffer def = ByteBuffer.wrap(lvalDefinition);
    def.order(ByteOrder.LITTLE_ENDIAN);
    int length = ByteUtil.get3ByteInt(def);
    // bail out gracefully here as we don't understand the format
    if (length < 0)
    {
       return null;
    }
    byte[] rtn = new byte[length];
    byte type = def.get();

    if(type == LONG_VALUE_TYPE_THIS_PAGE) {

      // inline long value
      def.getInt();  //Skip over lval_dp
      def.getInt();  //Skip over unknown
      def.get(rtn);

    } else {

      // long value on other page(s)
      if (lvalDefinition.length != getFormat().SIZE_LONG_VALUE_DEF) {
        throw new IOException("Expected " + getFormat().SIZE_LONG_VALUE_DEF +
                              " bytes in long value definition, but found " +
                              lvalDefinition.length);
      }

      int rowNum = ByteUtil.getUnsignedByte(def);
      int pageNum = ByteUtil.get3ByteInt(def, def.position());
      ByteBuffer lvalPage = getPageChannel().createPageBuffer();
      
      switch (type) {
      case LONG_VALUE_TYPE_OTHER_PAGE:
        {
          getPageChannel().readPage(lvalPage, pageNum);

          short rowStart = Table.findRowStart(lvalPage, rowNum, getFormat());
          short rowEnd = Table.findRowEnd(lvalPage, rowNum, getFormat());

          if((rowEnd - rowStart) != length) {
            throw new IOException("Unexpected lval row length");
          }
        
          lvalPage.position(rowStart);
          lvalPage.get(rtn);
        }
        break;
        
      case LONG_VALUE_TYPE_OTHER_PAGES:

        ByteBuffer rtnBuf = ByteBuffer.wrap(rtn);
        int remainingLen = length;
        while(remainingLen > 0) {
          lvalPage.clear();
          getPageChannel().readPage(lvalPage, pageNum);

          short rowStart = Table.findRowStart(lvalPage, rowNum, getFormat());
          short rowEnd = Table.findRowEnd(lvalPage, rowNum, getFormat());
          
          // read next page information
          lvalPage.position(rowStart);
          rowNum = ByteUtil.getUnsignedByte(lvalPage);
          pageNum = ByteUtil.get3ByteInt(lvalPage);

          // update rowEnd and remainingLen based on chunkLength
          int chunkLength = (rowEnd - rowStart) - 4;
          if(chunkLength > remainingLen) {
            rowEnd = (short)(rowEnd - (chunkLength - remainingLen));
            chunkLength = remainingLen;
          }
          remainingLen -= chunkLength;
          
          lvalPage.limit(rowEnd);
          rtnBuf.put(lvalPage);
        }
        
        break;
        
      default:
        throw new IOException("Unrecognized long value type: " + type);
      }
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
    byte[] binData = readLongValue(lvalDefinition);
    if(binData == null) {
      return null;
    }
    return decodeTextValue(binData);
  }

  /**
   * Decodes "Currency" values.
   * 
   * @param buffer Column value that points to currency data
   * @return BigDecimal representing the monetary value
   * @throws IOException if the value cannot be parsed 
   */
  private BigDecimal readCurrencyValue(ByteBuffer buffer)
    throws IOException
  {
    if(buffer.remaining() != 8) {
      throw new IOException("Invalid money value.");
    }
    
    return new BigDecimal(BigInteger.valueOf(buffer.getLong(0)), 4);
  }

  /**
   * Writes "Currency" values.
   */
  private void writeCurrencyValue(ByteBuffer buffer, Object value)
    throws IOException
  {
    try {
      BigDecimal decVal = toBigDecimal(value);

      // adjust scale (will cause the an ArithmeticException if number has too
      // many decimal places)
      decVal = decVal.setScale(4);
    
      // now, remove scale and convert to long (this will throw if the value is
      // too big)
      buffer.putLong(decVal.movePointRight(4).longValueExact());
    } catch(ArithmeticException e) {
      throw (IOException)
        new IOException("Currency value out of range").initCause(e);
    }
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
    try {
      BigDecimal decVal = toBigDecimal(value);

      boolean negative = (decVal.compareTo(BigDecimal.ZERO) < 0);
      if(negative) {
        decVal = decVal.negate();
      }

      // write sign byte
      buffer.put(negative ? (byte)0x80 : (byte)0);

      // adjust scale according to this column type (will cause the an
      // ArithmeticException if number has too many decimal places)
      decVal = decVal.setScale(getScale());

      // check precision
      if(decVal.precision() > getPrecision()) {
        throw new IOException(
            "Numeric value is too big for specified precision "
            + getPrecision() + ": " + decVal);
      }
    
      // convert to unscaled BigInteger, big-endian bytes
      byte[] intValBytes = decVal.unscaledValue().toByteArray();
      int maxByteLen = getType().getFixedSize() - 1;
      if(intValBytes.length > maxByteLen) {
        throw new IOException("Too many bytes for valid BigInteger?");
      }
      if(intValBytes.length < maxByteLen) {
        byte[] tmpBytes = new byte[maxByteLen];
        System.arraycopy(intValBytes, 0, tmpBytes,
                         (maxByteLen - intValBytes.length),
                         intValBytes.length);
        intValBytes = tmpBytes;
      }
      if(buffer.order() != ByteOrder.BIG_ENDIAN) {
        fixNumericByteOrder(intValBytes);
      }
      buffer.put(intValBytes);
    } catch(ArithmeticException e) {
      throw (IOException)
        new IOException("Numeric value out of range").initCause(e);
    }
  }

  /**
   * Decodes a date value.
   */
  private Date readDateValue(ByteBuffer buffer)
  {
    // seems access stores dates in the local timezone.  guess you just hope
    // you read it in the same timezone in which it was written!
    long dateBits = buffer.getLong();
    long time = fromDateDouble(Double.longBitsToDouble(dateBits));
    return new DateExt(time, dateBits);
  }
  
  /**
   * Returns a java long time value converted from an access date double.
   */
  private long fromDateDouble(double value)
  {
    long time = Math.round(value * MILLISECONDS_PER_DAY);
    time -= MILLIS_BETWEEN_EPOCH_AND_1900;
    time -= getTimeZoneOffset(time);
    return time;
  }

  /**
   * Writes a date value.
   */
  private void writeDateValue(ByteBuffer buffer, Object value)
  {
    if(value == null) {
      buffer.putDouble(0d);
    } else if(value instanceof DateExt) {
      
      // this is a Date value previously read from readDateValue().  use the
      // original bits to store the value so we don't lose any precision
      buffer.putLong(((DateExt)value).getDateBits());
      
    } else {
      
      buffer.putDouble(toDateDouble(value));
    }
  }

  /**
   * Returns an access date double converted from a java Date/Calendar/Number
   * time value.
   */
  private double toDateDouble(Object value)
  {
      // seems access stores dates in the local timezone.  guess you just
      // hope you read it in the same timezone in which it was written!
      long time = ((value instanceof Date) ?
                   ((Date)value).getTime() :
                   ((value instanceof Calendar) ?
                    ((Calendar)value).getTimeInMillis() :
                    ((Number)value).longValue()));
      time += getTimeZoneOffset(time);
      time += MILLIS_BETWEEN_EPOCH_AND_1900;
      return time / MILLISECONDS_PER_DAY;
  }

  /**
   * Gets the timezone offset from UTC for the given time (including DST).
   */
  private long getTimeZoneOffset(long time)
  {
    Calendar c = Calendar.getInstance(getTimeZone());
    c.setTimeInMillis(time);
    return ((long)c.get(Calendar.ZONE_OFFSET) + c.get(Calendar.DST_OFFSET));
  }  
  
  /**
   * Decodes a GUID value.
   */
  private String readGUIDValue(ByteBuffer buffer, ByteOrder order)
  {
    if(order != ByteOrder.BIG_ENDIAN) {
      byte[] tmpArr = new byte[16];
      buffer.get(tmpArr);

        // the first 3 guid components are integer components which need to
        // respect endianness, so swap 4-byte int, 2-byte int, 2-byte int
      ByteUtil.swap4Bytes(tmpArr, 0);
      ByteUtil.swap2Bytes(tmpArr, 4);
      ByteUtil.swap2Bytes(tmpArr, 6);
      buffer = ByteBuffer.wrap(tmpArr);
    }

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
  private void writeGUIDValue(ByteBuffer buffer, Object value, 
                              ByteOrder order)
    throws IOException
  {
    Matcher m = GUID_PATTERN.matcher(toCharSequence(value));
    if(m.matches()) {
      ByteBuffer origBuffer = null;
      byte[] tmpBuf = null;
      if(order != ByteOrder.BIG_ENDIAN) {
        // write to a temp buf so we can do some swapping below
        origBuffer = buffer;
        tmpBuf = new byte[16];
        buffer = ByteBuffer.wrap(tmpBuf);
      }

      ByteUtil.writeHexString(buffer, m.group(1));
      ByteUtil.writeHexString(buffer, m.group(2));
      ByteUtil.writeHexString(buffer, m.group(3));
      ByteUtil.writeHexString(buffer, m.group(4));
      ByteUtil.writeHexString(buffer, m.group(5));
      
      if(tmpBuf != null) {
        // the first 3 guid components are integer components which need to
        // respect endianness, so swap 4-byte int, 2-byte int, 2-byte int
        ByteUtil.swap4Bytes(tmpBuf, 0);
        ByteUtil.swap2Bytes(tmpBuf, 4);
        ByteUtil.swap2Bytes(tmpBuf, 6);
        origBuffer.put(tmpBuf);
      }

    } else {
      throw new IOException("Invalid GUID: " + value);
    }
  }
  
  /**
   * Write an LVAL column into a ByteBuffer inline if it fits, otherwise in
   * other data page(s).
   * @param value Value of the LVAL column
   * @return A buffer containing the LVAL definition and (possibly) the column
   *         value (unless written to other pages)
   */
  public ByteBuffer writeLongValue(byte[] value,
                                   int remainingRowLength) throws IOException
  {
    if(value.length > getType().getMaxSize()) {
      throw new IOException("value too big for column, max " +
                            getType().getMaxSize() + ", got " +
                            value.length);
    }

    // determine which type to write
    byte type = 0;
    int lvalDefLen = getFormat().SIZE_LONG_VALUE_DEF;
    if(((getFormat().SIZE_LONG_VALUE_DEF + value.length) <= remainingRowLength)
       && (value.length <= getFormat().MAX_INLINE_LONG_VALUE_SIZE)) {
      type = LONG_VALUE_TYPE_THIS_PAGE;
      lvalDefLen += value.length;
    } else if(value.length <= getFormat().MAX_LONG_VALUE_ROW_SIZE) {
      type = LONG_VALUE_TYPE_OTHER_PAGE;
    } else {
      type = LONG_VALUE_TYPE_OTHER_PAGES;
    }

    ByteBuffer def = getPageChannel().createBuffer(lvalDefLen);
    ByteUtil.put3ByteInt(def, value.length);
    def.put(type);

    if(type == LONG_VALUE_TYPE_THIS_PAGE) {
      // write long value inline
      def.putInt(0);
      def.putInt(0);  //Unknown
      def.put(value);
    } else {
      
      TempPageHolder lvalBufferH = getTable().getLongValueBuffer();
      ByteBuffer lvalPage = null;
      int firstLvalPageNum = PageChannel.INVALID_PAGE_NUMBER;
      byte firstLvalRow = 0;

      // write other page(s)
      switch(type) {
      case LONG_VALUE_TYPE_OTHER_PAGE:
        lvalPage = getLongValuePage(value.length, lvalBufferH);
        firstLvalPageNum = lvalBufferH.getPageNumber();
        firstLvalRow = (byte)Table.addDataPageRow(lvalPage, value.length,
                                                  getFormat(), 0);
        lvalPage.put(value);
        getPageChannel().writePage(lvalPage, firstLvalPageNum);
        break;

      case LONG_VALUE_TYPE_OTHER_PAGES:

        ByteBuffer buffer = ByteBuffer.wrap(value);
        int remainingLen = buffer.remaining();
        buffer.limit(0);
        lvalPage = getLongValuePage(getFormat().MAX_LONG_VALUE_ROW_SIZE,
                                    lvalBufferH);
        firstLvalPageNum = lvalBufferH.getPageNumber();
        int lvalPageNum = firstLvalPageNum;
        ByteBuffer nextLvalPage = null;
        int nextLvalPageNum = 0;
        while(remainingLen > 0) {
          lvalPage.clear();

          // figure out how much we will put in this page (we need 4 bytes for
          // the next page pointer)
          int chunkLength = Math.min(getFormat().MAX_LONG_VALUE_ROW_SIZE - 4,
                                     remainingLen);

          // figure out if we will need another page, and if so, allocate it
          if(chunkLength < remainingLen) {
            // force a new page to be allocated
            lvalBufferH.clear();
            nextLvalPage = getLongValuePage(
                getFormat().MAX_LONG_VALUE_ROW_SIZE, lvalBufferH);
            nextLvalPageNum = lvalBufferH.getPageNumber();
          } else {
            nextLvalPage = null;
            nextLvalPageNum = 0;
          }

          // add row to this page
          byte lvalRow = (byte)Table.addDataPageRow(lvalPage, chunkLength + 4,
                                                    getFormat(), 0);
          
          // write next page info (we'll always be writing into row 0 for
          // newly created pages)
          lvalPage.put((byte)0); // row number
          ByteUtil.put3ByteInt(lvalPage, nextLvalPageNum); // page number

          // write this page's chunk of data
          buffer.limit(buffer.limit() + chunkLength);
          lvalPage.put(buffer);
          remainingLen -= chunkLength;

          // write new page to database
          getPageChannel().writePage(lvalPage, lvalPageNum);

          if(lvalPageNum == firstLvalPageNum) {
            // save initial row info
            firstLvalRow = lvalRow;
          } else {
            // check assertion that we wrote to row 0 for all subsequent pages
            if(lvalRow != (byte)0) {
              throw new IllegalStateException("Expected row 0, but was " +
                                              lvalRow);
            }
          }
          
          // move to next page
          lvalPage = nextLvalPage;
          lvalPageNum = nextLvalPageNum;
        }
        break;

      default:
        throw new IOException("Unrecognized long value type: " + type);
      }

      // update def
      def.put(firstLvalRow);
      ByteUtil.put3ByteInt(def, firstLvalPageNum);
      def.putInt(0);  //Unknown
      
    }
      
    def.flip();
    return def;
  }

  /**
   * Writes the header info for a long value page.
   */
  private void writeLongValueHeader(ByteBuffer lvalPage)
  {
    lvalPage.put(PageTypes.DATA); //Page type
    lvalPage.put((byte) 1); //Unknown
    lvalPage.putShort((short)getFormat().PAGE_INITIAL_FREE_SPACE); //Free space
    lvalPage.put((byte) 'L');
    lvalPage.put((byte) 'V');
    lvalPage.put((byte) 'A');
    lvalPage.put((byte) 'L');
    lvalPage.putInt(0); //unknown
    lvalPage.putShort((short)0); // num rows in page
  }

  /**
   * Returns a long value data page with space for data of the given length.
   */
  private ByteBuffer getLongValuePage(int dataLength,
                                      TempPageHolder lvalBufferH)
    throws IOException
  {
    ByteBuffer lvalPage = null;
    if(lvalBufferH.getPageNumber() != PageChannel.INVALID_PAGE_NUMBER) {
      lvalPage = lvalBufferH.getPage(getPageChannel());
      if(Table.rowFitsOnDataPage(dataLength, lvalPage, getFormat())) {
        // the current page has space
        return lvalPage;
      }
    }

    // need new page
    lvalPage = lvalBufferH.setNewPage(getPageChannel());
    writeLongValueHeader(lvalPage);
    return lvalPage;
  }
  
  /**
   * Serialize an Object into a raw byte value for this column in little endian order
   * @param obj Object to serialize
   * @return A buffer containing the bytes
   */
  public ByteBuffer write(Object obj, int remainingRowLength)
    throws IOException
  {
    return write(obj, remainingRowLength, ByteOrder.LITTLE_ENDIAN);
  }
  
  /**
   * Serialize an Object into a raw byte value for this column
   * @param obj Object to serialize
   * @param order Order in which to serialize
   * @return A buffer containing the bytes
   */
  public ByteBuffer write(Object obj, int remainingRowLength, ByteOrder order)
    throws IOException
  {
    if(isRawData(obj)) {
      // just slap it right in (not for the faint of heart!)
      return ByteBuffer.wrap(((RawData)obj).getBytes());
    }

    if(!isVariableLength()) {
      return writeFixedLengthField(obj, order);
    }
      
    // var length column
    if(!getType().isLongValue()) {

      // this is an "inline" var length field
      switch(getType()) {
      case NUMERIC:
        // don't ask me why numerics are "var length" columns...
        ByteBuffer buffer = getPageChannel().createBuffer(
            getType().getFixedSize(), order);
        writeNumericValue(buffer, obj);
        buffer.flip();
        return buffer;

      case TEXT:
        byte[] encodedData = encodeTextValue(
            obj, 0, getLengthInUnits(), false).array();
        obj = encodedData;
        break;
        
      case BINARY:
      case UNKNOWN_0D:
        // should already be "encoded"
        break;
      default:
        throw new RuntimeException("unexpected inline var length type: " +
                                   getType());
      }

      ByteBuffer buffer = ByteBuffer.wrap(toByteArray(obj));
      buffer.order(order);
      return buffer;
    }

    // var length, long value column
    switch(getType()) {
    case OLE:
      // should already be "encoded"
      break;
    case MEMO:
      int maxMemoChars = DataType.MEMO.toUnitSize(DataType.MEMO.getMaxSize());
      obj = encodeTextValue(obj, 0, maxMemoChars, false).array();
      break;
    default:
      throw new RuntimeException("unexpected var length, long value type: " +
                                 getType());
    }    

    // create long value buffer
    return writeLongValue(toByteArray(obj), remainingRowLength);
  }

  /**
   * Serialize an Object into a raw byte value for this column
   * @param obj Object to serialize
   * @param order Order in which to serialize
   * @return A buffer containing the bytes
   */
  public ByteBuffer writeFixedLengthField(Object obj, ByteOrder order)
    throws IOException
  {
    int size = getType().getFixedSize(_columnLength);

    // create buffer for data
    ByteBuffer buffer = getPageChannel().createBuffer(size, order);

    // since booleans are not written by this method, it's safe to convert any
    // incoming boolean into an integer.
    obj = booleanToInteger(obj);

    switch(getType()) {
    case BOOLEAN:
      //Do nothing
      break;
    case  BYTE:
      buffer.put(toNumber(obj).byteValue());
      break;
    case INT:
      buffer.putShort(toNumber(obj).shortValue());
      break;
    case LONG:
      buffer.putInt(toNumber(obj).intValue());
      break;
    case MONEY:
      writeCurrencyValue(buffer, obj);
      break;
    case FLOAT:
      buffer.putFloat(toNumber(obj).floatValue());
      break;
    case DOUBLE:
      buffer.putDouble(toNumber(obj).doubleValue());
      break;
    case SHORT_DATE_TIME:
      writeDateValue(buffer, obj);
      break;
    case TEXT:
      // apparently text numeric values are also occasionally written as fixed
      // length...
      int numChars = getLengthInUnits();
      // force uncompressed encoding for fixed length text
      buffer.put(encodeTextValue(obj, numChars, numChars, true));
      break;
    case GUID:
      writeGUIDValue(buffer, obj, order);
      break;
    case NUMERIC:
      // yes, that's right, occasionally numeric values are written as fixed
      // length...
      writeNumericValue(buffer, obj);
      break;
    case BINARY:
    case UNKNOWN_0D:
    case UNKNOWN_11:
      byte[] bytes = toByteArray(obj);
      if(bytes.length != getLength()) {
        throw new IOException("Invalid fixed size binary data, size "
                              + getLength() + ", got " + bytes.length);
      }
      buffer.put(bytes);
      break;
    default:
      throw new IOException("Unsupported data type: " + getType());
    }
    buffer.flip();
    return buffer;
  }
  
  /**
   * Decodes a compressed or uncompressed text value.
   */
  private String decodeTextValue(byte[] data)
    throws IOException
  {
    try {

      // see if data is compressed.  the 0xFF, 0xFE sequence indicates that
      // compression is used (sort of, see algorithm below)
      boolean isCompressed = ((data.length > 1) &&
                              (data[0] == TEXT_COMPRESSION_HEADER[0]) &&
                              (data[1] == TEXT_COMPRESSION_HEADER[1]));

      if(isCompressed) {

        Expand expander = new Expand();
        
        // this is a whacky compression combo that switches back and forth
        // between compressed/uncompressed using a 0x00 byte (starting in
        // compressed mode)
        StringBuilder textBuf = new StringBuilder(data.length);
        // start after two bytes indicating compression use
        int dataStart = TEXT_COMPRESSION_HEADER.length;
        int dataEnd = dataStart;
        boolean inCompressedMode = true;
        while(dataEnd < data.length) {
          if(data[dataEnd] == (byte)0x00) {

            // handle current segment
            decodeTextSegment(data, dataStart, dataEnd, inCompressedMode,
                              expander, textBuf);
            inCompressedMode = !inCompressedMode;
            ++dataEnd;
            dataStart = dataEnd;
            
          } else {
            ++dataEnd;
          }
        }
        // handle last segment
        decodeTextSegment(data, dataStart, dataEnd, inCompressedMode,
                          expander, textBuf);

        return textBuf.toString();
        
      }
      
      return decodeUncompressedText(data, getCharset());
      
    } catch (IllegalInputException e) {
      throw (IOException)
        new IOException("Can't expand text column").initCause(e);
    } catch (EndOfInputException e) {
      throw (IOException)
        new IOException("Can't expand text column").initCause(e);
    }
  }

  /**
   * Decodes a segnment of a text value into the given buffer according to the
   * given status of the segment (compressed/uncompressed).
   */
  private void decodeTextSegment(byte[] data, int dataStart, int dataEnd,
                                 boolean inCompressedMode, Expand expander,
                                 StringBuilder textBuf)
    throws IllegalInputException, EndOfInputException
  {
    if(dataEnd <= dataStart) {
      // no data
      return;
    }
    int dataLength = dataEnd - dataStart;
    if(inCompressedMode) {
      // handle compressed data
      byte[] tmpData = new byte[dataLength];
      System.arraycopy(data, dataStart, tmpData, 0, dataLength);
      expander.reset();
      textBuf.append(expander.expand(tmpData));
    } else {
      // handle uncompressed data
      textBuf.append(decodeUncompressedText(data, dataStart, dataLength,
                                            getCharset()));
    }
  }

  /**
   * @param textBytes bytes of text to decode
   * @return the decoded string
   */
  private static CharBuffer decodeUncompressedText(
      byte[] textBytes, int startPos, int length, Charset charset)
  {
    return charset.decode(ByteBuffer.wrap(textBytes, startPos, length));
  }  

  /**
   * Encodes a text value, possibly compressing.
   */
  private ByteBuffer encodeTextValue(Object obj, int minChars, int maxChars,
                                     boolean forceUncompressed)
    throws IOException
  {
    CharSequence text = toCharSequence(obj);
    if((text.length() > maxChars) || (text.length() < minChars)) {
      throw new IOException("Text is wrong length for " + getType() +
                            " column, max " + maxChars
                            + ", min " + minChars + ", got " + text.length());
    }
    
    // may only compress if column type allows it
    if(!forceUncompressed && isCompressedUnicode()) {

      // for now, only do very simple compression (only compress text which is
      // all ascii text)
      if(isAsciiCompressible(text)) {

        byte[] encodedChars = new byte[TEXT_COMPRESSION_HEADER.length + 
                                       text.length()];
        encodedChars[0] = TEXT_COMPRESSION_HEADER[0];
        encodedChars[1] = TEXT_COMPRESSION_HEADER[1];
        for(int i = 0; i < text.length(); ++i) {
          encodedChars[i + TEXT_COMPRESSION_HEADER.length] = 
            (byte)text.charAt(i);
        }
        return ByteBuffer.wrap(encodedChars);
      }
    }

    return encodeUncompressedText(text, getCharset());
  }

  /**
   * Returns {@code true} if the given text can be compressed using simple
   * ASCII encoding, {@code false} otherwise.
   */
  private static boolean isAsciiCompressible(CharSequence text) {
    // only attempt to compress > 2 chars (compressing less than 3 chars would
    // not result in a space savings due to the 2 byte compression header)
    if(text.length() <= TEXT_COMPRESSION_HEADER.length) {
      return false;
    }
    // now, see if it is all printable ASCII
    for(int i = 0; i < text.length(); ++i) {
      char c = text.charAt(i);
      if(!Compress.isAsciiCrLfOrTab(c)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder rtn = new StringBuilder();
    rtn.append("\tName: (" + _table.getName() + ") " + _name);
    rtn.append("\n\tType: 0x" + Integer.toHexString(_type.getValue()) +
               " (" + _type + ")");
    rtn.append("\n\tNumber: " + _columnNumber);
    rtn.append("\n\tLength: " + _columnLength);
    rtn.append("\n\tVariable length: " + _variableLength);
    if(_variableLength) {
      rtn.append("\n\tCompressed Unicode: " + _compressedUnicode);
    }
    if(_autoNumber) {
      rtn.append("\n\tLast AutoNumber: " + _autoNumberGenerator.getLast());
    }
    rtn.append("\n\n");
    return rtn.toString();
  }
  
  /**
   * @param textBytes bytes of text to decode
   * @param charset relevant charset
   * @return the decoded string
   */
  public static String decodeUncompressedText(byte[] textBytes, 
                                              Charset charset)
  {
    return decodeUncompressedText(textBytes, 0, textBytes.length, charset)
      .toString();
  }

  /**
   * @param text Text to encode
   * @param db relevant db
   * @return A buffer with the text encoded
   */
  public static ByteBuffer encodeUncompressedText(CharSequence text,
                                                  Charset charset)
  {
    return charset.encode(CharBuffer.wrap(text));
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
  public static short countVariableLength(List<Column> columns) {
    short rtn = 0;
    for (Column col : columns) {
      if (col.isVariableLength()) {
        rtn++;
      }
    }
    return rtn;
  }

  /**
   * @param columns A list of columns in a table definition
   * @return The number of variable length columns which are not long values
   *         found in the list
   */
  public static short countNonLongVariableLength(List<Column> columns) {
    short rtn = 0;
    for (Column col : columns) {
      if (col.isVariableLength() && !col.getType().isLongValue()) {
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
    } else if(value instanceof BigInteger) {
      return new BigDecimal((BigInteger)value);
    } else if(value instanceof Number) {
      return new BigDecimal(((Number)value).doubleValue());
    }
    return new BigDecimal(value.toString());
  }

  /**
   * @return an appropriate Number representation of the given object.
   *         <code>null</code> is returned as 0 and Strings are parsed as
   *         Doubles.
   */
  private static Number toNumber(Object value)
  {
    if(value == null) {
      return BigDecimal.ZERO;
    } if(value instanceof Number) {
      return (Number)value;
    }
    return Double.valueOf(value.toString());
  }
  
  /**
   * @return an appropriate CharSequence representation of the given object.
   */
  public static CharSequence toCharSequence(Object value)
    throws IOException
  {
    if(value == null) {
      return null;
    } else if(value instanceof CharSequence) {
      return (CharSequence)value;
    } else if(value instanceof Clob) {
      try {
        Clob c = (Clob)value;
        // note, start pos is 1-based
        return c.getSubString(1L, (int)c.length());
      } catch(SQLException e) {
        throw (IOException)(new IOException(e.getMessage())).initCause(e);
      }
    } else if(value instanceof Reader) {
      char[] buf = new char[8 * 1024];
      StringBuilder sout = new StringBuilder();
      Reader in = (Reader)value;
      int read = 0;
      while((read = in.read(buf)) != -1) {
        sout.append(buf, 0, read);
      }
      return sout;
    }

    return value.toString();
  }

  /**
   * @return an appropriate byte[] representation of the given object.
   */
  public static byte[] toByteArray(Object value)
    throws IOException
  {
    if(value == null) {
      return null;
    } else if(value instanceof byte[]) {
      return (byte[])value;
    } else if(value instanceof Blob) {
      try {
        Blob b = (Blob)value;
        // note, start pos is 1-based
        return b.getBytes(1L, (int)b.length());
      } catch(SQLException e) {
        throw (IOException)(new IOException(e.getMessage())).initCause(e);
      }
    }

    ByteArrayOutputStream bout = new ByteArrayOutputStream();

    if(value instanceof InputStream) {
      byte[] buf = new byte[8 * 1024];
      InputStream in = (InputStream)value;
      int read = 0;
      while((read = in.read(buf)) != -1) {
        bout.write(buf, 0, read);
      }
    } else {
      // if all else fails, serialize it
      ObjectOutputStream oos = new ObjectOutputStream(bout);
      oos.writeObject(value);
      oos.close();
    }

    return bout.toByteArray();
  }

  /**
   * Interpret a boolean value (null == false)
   */
  public static boolean toBooleanValue(Object obj) {
    return ((obj != null) && ((Boolean)obj).booleanValue());
  }
  
  /**
   * Swaps the bytes of the given numeric in place.
   */
  private static void fixNumericByteOrder(byte[] bytes)
  {
    // fix endianness of each 4 byte segment
    for(int i = 0; i < 4; ++i) {
      ByteUtil.swap4Bytes(bytes, i * 4);
    }
  }

  /**
   * Treat booleans as integers (C-style).
   */
  private static Object booleanToInteger(Object obj) {
    if (obj instanceof Boolean) {
      obj = ((Boolean) obj) ? 1 : 0;
    }
    return obj;
  }

  /**
   * Returns a wrapper for raw column data that can be written without
   * understanding the data.  Useful for wrapping unparseable data for
   * re-writing.
   */
  static RawData rawDataWrapper(byte[] bytes) {
    return new RawData(bytes);
  }

  /**
   * Returs {@code true} if the given value is "raw" column data,
   * {@code false} otherwise.
   */
  static boolean isRawData(Object value) {
    return(value instanceof RawData);
  }

  /**
   * Date subclass which stashes the original date bits, in case we attempt to
   * re-write the value (will not lose precision).
   */
  private static final class DateExt extends Date
  {
    private static final long serialVersionUID = 0L;

    /** cached bits of the original date value */
    private transient final long _dateBits;

    private DateExt(long time, long dateBits) {
      super(time);
      _dateBits = dateBits;
    }

    public long getDateBits() {
      return _dateBits;
    }
    
    private Object writeReplace() throws ObjectStreamException {
      // if we are going to serialize this Date, convert it back to a normal
      // Date (in case it is restored outside of the context of jackcess)
      return new Date(super.getTime());
    }
  }

  /**
   * Wrapper for raw column data which can be re-written.
   */
  private static class RawData implements Serializable
  {
    private static final long serialVersionUID = 0L;

    private final byte[] _bytes;

    private RawData(byte[] bytes) {
      _bytes = bytes;
    }

    private byte[] getBytes() {
      return _bytes;
    }

    @Override
    public String toString() {
      return "RawData: " + ByteUtil.toHexString(getBytes());
    }

    private Object writeReplace() throws ObjectStreamException {
      // if we are going to serialize this, convert it back to a normal
      // byte[] (in case it is restored outside of the context of jackcess)
      return getBytes();
    }
  }

  /**
   * Base class for the supported autonumber types.
   */
  public abstract class AutoNumberGenerator
  {
    protected AutoNumberGenerator() {}

    /**
     * Returns the last autonumber generated by this generator.  Only valid
     * after a call to {@link Table#addRow}, otherwise undefined.
     */
    public abstract Object getLast();

    /**
     * Returns the next autonumber for this generator.
     * <p>
     * <i>Warning, calling this externally will result in this value being
     * "lost" for the table.</i>
     */
    public abstract Object getNext();

    /**
     * Returns the flags used when writing this column.
     */
    public abstract int getColumnFlags();

    /**
     * Returns the type of values generated by this generator.
     */
    public abstract DataType getType();
  }

  private final class LongAutoNumberGenerator extends AutoNumberGenerator
  {
    private LongAutoNumberGenerator() {}

    @Override
    public Object getLast() {
      // the table stores the last long autonumber used
      return getTable().getLastLongAutoNumber();
    }

    @Override
    public Object getNext() {
      // the table stores the last long autonumber used
      return getTable().getNextLongAutoNumber();
    }

    @Override
    public int getColumnFlags() {
      return AUTO_NUMBER_FLAG_MASK;
    }

    @Override
    public DataType getType() {
      return DataType.LONG;
    }
  }

  private final class GuidAutoNumberGenerator extends AutoNumberGenerator
  {
    private Object _lastAutoNumber;

    private GuidAutoNumberGenerator() {}

    @Override
    public Object getLast() {
      return _lastAutoNumber;
    }

    @Override
    public Object getNext() {
      // format guids consistently w/ Column.readGUIDValue()
      _lastAutoNumber = "{" + UUID.randomUUID() + "}";
      return _lastAutoNumber;
    }

    @Override
    public int getColumnFlags() {
      return AUTO_NUMBER_GUID_FLAG_MASK;
    }

    @Override
    public DataType getType() {
      return DataType.GUID;
    }
  }

}
