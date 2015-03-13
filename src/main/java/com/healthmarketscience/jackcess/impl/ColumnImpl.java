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

package com.healthmarketscience.jackcess.impl;

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
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.impl.complex.ComplexValueForeignKeyImpl;
import com.healthmarketscience.jackcess.util.ColumnValidator;
import com.healthmarketscience.jackcess.util.SimpleColumnValidator;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Access database column definition
 * @author Tim McCune
 * @usage _general_class_
 */
public class ColumnImpl implements Column, Comparable<ColumnImpl> {
  
  protected static final Log LOG = LogFactory.getLog(ColumnImpl.class);
  
  /**
   * Placeholder object for adding rows which indicates that the caller wants
   * the RowId of the new row.  Must be added as an extra value at the end of
   * the row values array.
   * @see TableImpl#asRowWithRowId
   * @usage _intermediate_field_
   */
  public static final Object RETURN_ROW_ID = "<RETURN_ROW_ID>";

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
   * mask for the fixed len bit
   * @usage _advanced_field_
   */
  public static final byte FIXED_LEN_FLAG_MASK = (byte)0x01;
  
  /**
   * mask for the auto number bit
   * @usage _advanced_field_
   */
  public static final byte AUTO_NUMBER_FLAG_MASK = (byte)0x04;
  
  /**
   * mask for the auto number guid bit
   * @usage _advanced_field_
   */
  public static final byte AUTO_NUMBER_GUID_FLAG_MASK = (byte)0x40;
  
  /**
   * mask for the hyperlink bit (on memo types)
   * @usage _advanced_field_
   */
  public static final byte HYPERLINK_FLAG_MASK = (byte)0x80;
  
  /**
   * mask for the unknown bit (possible "can be null"?)
   * @usage _advanced_field_
   */
  public static final byte UNKNOWN_FLAG_MASK = (byte)0x02;

  // some other flags?
  // 0x10: replication related field (or hidden?)

  protected static final byte COMPRESSED_UNICODE_EXT_FLAG_MASK = (byte)0x01;
  private static final byte CALCULATED_EXT_FLAG_MASK = (byte)0xC0;

  static final byte NUMERIC_NEGATIVE_BYTE = (byte)0x80;

  /** the value for the "general" sort order */
  private static final short GENERAL_SORT_ORDER_VALUE = 1033;

  /**
   * the "general" text sort order, legacy version (access 2000-2007)
   * @usage _intermediate_field_
   */
  public static final SortOrder GENERAL_LEGACY_SORT_ORDER =
    new SortOrder(GENERAL_SORT_ORDER_VALUE, (byte)0);

  /**
   * the "general" text sort order, latest version (access 2010+)
   * @usage _intermediate_field_
   */
  public static final SortOrder GENERAL_SORT_ORDER = 
    new SortOrder(GENERAL_SORT_ORDER_VALUE, (byte)1);

  /** pattern matching textual guid strings (allows for optional surrounding
      '{' and '}') */
  private static final Pattern GUID_PATTERN = Pattern.compile("\\s*[{]?([\\p{XDigit}]{8})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{12})[}]?\\s*");

  /** header used to indicate unicode text compression */
  private static final byte[] TEXT_COMPRESSION_HEADER = 
  { (byte)0xFF, (byte)0XFE };
  private static final char MIN_COMPRESS_CHAR = 1;
  private static final char MAX_COMPRESS_CHAR = 0xFF;

  
  /** owning table */
  private final TableImpl _table;
  /** Whether or not the column is of variable length */
  private final boolean _variableLength;
  /** Whether or not the column is an autonumber column */
  private final boolean _autoNumber;
  /** Whether or not the column is a calculated column */
  private final boolean _calculated;
  /** Data type */
  private final DataType _type;
  /** Maximum column length */
  private final short _columnLength;
  /** 0-based column number */
  private final short _columnNumber;
  /** index of the data for this column within a list of row data */
  private int _columnIndex;
  /** display index of the data for this column */
  private final int _displayIndex;
  /** Column name */
  private final String _name;
  /** the offset of the fixed data in the row */
  private final int _fixedDataOffset;
  /** the index of the variable length data in the var len offset table */
  private final int _varLenTableIndex;
  /** the auto number generator for this column (if autonumber column) */
  private final AutoNumberGenerator _autoNumberGenerator;
  /** properties for this column, if any */
  private PropertyMap _props;  
  /** Validator for writing new values */
  private ColumnValidator _validator = SimpleColumnValidator.INSTANCE;
  
  /**
   * @usage _advanced_method_
   */
  protected ColumnImpl(TableImpl table, String name, DataType type,
                       int colNumber, int fixedOffset, int varLenIndex) {
    _table = table;
    _name = name;
    _type = type;

    if(!_type.isVariableLength()) {
      _columnLength = (short)type.getFixedSize();
    } else {
      _columnLength = (short)type.getMaxSize();
    }
    _variableLength = type.isVariableLength();
    _autoNumber = false;
    _calculated = false;
    _autoNumberGenerator = null;
    _columnNumber = (short)colNumber;
    _columnIndex = colNumber;
    _displayIndex = colNumber;
    _fixedDataOffset = fixedOffset;
    _varLenTableIndex = varLenIndex;
  }
    
  /**
   * Read a column definition in from a buffer
   * @usage _advanced_method_
   */
  ColumnImpl(InitArgs args)
    throws IOException
  {
    _table = args.table;
    _name = args.name;
    _displayIndex = args.displayIndex;
    _type = args.type;
    
    _columnNumber = args.buffer.getShort(
        args.offset + getFormat().OFFSET_COLUMN_NUMBER);
    _columnLength = args.buffer.getShort(
        args.offset + getFormat().OFFSET_COLUMN_LENGTH);
    
    _variableLength = ((args.flags & FIXED_LEN_FLAG_MASK) == 0);
    _autoNumber = ((args.flags & 
                    (AUTO_NUMBER_FLAG_MASK | AUTO_NUMBER_GUID_FLAG_MASK)) != 0);
    _calculated = ((args.extFlags & CALCULATED_EXT_FLAG_MASK) != 0);
    
    _autoNumberGenerator = createAutoNumberGenerator();
    
    if(_variableLength) {
      _varLenTableIndex = args.buffer.getShort(
          args.offset + getFormat().OFFSET_COLUMN_VARIABLE_TABLE_INDEX);
      _fixedDataOffset = 0;
    } else {
      _fixedDataOffset = args.buffer.getShort(
          args.offset + getFormat().OFFSET_COLUMN_FIXED_DATA_OFFSET);
      _varLenTableIndex = 0;
    }
  }
  
  /**
   * Creates the appropriate ColumnImpl class and reads a column definition in
   * from a buffer
   * @param table owning table
   * @param buffer Buffer containing column definition
   * @param offset Offset in the buffer at which the column definition starts
   * @usage _advanced_method_
   */
  public static ColumnImpl create(TableImpl table, ByteBuffer buffer,
                                  int offset, String name, int displayIndex)
    throws IOException
  {
    InitArgs args = new InitArgs(table, buffer, offset, name, displayIndex);

    boolean calculated = ((args.extFlags & CALCULATED_EXT_FLAG_MASK) != 0);
    byte colType = args.colType;
    if(calculated) {
      // "real" data type is in the "result type" property
      PropertyMap colProps = table.getPropertyMaps().get(name);
      Byte resultType = (Byte)colProps.getValue(PropertyMap.RESULT_TYPE_PROP);
      if(resultType != null) {
        colType = resultType;
      }
    }
    
    try {
      args.type = DataType.fromByte(colType);
    } catch(IOException e) {
      LOG.warn(withErrorContext("Unsupported column type " + colType,
                                table.getDatabase(), table.getName(), name));
      boolean variableLength = ((args.flags & FIXED_LEN_FLAG_MASK) == 0);
      args.type = (variableLength ? DataType.UNSUPPORTED_VARLEN :
                   DataType.UNSUPPORTED_FIXEDLEN);
      return new UnsupportedColumnImpl(args);
    }

    if(calculated) {
      return CalculatedColumnUtil.create(args);
    }
    
    switch(args.type) {
    case TEXT:
      return new TextColumnImpl(args);
    case MEMO:
      return new MemoColumnImpl(args);
    case COMPLEX_TYPE:
      return new ComplexColumnImpl(args);
    default:
      // fall through
    }

    if(args.type.getHasScalePrecision()) {
      return new NumericColumnImpl(args);
    }
    if(args.type.isLongValue()) {
      return new LongValueColumnImpl(args);
    }
    
    return new ColumnImpl(args);
  }

   /**
   * Sets the usage maps for this column.
   */
  void setUsageMaps(UsageMap ownedPages, UsageMap freeSpacePages) {
    // base does nothing
  }

  /**
   * Secondary column initialization after the table is fully loaded.
   */
  void postTableLoadInit() throws IOException {
    // base does nothing
  }

  public TableImpl getTable() {
    return _table;
  }

  public DatabaseImpl getDatabase() {       
    return getTable().getDatabase();
  }
  
  /**
   * @usage _advanced_method_
   */
  public JetFormat getFormat() {
    return getDatabase().getFormat();
  }

  /**
   * @usage _advanced_method_
   */
  public PageChannel getPageChannel() {
    return getDatabase().getPageChannel();
  }
  
  public String getName() {
    return _name;
  }
  
  public boolean isVariableLength() {
    return _variableLength;
  }
  
  public boolean isAutoNumber() {
    return _autoNumber;
  }

  /**
   * @usage _advanced_method_
   */
  public short getColumnNumber() {
    return _columnNumber;
  }

  public int getColumnIndex() {
    return _columnIndex;
  }

  /**
   * @usage _advanced_method_
   */
  public void setColumnIndex(int newColumnIndex) {
    _columnIndex = newColumnIndex;
  }
  
  /**
   * @usage _advanced_method_
   */
  public int getDisplayIndex() {
    return _displayIndex;
  }

  public DataType getType() {
    return _type;
  }
  
  public int getSQLType() throws SQLException {
    return _type.getSQLType();
  }
  
  public boolean isCompressedUnicode() {
    return false;
  }

  public byte getPrecision() {
    return (byte)getType().getDefaultPrecision();
  }
  
  public byte getScale() {
    return (byte)getType().getDefaultScale();
  }

  /**
   * @usage _intermediate_method_
   */
  public SortOrder getTextSortOrder() {
    return null;
  }

  /**
   * @usage _intermediate_method_
   */
  public short getTextCodePage() {
    return 0;
  }

  public short getLength() {
    return _columnLength;
  }

  public short getLengthInUnits() {
    return (short)getType().toUnitSize(getLength());
  }

  public boolean isCalculated() {
    return _calculated;
  }
  
  /**
   * @usage _advanced_method_
   */
  public int getVarLenTableIndex() {
    return _varLenTableIndex;
  }
  
  /**
   * @usage _advanced_method_
   */
  public int getFixedDataOffset() {
    return _fixedDataOffset;
  }

  protected Charset getCharset() {
    return getDatabase().getCharset();
  }

  protected Calendar getCalendar() {
    return getDatabase().getCalendar();
  }

  public boolean isAppendOnly() {
    return (getVersionHistoryColumn() != null);
  }
  
  public ColumnImpl getVersionHistoryColumn() {
    return null;
  }

  /**
   * Returns the number of database pages owned by this column.
   * @usage _intermediate_method_
   */
  public int getOwnedPageCount() {
    return 0;
  }

  /**
   * @usage _advanced_method_
   */
  public void setVersionHistoryColumn(ColumnImpl versionHistoryCol) {
    throw new UnsupportedOperationException();
  }

  public boolean isHyperlink() {
    return false;
  }
  
  public ComplexColumnInfo<? extends ComplexValue> getComplexInfo() {
    return null;
  }

  public ColumnValidator getColumnValidator() {
    return _validator;
  }
  
  public void setColumnValidator(ColumnValidator newValidator) {
    
    if(isAutoNumber()) {
      // cannot set autonumber validator (autonumber values are controlled
      // internally)
      if(newValidator != null) {
        throw new IllegalArgumentException(withErrorContext(
                "Cannot set ColumnValidator for autonumber columns"));
      }
      // just leave default validator instance alone
      return;
    }
    
    if(newValidator == null) {
      newValidator = getDatabase().getColumnValidatorFactory()
        .createValidator(this);
      if(newValidator == null) {
        newValidator = SimpleColumnValidator.INSTANCE;
      }
    }
    _validator = newValidator;
  }
  
  byte getOriginalDataType() {
    return _type.getValue();
  }
  
  private AutoNumberGenerator createAutoNumberGenerator() {
    if(!_autoNumber || (_type == null)) {
      return null;
    }

    switch(_type) {
    case LONG:
      return new LongAutoNumberGenerator();
    case GUID:
      return new GuidAutoNumberGenerator();
    case COMPLEX_TYPE:
      return new ComplexTypeAutoNumberGenerator();
    default:
      LOG.warn(withErrorContext("Unknown auto number column type " + _type));
      return new UnsupportedAutoNumberGenerator(_type);
    }
  }

  /**
   * Returns the AutoNumberGenerator for this column if this is an autonumber
   * column, {@code null} otherwise.
   * @usage _advanced_method_
   */
  public AutoNumberGenerator getAutoNumberGenerator() {
    return _autoNumberGenerator;
  }

  public PropertyMap getProperties() throws IOException {
    if(_props == null) {
      _props = getTable().getPropertyMaps().get(getName());
    }
    return _props;
  }
  
  public Object setRowValue(Object[] rowArray, Object value) {
    rowArray[_columnIndex] = value;
    return value;
  }
  
  public Object setRowValue(Map<String,Object> rowMap, Object value) {
    rowMap.put(_name, value);
    return value;
  }
  
  public Object getRowValue(Object[] rowArray) {
    return rowArray[_columnIndex];
  }
  
  public Object getRowValue(Map<String,?> rowMap) {
    return rowMap.get(_name);
  }

  public boolean storeInNullMask() {
    return (getType() == DataType.BOOLEAN);
  }
  
  public boolean writeToNullMask(Object value) {
    return toBooleanValue(value);
  }

  public Object readFromNullMask(boolean isNull) {
    return Boolean.valueOf(!isNull);
  }

  /**
   * Deserialize a raw byte value for this column into an Object
   * @param data The raw byte value
   * @return The deserialized Object
   * @usage _advanced_method_
   */
  public Object read(byte[] data) throws IOException {
    return read(data, PageChannel.DEFAULT_BYTE_ORDER);
  }
  
  /**
   * Deserialize a raw byte value for this column into an Object
   * @param data The raw byte value
   * @param order Byte order in which the raw value is stored
   * @return The deserialized Object
   * @usage _advanced_method_
   */  
  public Object read(byte[] data, ByteOrder order) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(data).order(order);

    switch(getType()) {
    case BOOLEAN:
      throw new IOException(withErrorContext("Tried to read a boolean from data instead of null mask."));
    case BYTE:
      return Byte.valueOf(buffer.get());
    case INT:
      return Short.valueOf(buffer.getShort());
    case LONG:
      return Integer.valueOf(buffer.getInt());
    case DOUBLE:
      return Double.valueOf(buffer.getDouble());
    case FLOAT:
      return Float.valueOf(buffer.getFloat());
    case SHORT_DATE_TIME:
      return readDateValue(buffer);
    case BINARY:
      return data;
    case TEXT:
      return decodeTextValue(data);
    case MONEY:
      return readCurrencyValue(buffer);
    case NUMERIC:
      return readNumericValue(buffer);
    case GUID:
      return readGUIDValue(buffer, order);
    case UNKNOWN_0D:
    case UNKNOWN_11:
      // treat like "binary" data
      return data;
    case COMPLEX_TYPE:
      return new ComplexValueForeignKeyImpl(this, buffer.getInt());
    default:
      throw new IOException(withErrorContext("Unrecognized data type: " + _type));
    }
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
      throw new IOException(withErrorContext("Invalid money value"));
    }
    
    return new BigDecimal(BigInteger.valueOf(buffer.getLong(0)), 4);
  }

  /**
   * Writes "Currency" values.
   */
  private void writeCurrencyValue(ByteBuffer buffer, Object value)
    throws IOException
  {
    Object inValue = value;
    try {
      BigDecimal decVal = toBigDecimal(value);
      inValue = decVal;

      // adjust scale (will cause the an ArithmeticException if number has too
      // many decimal places)
      decVal = decVal.setScale(4);
    
      // now, remove scale and convert to long (this will throw if the value is
      // too big)
      buffer.putLong(decVal.movePointRight(4).longValueExact());
    } catch(ArithmeticException e) {
      throw (IOException)
        new IOException(withErrorContext(
                "Currency value '" + inValue + "' out of range"))
        .initCause(e);
    }
  }

  /**
   * Decodes a NUMERIC field.
   */
  private BigDecimal readNumericValue(ByteBuffer buffer)
  {
    boolean negate = (buffer.get() != 0);

    byte[] tmpArr = ByteUtil.getBytes(buffer, 16);

    if(buffer.order() != ByteOrder.BIG_ENDIAN) {
      fixNumericByteOrder(tmpArr);
    }

    return toBigDecimal(tmpArr, negate, getScale());
  }

  static BigDecimal toBigDecimal(byte[] bytes, boolean negate, int scale)
  {
    if((bytes[0] & 0x80) != 0) {
      // the data is effectively unsigned, but the BigInteger handles it as
      // signed twos complement.  we need to add an extra byte to the input so
      // that it will be treated as unsigned
      bytes = ByteUtil.copyOf(bytes, 0, bytes.length + 1, 1);
    }
    BigInteger intVal = new BigInteger(bytes);
    if(negate) {
      intVal = intVal.negate();
    }
    return new BigDecimal(intVal, scale);
  }

  /**
   * Writes a numeric value.
   */
  private void writeNumericValue(ByteBuffer buffer, Object value)
    throws IOException
  {
    Object inValue = value;
    try {
      BigDecimal decVal = toBigDecimal(value);
      inValue = decVal;

      int signum = decVal.signum();
      if(signum < 0) {
        decVal = decVal.negate();
      }

      // write sign byte
      buffer.put((signum < 0) ? NUMERIC_NEGATIVE_BYTE : 0);

      // adjust scale according to this column type (will cause the an
      // ArithmeticException if number has too many decimal places)
      decVal = decVal.setScale(getScale());

      // check precision
      if(decVal.precision() > getPrecision()) {
        throw new IOException(withErrorContext(
            "Numeric value is too big for specified precision "
            + getPrecision() + ": " + decVal));
      }
    
      // convert to unscaled BigInteger, big-endian bytes
      byte[] intValBytes = toUnscaledByteArray(
          decVal, getType().getFixedSize() - 1);
      if(buffer.order() != ByteOrder.BIG_ENDIAN) {
        fixNumericByteOrder(intValBytes);
      }
      buffer.put(intValBytes);
    } catch(ArithmeticException e) {
      throw (IOException)
        new IOException(withErrorContext(
                "Numeric value '" + inValue + "' out of range"))
        .initCause(e);
    }
  }

  byte[] toUnscaledByteArray(BigDecimal decVal, int maxByteLen)
    throws IOException
  {
    // convert to unscaled BigInteger, big-endian bytes
    byte[] intValBytes = decVal.unscaledValue().toByteArray();
    if(intValBytes.length > maxByteLen) {
      if((intValBytes[0] == 0) && ((intValBytes.length - 1) == maxByteLen)) {
        // in order to not return a negative two's complement value,
        // toByteArray() may return an extra leading 0 byte.  we are working
        // with unsigned values, so we can drop the extra leading 0
        intValBytes = ByteUtil.copyOf(intValBytes, 1, maxByteLen);
      } else {
        throw new IOException(withErrorContext(
                                  "Too many bytes for valid BigInteger?"));
      }
    } else if(intValBytes.length < maxByteLen) {
      intValBytes = ByteUtil.copyOf(intValBytes, 0, maxByteLen, 
                                    (maxByteLen - intValBytes.length));
    }
    return intValBytes;
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
   * @usage _advanced_method_
   */
  public long fromDateDouble(double value)
  {
    long time = Math.round(value * MILLISECONDS_PER_DAY);
    time -= MILLIS_BETWEEN_EPOCH_AND_1900;
    time -= getFromLocalTimeZoneOffset(time);
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
   * @usage _advanced_method_
   */
  public double toDateDouble(Object value)
  {
    // seems access stores dates in the local timezone.  guess you just
    // hope you read it in the same timezone in which it was written!
    long time = toDateLong(value);
    time += getToLocalTimeZoneOffset(time);
    time += MILLIS_BETWEEN_EPOCH_AND_1900;
    return time / MILLISECONDS_PER_DAY;
  }

  /**
   * @return an appropriate Date long value for the given object
   */
  private static long toDateLong(Object value) 
  {
    return ((value instanceof Date) ?
            ((Date)value).getTime() :
            ((value instanceof Calendar) ?
             ((Calendar)value).getTimeInMillis() :
             ((Number)value).longValue()));
  }

  /**
   * Gets the timezone offset from UTC to local time for the given time
   * (including DST).
   */
  private long getToLocalTimeZoneOffset(long time)
  {
    Calendar c = getCalendar();
    c.setTimeInMillis(time);
    return ((long)c.get(Calendar.ZONE_OFFSET) + c.get(Calendar.DST_OFFSET));
  }  
  
  /**
   * Gets the timezone offset from local time to UTC for the given time
   * (including DST).
   */
  private long getFromLocalTimeZoneOffset(long time)
  {
    // getting from local time back to UTC is a little wonky (and not
    // guaranteed to get you back to where you started)
    Calendar c = getCalendar();
    c.setTimeInMillis(time);
    // apply the zone offset first to get us closer to the original time
    c.setTimeInMillis(time - c.get(Calendar.ZONE_OFFSET));
    return ((long)c.get(Calendar.ZONE_OFFSET) + c.get(Calendar.DST_OFFSET));
  }  
  
  /**
   * Decodes a GUID value.
   */
  private static String readGUIDValue(ByteBuffer buffer, ByteOrder order)
  {
    if(order != ByteOrder.BIG_ENDIAN) {
      byte[] tmpArr = ByteUtil.getBytes(buffer, 16);

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
  private void writeGUIDValue(ByteBuffer buffer, Object value)
    throws IOException
  {
    Matcher m = GUID_PATTERN.matcher(toCharSequence(value));
    if(!m.matches()) {
      throw new IOException(withErrorContext("Invalid GUID: " + value));
    }

    ByteBuffer origBuffer = null;
    byte[] tmpBuf = null;
    if(buffer.order() != ByteOrder.BIG_ENDIAN) {
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
  }

  /**
   * Returns {@code true} if the given value is a "guid" value.
   */
  static boolean isGUIDValue(Object value) throws IOException {
    return GUID_PATTERN.matcher(toCharSequence(value)).matches();
  }

  /**
   * Passes the given obj through the currently configured validator for this
   * column and returns the result.
   */
  public Object validate(Object obj) throws IOException {
    return _validator.validate(this, obj);
  }
  
  /**
   * Serialize an Object into a raw byte value for this column in little
   * endian order
   * @param obj Object to serialize
   * @return A buffer containing the bytes
   * @usage _advanced_method_
   */
  public ByteBuffer write(Object obj, int remainingRowLength)
    throws IOException
  {
    return write(obj, remainingRowLength, PageChannel.DEFAULT_BYTE_ORDER);
  }
  
  /**
   * Serialize an Object into a raw byte value for this column
   * @param obj Object to serialize
   * @param order Order in which to serialize
   * @return A buffer containing the bytes
   * @usage _advanced_method_
   */
  public ByteBuffer write(Object obj, int remainingRowLength, ByteOrder order)
    throws IOException
  {
    if(isRawData(obj)) {
      // just slap it right in (not for the faint of heart!)
      return ByteBuffer.wrap(((RawData)obj).getBytes());
    }

    return writeRealData(obj, remainingRowLength, order);
  }

  protected ByteBuffer writeRealData(Object obj, int remainingRowLength, 
                                     ByteOrder order)
    throws IOException
  {
    if(!isVariableLength() || !getType().isVariableLength()) {
      return writeFixedLengthField(obj, order);
    }
      
    // this is an "inline" var length field
    switch(getType()) {
    case NUMERIC:
      // don't ask me why numerics are "var length" columns...
      ByteBuffer buffer = PageChannel.createBuffer(
          getType().getFixedSize(), order);
      writeNumericValue(buffer, obj);
      buffer.flip();
      return buffer;

    case TEXT:
      return encodeTextValue(
          obj, 0, getLengthInUnits(), false).order(order);
        
    case BINARY:
    case UNKNOWN_0D:
    case UNSUPPORTED_VARLEN:
      // should already be "encoded"
      break;
    default:
      throw new RuntimeException(withErrorContext(
              "unexpected inline var length type: " + getType()));
    }

    ByteBuffer buffer = ByteBuffer.wrap(toByteArray(obj)).order(order);
    return buffer;
  }

  /**
   * Serialize an Object into a raw byte value for this column
   * @param obj Object to serialize
   * @param order Order in which to serialize
   * @return A buffer containing the bytes
   * @usage _advanced_method_
   */
  protected ByteBuffer writeFixedLengthField(Object obj, ByteOrder order)
    throws IOException
  {
    int size = getType().getFixedSize(_columnLength);

    ByteBuffer buffer = writeFixedLengthField(
        obj, PageChannel.createBuffer(size, order));
    buffer.flip();
    return buffer;
  }

  protected ByteBuffer writeFixedLengthField(Object obj, ByteBuffer buffer)
    throws IOException
  {
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
      writeGUIDValue(buffer, obj);
      break;
    case NUMERIC:
      // yes, that's right, occasionally numeric values are written as fixed
      // length...
      writeNumericValue(buffer, obj);
      break;
    case BINARY:
    case UNKNOWN_0D:
    case UNKNOWN_11:
    case COMPLEX_TYPE:
      buffer.putInt(toNumber(obj).intValue());
      break;
    case UNSUPPORTED_FIXEDLEN:
      byte[] bytes = toByteArray(obj);
      if(bytes.length != getLength()) {
        throw new IOException(withErrorContext(
                                  "Invalid fixed size binary data, size "
                                  + getLength() + ", got " + bytes.length));
      }
      buffer.put(bytes);
      break;
    default:
      throw new IOException(withErrorContext(
                                "Unsupported data type: " + getType()));
    }
    return buffer;
  }
  
  /**
   * Decodes a compressed or uncompressed text value.
   */
  String decodeTextValue(byte[] data)
    throws IOException
  {
    // see if data is compressed.  the 0xFF, 0xFE sequence indicates that
    // compression is used (sort of, see algorithm below)
    boolean isCompressed = ((data.length > 1) &&
                            (data[0] == TEXT_COMPRESSION_HEADER[0]) &&
                            (data[1] == TEXT_COMPRESSION_HEADER[1]));

    if(isCompressed) {
        
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
                            textBuf);
          inCompressedMode = !inCompressedMode;
          ++dataEnd;
          dataStart = dataEnd;
            
        } else {
          ++dataEnd;
        }
      }
      // handle last segment
      decodeTextSegment(data, dataStart, dataEnd, inCompressedMode, textBuf);

      return textBuf.toString();
        
    }
      
    return decodeUncompressedText(data, getCharset());
  }

  /**
   * Decodes a segnment of a text value into the given buffer according to the
   * given status of the segment (compressed/uncompressed).
   */
  private void decodeTextSegment(byte[] data, int dataStart, int dataEnd,
                                 boolean inCompressedMode, 
                                 StringBuilder textBuf)
  {
    if(dataEnd <= dataStart) {
      // no data
      return;
    }
    int dataLength = dataEnd - dataStart;

    if(inCompressedMode) {
      byte[] tmpData = new byte[dataLength * 2];
      int tmpIdx = 0;
      for(int i = dataStart; i < dataEnd; ++i) {
        tmpData[tmpIdx] = data[i];
        tmpIdx += 2;
      } 
      data = tmpData;
      dataStart = 0;
      dataLength = data.length;
    }

    textBuf.append(decodeUncompressedText(data, dataStart, dataLength,
                                          getCharset()));
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
  ByteBuffer encodeTextValue(Object obj, int minChars, int maxChars,
                             boolean forceUncompressed)
    throws IOException
  {
    CharSequence text = toCharSequence(obj);
    if((text.length() > maxChars) || (text.length() < minChars)) {
      throw new IOException(withErrorContext(
                            "Text is wrong length for " + getType() +
                            " column, max " + maxChars
                            + ", min " + minChars + ", got " + text.length()));
    }
    
    // may only compress if column type allows it
    if(!forceUncompressed && isCompressedUnicode() &&
       (text.length() <= getFormat().MAX_COMPRESSED_UNICODE_SIZE) &&
       isUnicodeCompressible(text)) {

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

    return encodeUncompressedText(text, getCharset());
  }

  /**
   * Returns {@code true} if the given text can be compressed using compressed
   * unicode, {@code false} otherwise.
   */
  private static boolean isUnicodeCompressible(CharSequence text) {
    // only attempt to compress > 2 chars (compressing less than 3 chars would
    // not result in a space savings due to the 2 byte compression header)
    if(text.length() <= TEXT_COMPRESSION_HEADER.length) {
      return false;
    }
    // now, see if it is all compressible characters
    for(int i = 0; i < text.length(); ++i) {
      char c = text.charAt(i);
      if((c < MIN_COMPRESS_CHAR) || (c > MAX_COMPRESS_CHAR)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Constructs a byte containing the flags for this column.
   */
  private static byte getColumnBitFlags(ColumnBuilder col) {
    byte flags = UNKNOWN_FLAG_MASK;
    if(!col.isVariableLength()) {
      flags |= FIXED_LEN_FLAG_MASK;
    }
    if(col.isAutoNumber()) {
      byte autoNumFlags = 0;
      switch(col.getType()) {
      case LONG:
      case COMPLEX_TYPE:
        autoNumFlags = AUTO_NUMBER_FLAG_MASK;
        break;
      case GUID:
        autoNumFlags = AUTO_NUMBER_GUID_FLAG_MASK;
        break;
      default:
        // unknown autonum type
      }
      flags |= autoNumFlags;
    }
    if(col.isHyperlink()) {
      flags |= HYPERLINK_FLAG_MASK;
    }
    return flags;
  }
  
  @Override
  public String toString() {
    ToStringBuilder sb = CustomToStringStyle.builder(this)
      .append("name", "(" + _table.getName() + ") " + _name);
    byte typeValue = getOriginalDataType();
    sb.append("type", "0x" + Integer.toHexString(typeValue) +
              " (" + _type + ")")
      .append("number", _columnNumber)
      .append("length", _columnLength)
      .append("variableLength", _variableLength);       
    if(_calculated) {
      sb.append("calculated", _calculated);
    }
    if(_type.isTextual()) {
      sb.append("compressedUnicode", isCompressedUnicode())
        .append("textSortOrder", getTextSortOrder());
      if(getTextCodePage() > 0) {
        sb.append("textCodePage", getTextCodePage());
      }
      if(isAppendOnly()) {
        sb.append("appendOnly", isAppendOnly());
      } 
      if(isHyperlink()) {
        sb.append("hyperlink", isHyperlink());
      } 
    }
    if(_type.getHasScalePrecision()) {
      sb.append("precision", getPrecision())
        .append("scale", getScale());
    }
    if(_autoNumber) {
      sb.append("lastAutoNumber", _autoNumberGenerator.getLast());
    }
    if(getComplexInfo() != null) {
      sb.append("complexInfo", getComplexInfo());
    }
    return sb.toString();
  }
  
  /**
   * @param textBytes bytes of text to decode
   * @param charset relevant charset
   * @return the decoded string
   * @usage _advanced_method_
   */
  public static String decodeUncompressedText(byte[] textBytes, 
                                              Charset charset)
  {
    return decodeUncompressedText(textBytes, 0, textBytes.length, charset)
      .toString();
  }

  /**
   * @param text Text to encode
   * @param charset database charset
   * @return A buffer with the text encoded
   * @usage _advanced_method_
   */
  public static ByteBuffer encodeUncompressedText(CharSequence text,
                                                  Charset charset)
  {
    CharBuffer cb = ((text instanceof CharBuffer) ? 
                     (CharBuffer)text : CharBuffer.wrap(text));
    return charset.encode(cb);
  }

  
  /**
   * Orders Columns by column number.
   * @usage _general_method_
   */
  public int compareTo(ColumnImpl other) {
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
   * @usage _advanced_method_
   */
  public static short countVariableLength(List<ColumnBuilder> columns) {
    short rtn = 0;
    for (ColumnBuilder col : columns) {
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
   * @usage _advanced_method_
   */
  private static short countNonLongVariableLength(List<ColumnBuilder> columns) {
    short rtn = 0;
    for (ColumnBuilder col : columns) {
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
  static BigDecimal toBigDecimal(Object value)
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
    } else if(value instanceof Number) {
      return (Number)value;
    }
    return Double.valueOf(value.toString());
  }
  
  /**
   * @return an appropriate CharSequence representation of the given object.
   * @usage _advanced_method_
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
   * @usage _advanced_method_
   */
  public static byte[] toByteArray(Object value)
    throws IOException
  {
    if(value == null) {
      return null;
    } else if(value instanceof byte[]) {
      return (byte[])value;
    } else if(value instanceof OleUtil.OleBlobImpl) {
      return ((OleUtil.OleBlobImpl)value).getBytes();
    } else if(value instanceof Blob) {
      try {
        Blob b = (Blob)value;
        // note, start pos is 1-based
        return b.getBytes(1L, (int)b.length());
      } catch(SQLException e) {
        throw (IOException)(new IOException(e.getMessage())).initCause(e);
      }
    } else if(value instanceof RawData) {
      return ((RawData)value).getBytes();
    }

    ByteArrayOutputStream bout = new ByteArrayOutputStream();

    if(value instanceof InputStream) {
      ByteUtil.copy((InputStream)value, bout);
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
   * @usage _advanced_method_
   */
  public static boolean toBooleanValue(Object obj) {
    if(obj == null) {
      return false;
    } else if(obj instanceof Boolean) {
      return ((Boolean)obj).booleanValue();
    }
    return Boolean.parseBoolean(obj.toString());
  }
  
  /**
   * Swaps the bytes of the given numeric in place.
   */
  private static void fixNumericByteOrder(byte[] bytes)
  {
    // fix endianness of each 4 byte segment
    for(int i = 0; i < bytes.length; i+=4) {
      ByteUtil.swap4Bytes(bytes, i);
    }
  }

  /**
   * Treat booleans as integers (C-style).
   */
  protected static Object booleanToInteger(Object obj) {
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
  public static RawData rawDataWrapper(byte[] bytes) {
    return new RawData(bytes);
  }

  /**
   * Returns {@code true} if the given value is "raw" column data,
   * {@code false} otherwise.
   * @usage _advanced_method_
   */
  public static boolean isRawData(Object value) {
    return(value instanceof RawData);
  }

  /**
   * Writes the column definitions into a table definition buffer.
   * @param buffer Buffer to write to
   */
  protected static void writeDefinitions(TableCreator creator, ByteBuffer buffer)
    throws IOException
  {
    List<ColumnBuilder> columns = creator.getColumns();
    short fixedOffset = (short) 0;
    short variableOffset = (short) 0;
    // we specifically put the "long variable" values after the normal
    // variable length values so that we have a better chance of fitting it
    // all (because "long variable" values can go in separate pages)
    short longVariableOffset = countNonLongVariableLength(columns);
    for (ColumnBuilder col : columns) {

      buffer.put(col.getType().getValue());
      buffer.putInt(TableImpl.MAGIC_TABLE_NUMBER);  //constant magic number
      buffer.putShort(col.getColumnNumber());  //Column Number

      if(col.isVariableLength()) {
        if(!col.getType().isLongValue()) {
          buffer.putShort(variableOffset++);
        } else {
          buffer.putShort(longVariableOffset++);
        }          
      } else {
        buffer.putShort((short) 0);
      }

      buffer.putShort(col.getColumnNumber()); //Column Number again

      if(col.getType().isTextual()) {
        // this will write 4 bytes (note we don't support writing dbs which
        // use the text code page)
        writeSortOrder(buffer, col.getTextSortOrder(), creator.getFormat());
      } else {
        // note scale/precision not stored for calculated numeric fields
        if(col.getType().getHasScalePrecision() && !col.isCalculated()) {
          buffer.put(col.getPrecision());  // numeric precision
          buffer.put(col.getScale());  // numeric scale
        } else {
          buffer.put((byte) 0x00); //unused
          buffer.put((byte) 0x00); //unused
        }
        buffer.putShort((short) 0); //Unknown
      }

      buffer.put(getColumnBitFlags(col)); // misc col flags

      // note access doesn't seem to allow unicode compression for calced fields
      if(col.isCalculated()) {
        buffer.put(CALCULATED_EXT_FLAG_MASK);
      } else if (col.isCompressedUnicode()) {  //Compressed
        buffer.put(COMPRESSED_UNICODE_EXT_FLAG_MASK);
      } else {
        buffer.put((byte)0);
      }

      buffer.putInt(0); //Unknown, but always 0.

      //Offset for fixed length columns
      if(col.isVariableLength()) {
        buffer.putShort((short) 0);
      } else {
        buffer.putShort(fixedOffset);
        fixedOffset += col.getType().getFixedSize(col.getLength());
      }

      if(!col.getType().isLongValue()) {
        short length = col.getLength();
        if(col.isCalculated()) {
          // calced columns have additional value overhead
          if(!col.getType().isVariableLength() || 
             col.getType().getHasScalePrecision()) {
            length = CalculatedColumnUtil.CALC_FIXED_FIELD_LEN; 
          } else {
            length += CalculatedColumnUtil.CALC_EXTRA_DATA_LEN;
          }
        }
        buffer.putShort(length); //Column length
      } else {
        buffer.putShort((short)0x0000); // unused
      }

    }
    for (ColumnBuilder col : columns) {
      TableImpl.writeName(buffer, col.getName(), creator.getCharset());
    }
  }

  /**
   * Reads the sort order info from the given buffer from the given position.
   */
  static SortOrder readSortOrder(ByteBuffer buffer, int position,
                                 JetFormat format)
  {
    short value = buffer.getShort(position);
    byte version = 0;
    if(format.SIZE_SORT_ORDER == 4) {
      version = buffer.get(position + 3);
    }

    if(value == 0) {
      // probably a file we wrote, before handling sort order
      return format.DEFAULT_SORT_ORDER;
    }
    
    if(value == GENERAL_SORT_ORDER_VALUE) {
      if(version == GENERAL_LEGACY_SORT_ORDER.getVersion()) {
        return GENERAL_LEGACY_SORT_ORDER;
      }
      if(version == GENERAL_SORT_ORDER.getVersion()) {
        return GENERAL_SORT_ORDER;
      }
    }
    return new SortOrder(value, version);
  }

  /**
   * Reads the column cade page info from the given buffer, if supported for
   * this db.
   */
  static short readCodePage(ByteBuffer buffer, int offset, JetFormat format)
  {
      int cpOffset = format.OFFSET_COLUMN_CODE_PAGE;
      return ((cpOffset >= 0) ? buffer.getShort(offset + cpOffset) : 0);
  }

  /**
   * Read the extra flags field for a column definition.
   */
  static byte readExtraFlags(ByteBuffer buffer, int offset, JetFormat format)
  {
    int extFlagsOffset = format.OFFSET_COLUMN_EXT_FLAGS;
    return ((extFlagsOffset >= 0) ? buffer.get(offset + extFlagsOffset) : 0);
  }
  
  /**
   * Writes the sort order info to the given buffer at the current position.
   */
  private static void writeSortOrder(ByteBuffer buffer, SortOrder sortOrder,
                                     JetFormat format) {
    if(sortOrder == null) {
      sortOrder = format.DEFAULT_SORT_ORDER;
    }
    buffer.putShort(sortOrder.getValue());      
    if(format.SIZE_SORT_ORDER == 4) {
      buffer.put((byte)0x00); // unknown
      buffer.put(sortOrder.getVersion());
    }
  }

  /**
   * Returns {@code true} if the value is immutable, {@code false} otherwise.
   * This only handles values that are returned from the {@link #read} method.
   */
  static boolean isImmutableValue(Object value) {
    // for now, the only mutable value this class returns is byte[]
    return !(value instanceof byte[]);
  }

  /**
   * Converts the given value to the "internal" representation for the given
   * data type.
   */
  public static Object toInternalValue(DataType dataType, Object value)
    throws IOException
  {
    if(value == null) {
      return null;
    }

    switch(dataType) {
    case BOOLEAN:
      return ((value instanceof Boolean) ? value : toBooleanValue(value));
    case BYTE:
      return ((value instanceof Byte) ? value : toNumber(value).byteValue());
    case INT:
      return ((value instanceof Short) ? value : 
              toNumber(value).shortValue());
    case LONG:
      return ((value instanceof Integer) ? value : 
              toNumber(value).intValue());
    case MONEY:
      return toBigDecimal(value);
    case FLOAT:
      return ((value instanceof Float) ? value : 
              toNumber(value).floatValue());
    case DOUBLE:
      return ((value instanceof Double) ? value : 
              toNumber(value).doubleValue());
    case SHORT_DATE_TIME:
      return ((value instanceof DateExt) ? value :
              new Date(toDateLong(value)));
    case TEXT:
    case MEMO:
    case GUID:
      return ((value instanceof String) ? value : 
              toCharSequence(value).toString());
    case NUMERIC:
      return toBigDecimal(value);
    case COMPLEX_TYPE:
      // leave alone for now?
      return value;
    default:
      // some variation of binary data
      return toByteArray(value);
    }
  }

  String withErrorContext(String msg) {
    return withErrorContext(msg, getDatabase(), getTable().getName(), getName());
  }

  private static String withErrorContext(
      String msg, DatabaseImpl db, String tableName, String colName) {
    return msg + " (Db=" + db.getName() + ";Table=" + tableName + ";Column=" + 
      colName + ")";
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
      return CustomToStringStyle.valueBuilder(this)
        .append(null, getBytes())
        .toString();
    }

    private Object writeReplace() throws ObjectStreamException {
      // if we are going to serialize this, convert it back to a normal
      // byte[] (in case it is restored outside of the context of jackcess)
      return getBytes();
    }
  }

  /**
   * Base class for the supported autonumber types.
   * @usage _advanced_class_
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
    public abstract Object getNext(Object prevRowValue);

    /**
     * Restores a previous autonumber generated by this generator.
     */
    public abstract void restoreLast(Object last);
    
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
    public Object getNext(Object prevRowValue) {
      // the table stores the last long autonumber used
      return getTable().getNextLongAutoNumber();
    }

    @Override
    public void restoreLast(Object last) {
      if(last instanceof Integer) {
        getTable().restoreLastLongAutoNumber((Integer)last);
      }
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
    public Object getNext(Object prevRowValue) {
      // format guids consistently w/ Column.readGUIDValue()
      _lastAutoNumber = "{" + UUID.randomUUID() + "}";
      return _lastAutoNumber;
    }

    @Override
    public void restoreLast(Object last) {
      _lastAutoNumber = null;
    }
    
    @Override
    public DataType getType() {
      return DataType.GUID;
    }
  }

  private final class ComplexTypeAutoNumberGenerator extends AutoNumberGenerator
  {
    private ComplexTypeAutoNumberGenerator() {}

    @Override
    public Object getLast() {
      // the table stores the last ComplexType autonumber used
      return getTable().getLastComplexTypeAutoNumber();
    }

    @Override
    public Object getNext(Object prevRowValue) {
      int nextComplexAutoNum =
        ((prevRowValue == null) ?
         // the table stores the last ComplexType autonumber used
         getTable().getNextComplexTypeAutoNumber() :
         // same value is shared across all ComplexType values in a row
         ((ComplexValueForeignKey)prevRowValue).get());
      return new ComplexValueForeignKeyImpl(ColumnImpl.this, nextComplexAutoNum);
    }

    @Override
    public void restoreLast(Object last) {
      if(last instanceof ComplexValueForeignKey) {
        getTable().restoreLastComplexTypeAutoNumber(
            ((ComplexValueForeignKey)last).get());
      }
    }
    
    @Override
    public DataType getType() {
      return DataType.COMPLEX_TYPE;
    }
  }
  
  private final class UnsupportedAutoNumberGenerator extends AutoNumberGenerator
  {
    private final DataType _genType;
    
    private UnsupportedAutoNumberGenerator(DataType genType) {
      _genType = genType;
    }
    
    @Override
    public Object getLast() {
      return null;
    }

    @Override
    public Object getNext(Object prevRowValue) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void restoreLast(Object last) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public DataType getType() {
      return _genType;
    }
  }

  
  /**
   * Information about the sort order (collation) for a textual column.
   * @usage _intermediate_class_
   */
  public static final class SortOrder
  {
    private final short _value;
    private final byte _version;
    
    public SortOrder(short value, byte version) {
      _value = value;
      _version = version;
    }

    public short getValue() {
      return _value;
    }

    public byte getVersion() {
      return _version;
    }

    @Override
    public int hashCode() {
      return _value;
    }

    @Override
    public boolean equals(Object o) {
      return ((this == o) ||
              ((o != null) && (getClass() == o.getClass()) &&
               (_value == ((SortOrder)o)._value) &&
               (_version == ((SortOrder)o)._version)));
    }

    @Override
    public String toString() {
      return CustomToStringStyle.valueBuilder(this)
        .append(null, _value + "(" + _version + ")")
        .toString();
    }
  }

  /**
   * Utility struct for passing params through ColumnImpl constructors.
   */
  static final class InitArgs
  {
    public final TableImpl table;
    public final ByteBuffer buffer;
    public final int offset;
    public final String name;
    public final int displayIndex;
    public final byte colType;
    public final byte flags;
    public final byte extFlags;
    public DataType type;

    InitArgs(TableImpl table, ByteBuffer buffer, int offset, String name,
             int displayIndex) {
      this.table = table;
      this.buffer = buffer;
      this.offset = offset;
      this.name = name;
      this.displayIndex = displayIndex;
      
      this.colType = buffer.get(offset + table.getFormat().OFFSET_COLUMN_TYPE);
      this.flags = buffer.get(offset + table.getFormat().OFFSET_COLUMN_FLAGS);
      this.extFlags = readExtraFlags(buffer, offset, table.getFormat());
    }
  }
}
