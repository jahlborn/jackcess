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
import com.healthmarketscience.jackcess.impl.complex.ComplexColumnInfoImpl;
import com.healthmarketscience.jackcess.impl.complex.ComplexValueForeignKeyImpl;
import com.healthmarketscience.jackcess.impl.scsu.Compress;
import com.healthmarketscience.jackcess.impl.scsu.EndOfInputException;
import com.healthmarketscience.jackcess.impl.scsu.Expand;
import com.healthmarketscience.jackcess.impl.scsu.IllegalInputException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Access database column definition
 * @author Tim McCune
 * @usage _general_class_
 */
public class ColumnImpl implements Column, Comparable<ColumnImpl> {
  
  private static final Log LOG = LogFactory.getLog(ColumnImpl.class);
  
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
   * Long value (LVAL) type that indicates that the value is stored on the
   * same page
   */
  private static final byte LONG_VALUE_TYPE_THIS_PAGE = (byte) 0x80;
  /**
   * Long value (LVAL) type that indicates that the value is stored on another
   * page
   */
  private static final byte LONG_VALUE_TYPE_OTHER_PAGE = (byte) 0x40;
  /**
   * Long value (LVAL) type that indicates that the value is stored on
   * multiple other pages
   */
  private static final byte LONG_VALUE_TYPE_OTHER_PAGES = (byte) 0x00;
  /**
   * Mask to apply the long length in order to get the flag bits (only the
   * first 2 bits are type flags).
   */
  private static final int LONG_VALUE_TYPE_MASK = 0xC0000000;

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
  // 0x80: hyperlink (some memo based thing)

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

  /** placeholder for column which is not numeric */
  private static final NumericInfo DEFAULT_NUMERIC_INFO = new NumericInfo();

  /** placeholder for column which is not textual */
  private static final TextInfo DEFAULT_TEXT_INFO = new TextInfo();

  
  /** owning table */
  private final TableImpl _table;
  /** Whether or not the column is of variable length */
  private final boolean _variableLength;
  /** Whether or not the column is an autonumber column */
  private final boolean _autoNumber;
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
  private String _name;
  /** the offset of the fixed data in the row */
  private final int _fixedDataOffset;
  /** the index of the variable length data in the var len offset table */
  private final int _varLenTableIndex;
  /** information specific to numeric columns */
  private NumericInfo _numericInfo = DEFAULT_NUMERIC_INFO;
  /** information specific to text columns */
  private TextInfo _textInfo = DEFAULT_TEXT_INFO;
  /** the auto number generator for this column (if autonumber column) */
  private final AutoNumberGenerator _autoNumberGenerator;
  /** additional information specific to complex columns */
  private final ComplexColumnInfo<? extends ComplexValue> _complexInfo;
  /** properties for this column, if any */
  private PropertyMap _props;  
  /** Holds additional info for writing long values */
  private LongValueBufferHolder _lvalBufferH;
  
  /**
   * @usage _advanced_method_
   */
  protected ColumnImpl(TableImpl table, DataType type, int colNumber,
                       int fixedOffset, int varLenIndex) {
    _table = table;
    _type = type;

    if(!_type.isVariableLength()) {
      _columnLength = (short)type.getFixedSize();
    } else {
      _columnLength = (short)type.getMaxSize();
    }
    _variableLength = type.isVariableLength();
    if(type.getHasScalePrecision()) {
      modifyNumericInfo();
      _numericInfo._scale = (byte)type.getDefaultScale();
      _numericInfo._precision =(byte)type.getDefaultPrecision();
    }
    _autoNumber = false;
    _autoNumberGenerator = null;
    _columnNumber = (short)colNumber;
    _columnIndex = colNumber;
    _displayIndex = colNumber;
    _fixedDataOffset = fixedOffset;
    _varLenTableIndex = varLenIndex;
    _complexInfo = null;
  }
    
  /**
   * Read a column definition in from a buffer
   * @param table owning table
   * @param buffer Buffer containing column definition
   * @param offset Offset in the buffer at which the column definition starts
   * @usage _advanced_method_
   */
  public ColumnImpl(TableImpl table, ByteBuffer buffer, int offset, 
                    int displayIndex)
    throws IOException
  {
    _table = table;
    _displayIndex = displayIndex;
    
    byte colType = buffer.get(offset + getFormat().OFFSET_COLUMN_TYPE);
    _columnNumber = buffer.getShort(offset + getFormat().OFFSET_COLUMN_NUMBER);
    _columnLength = buffer.getShort(offset + getFormat().OFFSET_COLUMN_LENGTH);
    
    byte flags = buffer.get(offset + getFormat().OFFSET_COLUMN_FLAGS);
    _variableLength = ((flags & FIXED_LEN_FLAG_MASK) == 0);
    _autoNumber = ((flags & (AUTO_NUMBER_FLAG_MASK | AUTO_NUMBER_GUID_FLAG_MASK))
                   != 0);

    DataType type = null;
    try {
      type = DataType.fromByte(colType);
    } catch(IOException e) {
      LOG.warn("Unsupported column type " + colType);
      type = (_variableLength ? DataType.UNSUPPORTED_VARLEN :
              DataType.UNSUPPORTED_FIXEDLEN);
      setUnknownDataType(colType);
    }
    _type = type;
    
    if (_type.getHasScalePrecision()) {
      modifyNumericInfo();
      _numericInfo._precision = buffer.get(offset +
                                           getFormat().OFFSET_COLUMN_PRECISION);
      _numericInfo._scale = buffer.get(offset + getFormat().OFFSET_COLUMN_SCALE);
    } else if(_type.isTextual()) {
      modifyTextInfo();

      // co-located w/ precision/scale
      _textInfo._sortOrder = readSortOrder(
          buffer, offset + getFormat().OFFSET_COLUMN_SORT_ORDER, getFormat());
      int cpOffset = getFormat().OFFSET_COLUMN_CODE_PAGE;
      if(cpOffset >= 0) {
        _textInfo._codePage = buffer.getShort(offset + cpOffset);
      }

      _textInfo._compressedUnicode = ((buffer.get(offset +
        getFormat().OFFSET_COLUMN_COMPRESSED_UNICODE) & 1) == 1);

      if(_type == DataType.MEMO) {
        // only memo fields can be hyperlinks
        _textInfo._hyperlink = ((flags & HYPERLINK_FLAG_MASK) != 0);
      }
    }
    
    _autoNumberGenerator = createAutoNumberGenerator();
    
    if(_variableLength) {
      _varLenTableIndex = buffer.getShort(offset + getFormat().OFFSET_COLUMN_VARIABLE_TABLE_INDEX);
      _fixedDataOffset = 0;
    } else {
      _fixedDataOffset = buffer.getShort(offset + getFormat().OFFSET_COLUMN_FIXED_DATA_OFFSET);
      _varLenTableIndex = 0;
    }

    // load complex info
    if(_type == DataType.COMPLEX_TYPE) {
      _complexInfo = ComplexColumnSupport.create(this, buffer, offset);
    } else {
      _complexInfo = null;
    } 
  }

   /**
   * Sets the usage maps for this column.
   */
  void setUsageMaps(UsageMap ownedPages, UsageMap freeSpacePages) {
    _lvalBufferH = new UmapLongValueBufferHolder(ownedPages, freeSpacePages);
  }

  /**
   * Secondary column initialization after the table is fully loaded.
   */
  void postTableLoadInit() throws IOException {
    if(getType().isLongValue() && (_lvalBufferH == null)) {
      _lvalBufferH = new LegacyLongValueBufferHolder();
    }
    if(_complexInfo != null) {
      ((ComplexColumnInfoImpl<? extends ComplexValue>)_complexInfo)
      .postTableLoadInit();
    }
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

  /**
   * @usage _advanced_method_
   */
  public void setName(String name) {
    _name = name;
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
    return _textInfo._compressedUnicode;
  }

  public byte getPrecision() {
    return _numericInfo._precision;
  }
  
  public byte getScale() {
    return _numericInfo._scale;
  }

  /**
   * @usage _intermediate_method_
   */
  public SortOrder getTextSortOrder() {
    return _textInfo._sortOrder;
  }

  /**
   * @usage _intermediate_method_
   */
  public short getTextCodePage() {
    return _textInfo._codePage;
  }

  public short getLength() {
    return _columnLength;
  }

  public short getLengthInUnits() {
    return (short)getType().toUnitSize(getLength());
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
    return _textInfo._versionHistoryCol;
  }

   /**
   * Returns the number of database pages owned by this column.
   * @usage _intermediate_method_
   */
  public int getOwnedPageCount() {
    return ((_lvalBufferH == null) ? 0 : _lvalBufferH.getOwnedPageCount());
  }

  /**
   * @usage _advanced_method_
   */
  public void setVersionHistoryColumn(ColumnImpl versionHistoryCol) {
    modifyTextInfo();
    _textInfo._versionHistoryCol = versionHistoryCol;
  }

  public boolean isHyperlink() {
    return _textInfo._hyperlink;
  }
  
  public ComplexColumnInfo<? extends ComplexValue> getComplexInfo() {
    return _complexInfo;
  }
  
  private void setUnknownDataType(byte type) {
    // slight hack, stash the original type in the _scale
    modifyNumericInfo();
    _numericInfo._scale = type;
  }

  private byte getUnknownDataType() {
    // slight hack, we stashed the real type in the _scale
    return _numericInfo._scale;
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
      LOG.warn("Unknown auto number column type " + _type);
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

  private void modifyNumericInfo() {
    if(_numericInfo == DEFAULT_NUMERIC_INFO) {
      _numericInfo = new NumericInfo();
    }
  }
  
  private void modifyTextInfo() {
    if(_textInfo == DEFAULT_TEXT_INFO) {
      _textInfo = new TextInfo();
    }
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
    } else if (_type == DataType.COMPLEX_TYPE) {
      return new ComplexValueForeignKeyImpl(this, buffer.getInt());
    } else if(_type.isUnsupported()) {
      return rawDataWrapper(data);
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
    ByteBuffer def = PageChannel.wrap(lvalDefinition);
    int lengthWithFlags = def.getInt();
    int length = lengthWithFlags & (~LONG_VALUE_TYPE_MASK);

    byte[] rtn = new byte[length];
    byte type = (byte)((lengthWithFlags & LONG_VALUE_TYPE_MASK) >>> 24);

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

          short rowStart = TableImpl.findRowStart(lvalPage, rowNum, getFormat());
          short rowEnd = TableImpl.findRowEnd(lvalPage, rowNum, getFormat());

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

          short rowStart = TableImpl.findRowStart(lvalPage, rowNum, getFormat());
          short rowEnd = TableImpl.findRowEnd(lvalPage, rowNum, getFormat());
          
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
  private static BigDecimal readCurrencyValue(ByteBuffer buffer)
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
  private static void writeCurrencyValue(ByteBuffer buffer, Object value)
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
        new IOException("Currency value '" + inValue + "' out of range")
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
    Object inValue = value;
    try {
      BigDecimal decVal = toBigDecimal(value);
      inValue = decVal;

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
        new IOException("Numeric value '" + inValue + "' out of range")
        .initCause(e);
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
      long time = ((value instanceof Date) ?
                   ((Date)value).getTime() :
                   ((value instanceof Calendar) ?
                    ((Calendar)value).getTimeInMillis() :
                    ((Number)value).longValue()));
      time += getToLocalTimeZoneOffset(time);
      time += MILLIS_BETWEEN_EPOCH_AND_1900;
      return time / MILLISECONDS_PER_DAY;
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
  private static void writeGUIDValue(ByteBuffer buffer, Object value, 
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
   * @usage _advanced_method_
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
    // take length and apply type to first byte
    int lengthWithFlags = value.length | (type << 24);
    def.putInt(lengthWithFlags);

    if(type == LONG_VALUE_TYPE_THIS_PAGE) {
      // write long value inline
      def.putInt(0);
      def.putInt(0);  //Unknown
      def.put(value);
    } else {
      
      ByteBuffer lvalPage = null;
      int firstLvalPageNum = PageChannel.INVALID_PAGE_NUMBER;
      byte firstLvalRow = 0;

      // write other page(s)
      switch(type) {
      case LONG_VALUE_TYPE_OTHER_PAGE:
        lvalPage = _lvalBufferH.getLongValuePage(value.length);
        firstLvalPageNum = _lvalBufferH.getPageNumber();
        firstLvalRow = (byte)TableImpl.addDataPageRow(lvalPage, value.length,
                                                  getFormat(), 0);
        lvalPage.put(value);
        getPageChannel().writePage(lvalPage, firstLvalPageNum);
        break;

      case LONG_VALUE_TYPE_OTHER_PAGES:

        ByteBuffer buffer = ByteBuffer.wrap(value);
        int remainingLen = buffer.remaining();
        buffer.limit(0);
        lvalPage = _lvalBufferH.getLongValuePage(remainingLen);
        firstLvalPageNum = _lvalBufferH.getPageNumber();
        firstLvalRow = (byte)TableImpl.getRowsOnDataPage(lvalPage, getFormat());
        int lvalPageNum = firstLvalPageNum;
        ByteBuffer nextLvalPage = null;
        int nextLvalPageNum = 0;
        int nextLvalRowNum = 0;
        while(remainingLen > 0) {
          lvalPage.clear();

          // figure out how much we will put in this page (we need 4 bytes for
          // the next page pointer)
          int chunkLength = Math.min(getFormat().MAX_LONG_VALUE_ROW_SIZE - 4,
                                     remainingLen);

          // figure out if we will need another page, and if so, allocate it
          if(chunkLength < remainingLen) {
            // force a new page to be allocated for the chunk after this
            _lvalBufferH.clear();
            nextLvalPage = _lvalBufferH.getLongValuePage(
                (remainingLen - chunkLength) + 4);
            nextLvalPageNum = _lvalBufferH.getPageNumber();
            nextLvalRowNum = TableImpl.getRowsOnDataPage(nextLvalPage, 
                                                         getFormat());
          } else {
            nextLvalPage = null;
            nextLvalPageNum = 0;
            nextLvalRowNum = 0;
          }

          // add row to this page
          byte lvalRow = (byte)TableImpl.addDataPageRow(lvalPage, chunkLength + 4,
                                                        getFormat(), 0);
          
          // write next page info
          lvalPage.put((byte)nextLvalRowNum); // row number
          ByteUtil.put3ByteInt(lvalPage, nextLvalPageNum); // page number

          // write this page's chunk of data
          buffer.limit(buffer.limit() + chunkLength);
          lvalPage.put(buffer);
          remainingLen -= chunkLength;

          // write new page to database
          getPageChannel().writePage(lvalPage, lvalPageNum);

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
    lvalPage.putShort((short)getFormat().DATA_PAGE_INITIAL_FREE_SPACE); //Free space
    lvalPage.put((byte) 'L');
    lvalPage.put((byte) 'V');
    lvalPage.put((byte) 'A');
    lvalPage.put((byte) 'L');
    lvalPage.putInt(0); //unknown
    lvalPage.putShort((short)0); // num rows in page
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

    if(!isVariableLength() || !getType().isVariableLength()) {
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
      case UNSUPPORTED_VARLEN:
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
   * @usage _advanced_method_
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
    case COMPLEX_TYPE:
      buffer.putInt(toNumber(obj).intValue());
      break;
    case UNSUPPORTED_FIXEDLEN:
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
      byte[] tmpData = ByteUtil.copyOf(data, dataStart, dataLength);
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
    if(!forceUncompressed && isCompressedUnicode() &&
       (text.length() <= getFormat().MAX_COMPRESSED_UNICODE_SIZE)) {

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

  /**
   * Constructs a byte containing the flags for this column.
   */
  private static byte getColumnBitFlags(ColumnBuilder col) {
    byte flags = UNKNOWN_FLAG_MASK;
    if(!col.getType().isVariableLength()) {
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
    StringBuilder rtn = new StringBuilder();
    rtn.append("\tName: (" + _table.getName() + ") " + _name);
    byte typeValue = _type.getValue();
    if(_type.isUnsupported()) {
      typeValue = getUnknownDataType();
    }
    rtn.append("\n\tType: 0x" + Integer.toHexString(typeValue) +
               " (" + _type + ")");
    rtn.append("\n\tNumber: " + _columnNumber);
    rtn.append("\n\tLength: " + _columnLength);
    rtn.append("\n\tVariable length: " + _variableLength);
    if(_type.isTextual()) {
      rtn.append("\n\tCompressed Unicode: " + _textInfo._compressedUnicode);
      rtn.append("\n\tText Sort order: " + _textInfo._sortOrder);
      if(_textInfo._codePage > 0) {
        rtn.append("\n\tText Code Page: " + _textInfo._codePage);
      }
      if(isAppendOnly()) {
        rtn.append("\n\tAppend only: " + isAppendOnly());
      } 
      if(isHyperlink()) {
        rtn.append("\n\tHyperlink: " + isHyperlink());
      } 
    }      
    if(_autoNumber) {
      rtn.append("\n\tLast AutoNumber: " + _autoNumberGenerator.getLast());
    }
    if(_complexInfo != null) {
      rtn.append("\n\tComplexInfo: " + _complexInfo);
    }
    rtn.append("\n\n");
    return rtn.toString();
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
      if (col.getType().isVariableLength()) {
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
  public static short countNonLongVariableLength(List<ColumnBuilder> columns) {
    short rtn = 0;
    for (ColumnBuilder col : columns) {
      if (col.getType().isVariableLength() && !col.getType().isLongValue()) {
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
   * @usage _advanced_method_
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
  static RawData rawDataWrapper(byte[] bytes) {
    return new RawData(bytes);
  }

  /**
   * Returs {@code true} if the given value is "raw" column data,
   * {@code false} otherwise.
   * @usage _advanced_method_
   */
  public static boolean isRawData(Object value) {
    return(value instanceof RawData);
  }

  /**
   * Writes the column definitions into a table definition buffer.
   * @param buffer Buffer to write to
   * @param columns List of Columns to write definitions for
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
      if (col.getType().isVariableLength()) {
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
        if(col.getType().getHasScalePrecision()) {
          buffer.put(col.getPrecision());  // numeric precision
          buffer.put(col.getScale());  // numeric scale
        } else {
          buffer.put((byte) 0x00); //unused
          buffer.put((byte) 0x00); //unused
        }
        buffer.putShort((short) 0); //Unknown
      }
      buffer.put(getColumnBitFlags(col)); // misc col flags
      if (col.isCompressedUnicode()) {  //Compressed
        buffer.put((byte) 1);
      } else {
        buffer.put((byte) 0);
      }
      buffer.putInt(0); //Unknown, but always 0.
      //Offset for fixed length columns
      if (col.getType().isVariableLength()) {
        buffer.putShort((short) 0);
      } else {
        buffer.putShort(fixedOffset);
        fixedOffset += col.getType().getFixedSize(col.getLength());
      }
      if(!col.getType().isLongValue()) {
        buffer.putShort(col.getLength()); //Column length
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
      return _value + "(" + _version + ")";
    }
  }

  /**
   * Information specific to numeric types.
   */
  private static final class NumericInfo
  {
    /** Numeric precision */
    private byte _precision;
    /** Numeric scale */
    private byte _scale;
  }

  /**
   * Information specific to textual types.
   */
  private static final class TextInfo
  {
    /** whether or not they are compressed */ 
    private boolean _compressedUnicode;
    /** the collating sort order for a text field */
    private SortOrder _sortOrder;
    /** the code page for a text field (for certain db versions) */
    private short _codePage;
    /** complex column which tracks the version history for this "append only"
        column */
    private ColumnImpl _versionHistoryCol;
    /** whether or not this is a hyperlink column (only possible for columns
        of type MEMO) */
    private boolean _hyperlink;
  }

  /**
   * Manages secondary page buffers for long value writing.
   */
  private abstract class LongValueBufferHolder
  {
    /**
     * Returns a long value data page with space for data of the given length.
     */
    public ByteBuffer getLongValuePage(int dataLength) throws IOException {

      TempPageHolder lvalBufferH = getBufferHolder();
      dataLength = Math.min(dataLength, getFormat().MAX_LONG_VALUE_ROW_SIZE);

      ByteBuffer lvalPage = null;
      if(lvalBufferH.getPageNumber() != PageChannel.INVALID_PAGE_NUMBER) {
        lvalPage = lvalBufferH.getPage(getPageChannel());
        if(TableImpl.rowFitsOnDataPage(dataLength, lvalPage, getFormat())) {
          // the current page has space
          return lvalPage;
}
      }

      // need new page
      return findNewPage(dataLength);
    }

    protected ByteBuffer findNewPage(int dataLength) throws IOException {
      ByteBuffer lvalPage = getBufferHolder().setNewPage(getPageChannel());
      writeLongValueHeader(lvalPage);
      return lvalPage;
    }

    public int getOwnedPageCount() {
      return 0;
    }

    /**
     * Returns the page number of the current long value data page.
     */
    public int getPageNumber() {
      return getBufferHolder().getPageNumber();
    }

    /**
     * Discards the current the current long value data page.
     */
    public void clear() throws IOException {
      getBufferHolder().clear();
    }

    protected abstract TempPageHolder getBufferHolder();
  }

  /**
   * Manages a common, shared extra page for long values.  This is legacy
   * behavior from before it was understood that there were additional usage
   * maps for each columns.
   */
  private final class LegacyLongValueBufferHolder extends LongValueBufferHolder
  {
    @Override
    protected TempPageHolder getBufferHolder() {
      return getTable().getLongValueBuffer();
    }
  }

  /**
   * Manages the column usage maps for long values.
   */
  private final class UmapLongValueBufferHolder extends LongValueBufferHolder
  {
    /** Usage map of pages that this column owns */
    private final UsageMap _ownedPages;
    /** Usage map of pages that this column owns with free space on them */
    private final UsageMap _freeSpacePages;
    /** page buffer used to write "long value" data */
    private final TempPageHolder _longValueBufferH =
      TempPageHolder.newHolder(TempBufferHolder.Type.SOFT);

    private UmapLongValueBufferHolder(UsageMap ownedPages,
                                      UsageMap freeSpacePages) {
      _ownedPages = ownedPages;
      _freeSpacePages = freeSpacePages;
    }

    @Override
    protected TempPageHolder getBufferHolder() {
      return _longValueBufferH;
    }

    @Override
    public int getOwnedPageCount() {
      return _ownedPages.getPageCount();
    }

    @Override
    protected ByteBuffer findNewPage(int dataLength) throws IOException {

      // grab last owned page and check for free space.  
      ByteBuffer newPage = TableImpl.findFreeRowSpace(      
          _ownedPages, _freeSpacePages, _longValueBufferH);
      
      if(newPage != null) {
        if(TableImpl.rowFitsOnDataPage(dataLength, newPage, getFormat())) {
          return newPage;
        }
        // discard this page and allocate a new one
        clear();
      }

      // nothing found on current pages, need new page
      newPage = super.findNewPage(dataLength);
      int pageNumber = getPageNumber();
      _ownedPages.addPageNumber(pageNumber);
      _freeSpacePages.addPageNumber(pageNumber);
      return newPage;
    }

    @Override
    public void clear() throws IOException {
      int pageNumber = getPageNumber();
      if(pageNumber != PageChannel.INVALID_PAGE_NUMBER) {
        _freeSpacePages.removePageNumber(pageNumber, true);
      }
      super.clear();
    }
  }
}