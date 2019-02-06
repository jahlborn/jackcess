/*
Copyright (c) 2005 Health Market Science, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.InvalidValueException;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.expr.Identifier;
import com.healthmarketscience.jackcess.impl.complex.ComplexValueForeignKeyImpl;
import com.healthmarketscience.jackcess.impl.expr.NumberFormatter;
import com.healthmarketscience.jackcess.util.ColumnValidator;
import com.healthmarketscience.jackcess.util.SimpleColumnValidator;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Access database column definition
 * @author Tim McCune
 * @usage _intermediate_class_
 */
public class ColumnImpl implements Column, Comparable<ColumnImpl>, DateTimeContext
{

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
  private static final long MILLISECONDS_PER_DAY = (24L * 60L * 60L * 1000L);
  private static final long SECONDS_PER_DAY = (24L * 60L * 60L);
  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final long NANOS_PER_MILLI = 1_000_000L;
  private static final long MILLIS_PER_SECOND = 1000L;

  /**
   * Access starts counting dates at Dec 30, 1899 (note, this strange date
   * seems to be caused by MS compatibility with Lotus-1-2-3 and incorrect
   * leap years).  Java starts counting at Jan 1, 1970.  This is the # of
   * millis between them for conversion.
   */
  static final long MILLIS_BETWEEN_EPOCH_AND_1900 =
    25569L * MILLISECONDS_PER_DAY;

  public static final LocalDate BASE_LD = LocalDate.of(1899, 12, 30);
  public static final LocalTime BASE_LT = LocalTime.of(0, 0);
  public static final LocalDateTime BASE_LDT = LocalDateTime.of(BASE_LD, BASE_LT);

  private static final DateTimeFactory DEF_DATE_TIME_FACTORY =
    new DefaultDateTimeFactory();

  static final DateTimeFactory LDT_DATE_TIME_FACTORY =
    new LDTDateTimeFactory();

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
   * mask for the "is updatable" field bit
   * @usage _advanced_field_
   */
  public static final byte UPDATABLE_FLAG_MASK = (byte)0x02;

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

  /** auto numbers must be > 0 */
  static final int INVALID_AUTO_NUMBER = 0;

  static final int INVALID_LENGTH = -1;


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
  /** default value generator */
  private ColDefaultValueEvalContext _defValue;
  /** length of the column in units, lazily computed */
  private int _lengthInUnits = INVALID_LENGTH;

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

  void collectUsageMapPages(Collection<Integer> pages) {
    // base does nothing
  }

  /**
   * Secondary column initialization after the table is fully loaded.
   */
  void postTableLoadInit() throws IOException {
    // base does nothing
  }

  @Override
  public TableImpl getTable() {
    return _table;
  }

  @Override
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

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public boolean isVariableLength() {
    return _variableLength;
  }

  @Override
  public boolean isAutoNumber() {
    return _autoNumber;
  }

  /**
   * @usage _advanced_method_
   */
  public short getColumnNumber() {
    return _columnNumber;
  }

  @Override
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

  @Override
  public DataType getType() {
    return _type;
  }

  @Override
  public int getSQLType() throws SQLException {
    return _type.getSQLType();
  }

  @Override
  public boolean isCompressedUnicode() {
    return false;
  }

  @Override
  public byte getPrecision() {
    return (byte)getType().getDefaultPrecision();
  }

  @Override
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

  @Override
  public short getLength() {
    return _columnLength;
  }

  @Override
  public final short getLengthInUnits() {
    if(_lengthInUnits == INVALID_LENGTH) {
      _lengthInUnits = calcLengthInUnits();
    }
    return (short)_lengthInUnits;
  }

  protected int calcLengthInUnits() {
    return getType().toUnitSize(getLength(), getFormat());
  }

  @Override
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

  @Override
  public TimeZone getTimeZone() {
    return getDatabase().getTimeZone();
  }

  @Override
  public ZoneId getZoneId() {
    return getDatabase().getZoneId();
  }

  @Override
  public DateTimeFactory getDateTimeFactory() {
    return getDatabase().getDateTimeFactory();
  }

  @Override
  public boolean isAppendOnly() {
    return (getVersionHistoryColumn() != null);
  }

  @Override
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

  @Override
  public boolean isHyperlink() {
    return false;
  }

  @Override
  public ComplexColumnInfo<? extends ComplexValue> getComplexInfo() {
    return null;
  }

  void initColumnValidator() throws IOException {

    if(getDatabase().isReadOnly()) {
      // validators are irrelevant for read-only databases
      return;
    }

    // first initialize any "external" (user-defined) validator
    setColumnValidator(null);

    // next, initialize any "internal" (property defined) validators
    reloadPropertiesValidators();
  }

  void reloadPropertiesValidators() throws IOException {

    if(isAutoNumber()) {
      // none of the props stuff applies to autonumber columns
      return;
    }

    if(isCalculated()) {

      CalcColEvalContext calcCol = null;

      if(getDatabase().isEvaluateExpressions()) {

        // init calc col expression evaluator
        PropertyMap props = getProperties();
        String calcExpr = (String)props.getValue(PropertyMap.EXPRESSION_PROP);
        calcCol = new CalcColEvalContext(this).setExpr(calcExpr);
      }

      setCalcColEvalContext(calcCol);

      // none of the remaining props stuff applies to calculated columns
      return;
    }

    // discard any existing internal validators and re-compute them
    // (essentially unwrap the external validator)
    _validator = getColumnValidator();
    _defValue = null;

    PropertyMap props = getProperties();

    // if the "required" property is enabled, add appropriate validator
    boolean required = (Boolean)props.getValue(PropertyMap.REQUIRED_PROP,
                                               Boolean.FALSE);
    if(required) {
      _validator = new RequiredColValidator(_validator);
    }

    // if the "allow zero len" property is disabled (textual columns only),
    // add appropriate validator
    boolean allowZeroLen =
      !getType().isTextual() ||
      (Boolean)props.getValue(PropertyMap.ALLOW_ZERO_LEN_PROP,
                              Boolean.TRUE);
    if(!allowZeroLen) {
      _validator = new NoZeroLenColValidator(_validator);
    }

    // only check for props based exprs if this is enabled
    if(!getDatabase().isEvaluateExpressions()) {
      return;
    }

    String exprStr = PropertyMaps.getTrimmedStringProperty(
        props, PropertyMap.VALIDATION_RULE_PROP);

    if(exprStr != null) {
      String helpStr = PropertyMaps.getTrimmedStringProperty(
          props, PropertyMap.VALIDATION_TEXT_PROP);

      _validator = new ColValidatorEvalContext(this)
        .setExpr(exprStr, helpStr)
        .toColumnValidator(_validator);
    }

    String defValueStr = PropertyMaps.getTrimmedStringProperty(
        props, PropertyMap.DEFAULT_VALUE_PROP);
    if(defValueStr != null) {
      _defValue = new ColDefaultValueEvalContext(this)
        .setExpr(defValueStr);
    }
  }

  void propertiesUpdated() throws IOException {
    reloadPropertiesValidators();
  }

  @Override
  public ColumnValidator getColumnValidator() {
    // unwrap any "internal" validator
    return ((_validator instanceof InternalColumnValidator) ?
            ((InternalColumnValidator)_validator).getExternal() : _validator);
  }

  @Override
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

    // handle delegation if "internal" validator in use
    if(_validator instanceof InternalColumnValidator) {
      ((InternalColumnValidator)_validator).setExternal(newValidator);
    } else {
      _validator = newValidator;
    }
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

  @Override
  public PropertyMap getProperties() throws IOException {
    if(_props == null) {
      _props = getTable().getPropertyMaps().get(getName());
    }
    return _props;
  }

  @Override
  public Object setRowValue(Object[] rowArray, Object value) {
    rowArray[_columnIndex] = value;
    return value;
  }

  @Override
  public Object setRowValue(Map<String,Object> rowMap, Object value) {
    rowMap.put(_name, value);
    return value;
  }

  @Override
  public Object getRowValue(Object[] rowArray) {
    return rowArray[_columnIndex];
  }

  @Override
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
    case BIG_INT:
      return Long.valueOf(buffer.getLong());
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
        throw new InvalidValueException(withErrorContext(
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
        throw new InvalidValueException(withErrorContext(
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
  private Object readDateValue(ByteBuffer buffer) {
    long dateBits = buffer.getLong();
    return getDateTimeFactory().fromDateBits(this, dateBits);
  }

  /**
   * Returns a java long time value converted from an access date double.
   * @usage _advanced_method_
   */
  public long fromDateDouble(double value) {
    return fromDateDouble(value, getTimeZone());
  }

  private static long fromDateDouble(double value, TimeZone tz) {
    long localTime = fromLocalDateDouble(value);
    return localTime - getFromLocalTimeZoneOffset(localTime, tz);
  }

  static long fromLocalDateDouble(double value) {
    long datePart = ((long)value) * MILLISECONDS_PER_DAY;

    // the fractional part of the double represents the time.  it is always
    // a positive fraction of the day (even if the double is negative),
    // _not_ the time distance from zero (as one would expect with "normal"
    // numbers).  therefore, we need to do a little number logic to convert
    // the absolute time fraction into a normal distance from zero number.
    long timePart = Math.round((Math.abs(value) % 1.0d) *
                               MILLISECONDS_PER_DAY);

    long time = datePart + timePart;
    return time - MILLIS_BETWEEN_EPOCH_AND_1900;
  }

  public static LocalDateTime ldtFromLocalDateDouble(double value) {
    Duration dateTimeOffset = durationFromLocalDateDouble(value);
    return BASE_LDT.plus(dateTimeOffset);
  }

  private static Duration durationFromLocalDateDouble(double value) {
    long dateSeconds = ((long)value) * SECONDS_PER_DAY;

    // the fractional part of the double represents the time.  it is always
    // a positive fraction of the day (even if the double is negative),
    // _not_ the time distance from zero (as one would expect with "normal"
    // numbers).  therefore, we need to do a little number logic to convert
    // the absolute time fraction into a normal distance from zero number.

    double secondsDouble = (Math.abs(value) % 1.0d) * SECONDS_PER_DAY;
    long timeSeconds = (long)secondsDouble;
    long timeMillis = (long)(roundToMillis(secondsDouble % 1.0d) *
                             MILLIS_PER_SECOND);

    return Duration.ofSeconds(dateSeconds + timeSeconds,
                              timeMillis * NANOS_PER_MILLI);
  }

  /**
   * Writes a date value.
   */
  private void writeDateValue(ByteBuffer buffer, Object value)
    throws InvalidValueException
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
    throws InvalidValueException
  {
    try {
      return toDateDouble(value, this);
    } catch(IllegalArgumentException iae) {
      throw new InvalidValueException(withErrorContext(iae.getMessage()), iae);
    }
  }

  /**
   * Returns an access date double converted from a java
   * Date/Calendar/Number/Temporal time value.
   * @usage _advanced_method_
   */
  private static double toDateDouble(Object value, DateTimeContext dtc) {
    return dtc.getDateTimeFactory().toDateDouble(value, dtc);
  }

  private static LocalDateTime toLocalDateTime(
      TemporalAccessor value, DateTimeContext dtc) {

    // handle some common Temporal types
    if(value instanceof LocalDateTime) {
      return (LocalDateTime)value;
    }
    if(value instanceof ZonedDateTime) {
      // if the temporal value has a timezone, convert it to this db's timezone
      return ((ZonedDateTime)value).withZoneSameInstant(
          dtc.getZoneId()).toLocalDateTime();
    }
    if(value instanceof Instant) {
      return LocalDateTime.ofInstant((Instant)value, dtc.getZoneId());
    }
    if(value instanceof LocalDate) {
      return ((LocalDate)value).atTime(BASE_LT);
    }
    if(value instanceof LocalTime) {
      return ((LocalTime)value).atDate(BASE_LD);
    }

    // generic handling for many other Temporal types
    try {

      LocalDate ld = value.query(TemporalQueries.localDate());
      if(ld == null) {
        ld = BASE_LD;
      }
      LocalTime lt = value.query(TemporalQueries.localTime());
      if(lt == null) {
        lt = BASE_LT;
      }
      ZoneId zone = value.query(TemporalQueries.zone());
      if(zone != null) {
        // the Temporal has a zone, see if it is the right zone.  if not,
        // adjust it
        ZoneId zoneId = dtc.getZoneId();
        if(!zoneId.equals(zone)) {
          return ZonedDateTime.of(ld, lt, zone).withZoneSameInstant(zoneId)
            .toLocalDateTime();
        }
      }

      return LocalDateTime.of(ld, lt);

    } catch(DateTimeException | ArithmeticException e) {
      throw new IllegalArgumentException(
          "Unsupported temporal type " + value.getClass(), e);
    }
  }

  private static Instant toInstant(TemporalAccessor value, DateTimeContext dtc) {
    if(value instanceof ZonedDateTime) {
      return ((ZonedDateTime)value).toInstant();
    }
    if(value instanceof Instant) {
      return (Instant)value;
    }
    return toLocalDateTime(value, dtc).atZone(dtc.getZoneId()).toInstant();
  }

  static double toLocalDateDouble(long time) {
    time += MILLIS_BETWEEN_EPOCH_AND_1900;

    if(time < 0L) {
      // reverse the crazy math described in fromLocalDateDouble
      long timePart = -time % MILLISECONDS_PER_DAY;
      if(timePart > 0) {
        time -= (2 * (MILLISECONDS_PER_DAY - timePart));
      }
    }

    return time / (double)MILLISECONDS_PER_DAY;
  }

  public static double toDateDouble(LocalDateTime ldt) {
    Duration dateTimeOffset = Duration.between(BASE_LDT, ldt);
    return toLocalDateDouble(dateTimeOffset);
  }

  private static double toLocalDateDouble(Duration time) {
    long dateTimeSeconds = time.getSeconds();
    long timeSeconds = dateTimeSeconds % SECONDS_PER_DAY;
    if(timeSeconds < 0) {
      timeSeconds += SECONDS_PER_DAY;
    }
    long dateSeconds = dateTimeSeconds - timeSeconds;
    long timeNanos = time.getNano();

    // we have a difficult choice to make here between keeping a value which
    // most accurately represents the bits saved and rounding to a value that
    // would match what the user would expect too see.  since we do a double
    // to long conversion, we end up in a situation where the value might be
    // 19.9999 seconds.  access will display this as 20 seconds (access seems
    // to only record times to second precision).  if we return 19.9999, then
    // when the value is written back out it will be exactly the same double
    // (good), but will display as 19 seconds (bad because it looks wrong to
    // the user).  on the flip side, if we round, the value will display
    // "correctly" to the user, but if the value is written back out, it will
    // be a slightly different double value.  this may not be a problem for
    // most situations, but may result in incorrect index based lookups.  in
    // the old date time handling we use DateExt to store the original bits.
    // in jdk8, we cannot extend LocalDateTime.  for now, we will try
    // returning the value rounded to milliseconds (technically still more
    // precision than access uses but more likely to round trip to the same
    // value).
    double timeDouble = ((roundToMillis((double)timeNanos / NANOS_PER_SECOND) +
                          timeSeconds) / SECONDS_PER_DAY);

    double dateDouble = ((double)dateSeconds / SECONDS_PER_DAY);

    if(dateSeconds < 0) {
      timeDouble = -timeDouble;
    }

    return dateDouble + timeDouble;
  }

  /**
   * Rounds the given decimal to milliseconds (3 decimal places) using the
   * standard access rounding mode.
   */
  private static double roundToMillis(double dbl) {
    return ((dbl == 0d) ? dbl :
            new BigDecimal(dbl).setScale(3, NumberFormatter.ROUND_MODE)
            .doubleValue());
  }

  /**
   * @return an appropriate Date long value for the given object
   */
  private static long toDateLong(Object value) {
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
  private static long getToLocalTimeZoneOffset(long time, TimeZone tz) {
    return tz.getOffset(time);
  }

  /**
   * Gets the timezone offset from local time to UTC for the given time
   * (including DST).
   */
  private static long getFromLocalTimeZoneOffset(long time, TimeZone tz) {
    // getting from local time back to UTC is a little wonky (and not
    // guaranteed to get you back to where you started).  apply the zone
    // offset first to get us closer to the original time
    return tz.getOffset(time - tz.getRawOffset());
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
      throw new InvalidValueException(
          withErrorContext("Invalid GUID: " + value));
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
   * Returns a default value for this column
   */
  public Object generateDefaultValue() throws IOException {
    return ((_defValue != null) ? _defValue.eval() : null);
  }

  /**
   * Passes the given obj through the currently configured validator for this
   * column and returns the result.
   */
  public Object validate(Object obj) throws IOException {
    return _validator.validate(this, obj);
  }

  /**
   * Returns the context used to manage calculated column values.
   */
  protected CalcColEvalContext getCalculationContext() {
    throw new UnsupportedOperationException();
  }

  protected void setCalcColEvalContext(CalcColEvalContext calcCol) {
    throw new UnsupportedOperationException();
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
    case BIG_INT:
      buffer.putLong(toNumber(obj).longValue());
      break;
    case UNSUPPORTED_FIXEDLEN:
      byte[] bytes = toByteArray(obj);
      if(bytes.length != getLength()) {
        throw new InvalidValueException(withErrorContext(
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
      throw new InvalidValueException(withErrorContext(
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
    byte flags = UPDATABLE_FLAG_MASK;
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
      sb.append("calculated", _calculated)
        .append("expression",
                CustomToStringStyle.ignoreNull(getCalculationContext()));
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
    sb.append("complexInfo", CustomToStringStyle.ignoreNull(getComplexInfo()))
      .append("validator", CustomToStringStyle.ignoreNull(
                  ((_validator != SimpleColumnValidator.INSTANCE) ?
                   _validator : null)))
      .append("defaultValue", CustomToStringStyle.ignoreNull(_defValue));
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
  @Override
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
   * @return an appropriate BigDecimal representation of the given object.
   *         <code>null</code> is returned as 0 and Numbers are converted
   *         using their double representation.
   */
  BigDecimal toBigDecimal(Object value)
  {
    return toBigDecimal(value, getDatabase());
  }

  /**
   * @return an appropriate BigDecimal representation of the given object.
   *         <code>null</code> is returned as 0 and Numbers are converted
   *         using their double representation.
   */
  static BigDecimal toBigDecimal(Object value, DatabaseImpl db)
  {
    if(value == null) {
      return BigDecimal.ZERO;
    } else if(value instanceof BigDecimal) {
      return (BigDecimal)value;
    } else if(value instanceof BigInteger) {
      return new BigDecimal((BigInteger)value);
    } else if(value instanceof Number) {
      return new BigDecimal(((Number)value).doubleValue());
    } else if(value instanceof Boolean) {
      // access seems to like -1 for true and 0 for false
      return ((Boolean)value) ? BigDecimal.valueOf(-1) : BigDecimal.ZERO;
    } else if(value instanceof Date) {
      return new BigDecimal(toDateDouble(value, db));
    } else if(value instanceof LocalDateTime) {
      return new BigDecimal(toDateDouble((LocalDateTime)value));
    }
    return new BigDecimal(value.toString());
  }

  /**
   * @return an appropriate Number representation of the given object.
   *         <code>null</code> is returned as 0 and Strings are parsed as
   *         Doubles.
   */
  private Number toNumber(Object value)
  {
    return toNumber(value, getDatabase());
  }

  /**
   * @return an appropriate Number representation of the given object.
   *         <code>null</code> is returned as 0 and Strings are parsed as
   *         Doubles.
   */
  private static Number toNumber(Object value, DatabaseImpl db)
  {
    if(value == null) {
      return BigDecimal.ZERO;
    } else if(value instanceof Number) {
      return (Number)value;
    } else if(value instanceof Boolean) {
      // access seems to like -1 for true and 0 for false
      return ((Boolean)value) ? -1 : 0;
    } else if(value instanceof Date) {
      return toDateDouble(value, db);
    } else if(value instanceof LocalDateTime) {
      return toDateDouble((LocalDateTime)value);
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
    } else if(obj instanceof Number) {
      // Access considers 0 as "false"
      if(obj instanceof BigDecimal) {
        return (((BigDecimal)obj).compareTo(BigDecimal.ZERO) != 0);
      }
      if(obj instanceof BigInteger) {
        return (((BigInteger)obj).compareTo(BigInteger.ZERO) != 0);
      }
      return (((Number)obj).doubleValue() != 0.0d);
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
   * Treat booleans as integers (access-style).
   */
  protected static Object booleanToInteger(Object obj) {
    if (obj instanceof Boolean) {
      obj = ((Boolean) obj) ? -1 : 0;
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
    // we specifically put the "long variable" values after the normal
    // variable length values so that we have a better chance of fitting it
    // all (because "long variable" values can go in separate pages)
    int longVariableOffset = creator.countNonLongVariableLength();
    creator.setColumnOffsets(0, 0, longVariableOffset);

    for (ColumnBuilder col : creator.getColumns()) {
      writeDefinition(creator, col, buffer);
    }

    for (ColumnBuilder col : creator.getColumns()) {
      TableImpl.writeName(buffer, col.getName(), creator.getCharset());
    }
  }

  protected static void writeDefinition(
      TableMutator mutator, ColumnBuilder col, ByteBuffer buffer)
    throws IOException
  {
    TableMutator.ColumnOffsets colOffsets = mutator.getColumnOffsets();

    buffer.put(col.getType().getValue());
    buffer.putInt(TableImpl.MAGIC_TABLE_NUMBER);  //constant magic number
    buffer.putShort(col.getColumnNumber());  //Column Number

    if(col.isVariableLength()) {
      buffer.putShort(colOffsets.getNextVariableOffset(col));
    } else {
      buffer.putShort((short) 0);
    }

    buffer.putShort(col.getColumnNumber()); //Column Number again

    if(col.getType().isTextual()) {
      // this will write 4 bytes (note we don't support writing dbs which
      // use the text code page)
      writeSortOrder(buffer, col.getTextSortOrder(), mutator.getFormat());
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
      buffer.putShort(colOffsets.getNextFixedOffset(col));
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

  protected static void writeColUsageMapDefinitions(
      TableCreator creator, ByteBuffer buffer)
    throws IOException
  {
    // write long value column usage map references
    for(ColumnBuilder lvalCol : creator.getLongValueColumns()) {
      writeColUsageMapDefinition(creator, lvalCol, buffer);
    }
  }

  protected static void writeColUsageMapDefinition(
      TableMutator creator, ColumnBuilder lvalCol, ByteBuffer buffer)
    throws IOException
  {
    TableMutator.ColumnState colState = creator.getColumnState(lvalCol);

    buffer.putShort(lvalCol.getColumnNumber());

    // owned pages umap (both are on same page)
    buffer.put(colState.getUmapOwnedRowNumber());
    ByteUtil.put3ByteInt(buffer, colState.getUmapPageNumber());
    // free space pages umap
    buffer.put(colState.getUmapFreeRowNumber());
    ByteUtil.put3ByteInt(buffer, colState.getUmapPageNumber());
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
  public static Object toInternalValue(DataType dataType, Object value,
                                       DatabaseImpl db)
    throws IOException
  {
    return toInternalValue(dataType, value, db, null);
  }

  static Object toInternalValue(DataType dataType, Object value,
                                DatabaseImpl db,
                                ColumnImpl.DateTimeFactory factory)
    throws IOException
  {
    if(value == null) {
      return null;
    }

    switch(dataType) {
    case BOOLEAN:
      return ((value instanceof Boolean) ? value : toBooleanValue(value));
    case BYTE:
      return ((value instanceof Byte) ? value : toNumber(value, db).byteValue());
    case INT:
      return ((value instanceof Short) ? value :
              toNumber(value, db).shortValue());
    case LONG:
      return ((value instanceof Integer) ? value :
              toNumber(value, db).intValue());
    case MONEY:
      return toBigDecimal(value, db);
    case FLOAT:
      return ((value instanceof Float) ? value :
              toNumber(value, db).floatValue());
    case DOUBLE:
      return ((value instanceof Double) ? value :
              toNumber(value, db).doubleValue());
    case SHORT_DATE_TIME:
      if(factory == null) {
        factory = db.getDateTimeFactory();
      }
      return factory.toInternalValue(db, value);
    case TEXT:
    case MEMO:
    case GUID:
      return ((value instanceof String) ? value :
              toCharSequence(value).toString());
    case NUMERIC:
      return toBigDecimal(value, db);
    case COMPLEX_TYPE:
      // leave alone for now?
      return value;
    case BIG_INT:
      return ((value instanceof Long) ? value :
              toNumber(value, db).longValue());
    default:
      // some variation of binary data
      return toByteArray(value);
    }
  }

  protected static DateTimeFactory getDateTimeFactory(DateTimeType type) {
    return ((type == DateTimeType.LOCAL_DATE_TIME) ?
            LDT_DATE_TIME_FACTORY : DEF_DATE_TIME_FACTORY);
  }

  String withErrorContext(String msg) {
    return withErrorContext(msg, getDatabase(), getTable().getName(), getName());
  }

  boolean isThisColumn(Identifier identifier) {
    return(getTable().isThisTable(identifier) &&
           getName().equalsIgnoreCase(identifier.getObjectName()));
  }

  private static String withErrorContext(
      String msg, DatabaseImpl db, String tableName, String colName) {
    return msg + " (Db=" + db.getName() + ";Table=" + tableName + ";Column=" +
      colName + ")";
  }

  /**
   * Date subclass which stashes the original date bits, in case we attempt to
   * re-write the value (will not lose precision).  Also, this implementation
   * is immutable.
   */
  @SuppressWarnings("deprecation")
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

    @Override
    public void setDate(int time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setHours(int time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setMinutes(int time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setMonth(int time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setSeconds(int time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setYear(int time) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setTime(long time) {
      throw new UnsupportedOperationException();
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
    public abstract Object getNext(TableImpl.WriteRowState writeRowState);

    /**
     * Returns a valid autonumber for this generator.
     * <p>
     * <i>Warning, calling this externally may result in this value being
     * "lost" for the table.</i>
     */
    public abstract Object handleInsert(
        TableImpl.WriteRowState writeRowState, Object inRowValue)
      throws IOException;

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
    public Object getNext(TableImpl.WriteRowState writeRowState) {
      // the table stores the last long autonumber used
      return getTable().getNextLongAutoNumber();
    }

    @Override
    public Object handleInsert(TableImpl.WriteRowState writeRowState,
                               Object inRowValue)
      throws IOException
    {
      int inAutoNum = toNumber(inRowValue).intValue();
      if(inAutoNum <= INVALID_AUTO_NUMBER &&
         !getTable().isAllowAutoNumberInsert()) {
        throw new InvalidValueException(withErrorContext(
                "Invalid auto number value " + inAutoNum));
      }
      // the table stores the last long autonumber used
      getTable().adjustLongAutoNumber(inAutoNum);
      return inAutoNum;
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
    public Object getNext(TableImpl.WriteRowState writeRowState) {
      // format guids consistently w/ Column.readGUIDValue()
      _lastAutoNumber = "{" + UUID.randomUUID() + "}";
      return _lastAutoNumber;
    }

    @Override
    public Object handleInsert(TableImpl.WriteRowState writeRowState,
                               Object inRowValue)
      throws IOException
    {
      _lastAutoNumber = toCharSequence(inRowValue);
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
    public Object getNext(TableImpl.WriteRowState writeRowState) {
      // same value is shared across all ComplexType values in a row
      int nextComplexAutoNum = writeRowState.getComplexAutoNumber();
      if(nextComplexAutoNum <= INVALID_AUTO_NUMBER) {
        // the table stores the last ComplexType autonumber used
        nextComplexAutoNum = getTable().getNextComplexTypeAutoNumber();
        writeRowState.setComplexAutoNumber(nextComplexAutoNum);
      }
      return new ComplexValueForeignKeyImpl(ColumnImpl.this,
                                            nextComplexAutoNum);
    }

    @Override
    public Object handleInsert(TableImpl.WriteRowState writeRowState,
                               Object inRowValue)
      throws IOException
    {
      ComplexValueForeignKey inComplexFK = null;
      if(inRowValue instanceof ComplexValueForeignKey) {
        inComplexFK = (ComplexValueForeignKey)inRowValue;
      } else {
        inComplexFK = new ComplexValueForeignKeyImpl(
            ColumnImpl.this, toNumber(inRowValue).intValue());
      }

      if(inComplexFK.getColumn() != ColumnImpl.this) {
        throw new InvalidValueException(withErrorContext(
                "Wrong column for complex value foreign key, found " +
                inComplexFK.getColumn().getName()));
      }
      if(inComplexFK.get() < 1) {
        throw new InvalidValueException(withErrorContext(
                "Invalid complex value foreign key value " + inComplexFK.get()));
      }
      // same value is shared across all ComplexType values in a row
      int prevRowValue = writeRowState.getComplexAutoNumber();
      if(prevRowValue <= INVALID_AUTO_NUMBER) {
        writeRowState.setComplexAutoNumber(inComplexFK.get());
      } else if(prevRowValue != inComplexFK.get()) {
        throw new InvalidValueException(withErrorContext(
                "Inconsistent complex value foreign key values: found " +
                prevRowValue + ", given " + inComplexFK));
      }

      // the table stores the last ComplexType autonumber used
      getTable().adjustComplexTypeAutoNumber(inComplexFK.get());

      return inComplexFK;
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
    public Object getNext(TableImpl.WriteRowState writeRowState) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object handleInsert(TableImpl.WriteRowState writeRowState,
                               Object inRowValue) {
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

  /**
   * "Internal" column validator for columns with the "required" property
   * enabled.
   */
  private static final class RequiredColValidator extends InternalColumnValidator
  {
    private RequiredColValidator(ColumnValidator delegate) {
      super(delegate);
    }

    @Override
    protected Object internalValidate(Column col, Object val)
      throws IOException
    {
      if(val == null) {
        throw new InvalidValueException(
            ((ColumnImpl)col).withErrorContext(
                "Missing value for required column"));
      }
      return val;
    }

    @Override
    protected void appendToString(StringBuilder sb) {
      sb.append("required=true");
    }
  }

  /**
   * "Internal" column validator for text columns with the "allow zero len"
   * property disabled.
   */
  private static final class NoZeroLenColValidator extends InternalColumnValidator
  {
    private NoZeroLenColValidator(ColumnValidator delegate) {
      super(delegate);
    }

    @Override
    protected Object internalValidate(Column col, Object val)
      throws IOException
    {
      CharSequence valStr = ColumnImpl.toCharSequence(val);
      // oddly enough null is allowed for non-zero len strings
      if((valStr != null) && valStr.length() == 0) {
        throw new InvalidValueException(
            ((ColumnImpl)col).withErrorContext(
                "Zero length string is not allowed"));
      }
      return valStr;
    }

    @Override
    protected void appendToString(StringBuilder sb) {
      sb.append("allowZeroLength=false");
    }
  }

  /**
   * Factory which handles date/time values appropriately for a DateTimeType.
   */
  protected static abstract class DateTimeFactory
  {
    public abstract DateTimeType getType();

    public abstract Object fromDateBits(ColumnImpl col, long dateBits);

    public abstract double toDateDouble(Object value, DateTimeContext dtc);

    public abstract Object toInternalValue(DatabaseImpl db, Object value);
  }

  /**
   * Factory impl for legacy Date handling.
   */
  private static final class DefaultDateTimeFactory extends DateTimeFactory
  {
    @Override
    public DateTimeType getType() {
      return DateTimeType.DATE;
    }

    @Override
    public Object fromDateBits(ColumnImpl col, long dateBits) {
      long time = col.fromDateDouble(
          Double.longBitsToDouble(dateBits));
      return new DateExt(time, dateBits);
    }

    @Override
    public double toDateDouble(Object value, DateTimeContext dtc) {
      // ZoneId and TimeZone have different rules for older timezones, so we
      // need to consistently use one or the other depending on the date/time
      // type
      long time = 0L;
      if(value instanceof TemporalAccessor) {
        time = toInstant((TemporalAccessor)value, dtc).toEpochMilli();
      } else {
        time = toDateLong(value);
      }
      // seems access stores dates in the local timezone.  guess you just
      // hope you read it in the same timezone in which it was written!
      time += getToLocalTimeZoneOffset(time, dtc.getTimeZone());
      return toLocalDateDouble(time);
    }

    @Override
    public Object toInternalValue(DatabaseImpl db, Object value) {
      return ((value instanceof Date) ? value :
              new Date(toDateLong(value)));
    }
  }

  /**
   * Factory impl for LocalDateTime handling.
   */
  private static final class LDTDateTimeFactory extends DateTimeFactory
  {
    @Override
    public DateTimeType getType() {
      return DateTimeType.LOCAL_DATE_TIME;
    }

    @Override
    public Object fromDateBits(ColumnImpl col, long dateBits) {
      return ldtFromLocalDateDouble(Double.longBitsToDouble(dateBits));
    }

    @Override
    public double toDateDouble(Object value, DateTimeContext dtc) {
      // ZoneId and TimeZone have different rules for older timezones, so we
      // need to consistently use one or the other depending on the date/time
      // type
      if(!(value instanceof TemporalAccessor)) {
        value = Instant.ofEpochMilli(toDateLong(value));
      }
      return ColumnImpl.toDateDouble(
          toLocalDateTime((TemporalAccessor)value, dtc));
    }

    @Override
    public Object toInternalValue(DatabaseImpl db, Object value) {
      if(value instanceof TemporalAccessor) {
        return toLocalDateTime((TemporalAccessor)value, db);
      }
      Instant inst = Instant.ofEpochMilli(toDateLong(value));
      return LocalDateTime.ofInstant(inst, db.getZoneId());
    }
  }
}
