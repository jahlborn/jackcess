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

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.time.LocalDateTime;

import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.JetFormat;
import com.healthmarketscience.jackcess.impl.SqlHelper;

/**
 * Supported access data types.
 *
 * @author Tim McCune
 * @usage _general_class_
 */
public enum DataType {

  /**
   * Corresponds to a java {@link Boolean}.  Accepts Boolean or {@code null}
   * (which is considered {@code false}).  Equivalent to SQL {@link
   * Types#BOOLEAN}.
   */
  BOOLEAN((byte) 0x01, Types.BOOLEAN, 1),
  /**
   * Corresponds to a java {@link Byte}.  Accepts any {@link Number} (using
   * {@link Number#byteValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#TINYINT}, {@link Types#BIT}.
   */
  BYTE((byte) 0x02, Types.TINYINT, 1),
  /**
   * Corresponds to a java {@link Short}.  Accepts any {@link Number} (using
   * {@link Number#shortValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#SMALLINT}.
   */
  INT((byte) 0x03, Types.SMALLINT, 2),
  /**
   * Corresponds to a java {@link Integer}.  Accepts any {@link Number} (using
   * {@link Number#intValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#INTEGER}, {@link Types#BIGINT}.
   */
  LONG((byte) 0x04, Types.INTEGER, 4),
  /**
   * Corresponds to a java {@link BigDecimal} with at most 4 decimal places.
   * Accepts any {@link Number} (using {@link Number#doubleValue}), a
   * BigInteger, a BigDecimal (with at most 4 decimal places), Boolean as 1 or
   * 0, any Object converted to a String and parsed as BigDecimal, or {@code
   * null}.  Equivalent to SQL {@link Types#DECIMAL}.
   */
  MONEY((byte) 0x05, Types.DECIMAL, 8, false, false, 0, 0, 0, false, 4, 4, 4,
        19, 19, 19, 1),
  /**
   * Corresponds to a java {@link Float}.  Accepts any {@link Number} (using
   * {@link Number#floatValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#FLOAT}.
   */
  FLOAT((byte) 0x06, Types.FLOAT, 4),
  /**
   * Corresponds to a java {@link Double}.  Accepts any {@link Number} (using
   * {@link Number#doubleValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#DOUBLE}, {@link Types#REAL}.
   */
  DOUBLE((byte) 0x07, Types.DOUBLE, 8),
  /**
   * Corresponds to a java {@link Date} or {@link LocalDateTime}.  Accepts a
   * Date, LocalDateTime (or related types), any {@link Number} (using {@link
   * Number#longValue}), or {@code null}.  Equivalent to SQL {@link
   * Types#TIMESTAMP}, {@link Types#DATE}, {@link Types#TIME}.
   */
  SHORT_DATE_TIME((byte) 0x08, Types.TIMESTAMP, 8),
  /**
   * Corresponds to a java {@code byte[]} of max length 255 bytes.  Accepts a
   * {@code byte[]}, or {@code null}.  Equivalent to SQL {@link Types#BINARY},
   * {@link Types#VARBINARY}.
   */
  BINARY((byte) 0x09, Types.BINARY, null, true, false, 0, 255, 255, 1),
  /**
   * Corresponds to a java {@link String} of max length 255 chars.  Accepts
   * any {@link CharSequence}, any Object converted to a String , or {@code
   * null}.  Equivalent to SQL {@link Types#VARCHAR}, {@link Types#CHAR}.
   */
  TEXT((byte) 0x0A, Types.VARCHAR, null, true, false, 0,
       JetFormat.TEXT_FIELD_MAX_LENGTH, JetFormat.TEXT_FIELD_MAX_LENGTH,
       JetFormat.TEXT_FIELD_UNIT_SIZE),
  /**
   * Corresponds to a java {@code byte[]} of max length 16777215 bytes.
   * Accepts a {@code byte[]}, or {@code null}.  Equivalent to SQL
   * {@link Types#LONGVARBINARY}, {@link Types#BLOB}.
   */
  OLE((byte) 0x0B, Types.LONGVARBINARY, null, true, true, 0, 0, 0x3FFFFFFF,
      1),
  /**
   * Corresponds to a java {@link String} of max length 8388607 chars.
   * Accepts any {@link CharSequence}, any Object converted to a String , or
   * {@code null}.  Equivalent to SQL {@link Types#LONGVARCHAR}, {@link
   * Types#CLOB}.
   */
  MEMO((byte) 0x0C, Types.LONGVARCHAR, null, true, true, 0, 0, 0x3FFFFFFF,
       JetFormat.TEXT_FIELD_UNIT_SIZE),
  /**
   * Unknown data.  Handled like {@link #BINARY}.
   */
  UNKNOWN_0D((byte) 0x0D, null, null, true, false, 0, 255, 255, 1),
  /**
   * Corresponds to a java {@link String} with the pattern
   * <code>"{xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}"</code>, also known as a
   * "Replication ID" in Access.  Accepts any
   * Object converted to a String matching this pattern (surrounding "{}" are
   * optional, so {@link java.util.UUID}s are supported), or {@code null}.
   */
  GUID((byte) 0x0F, null, 16),
  /**
   * Corresponds to a java {@link BigDecimal}.  Accepts any {@link Number}
   * (using {@link Number#doubleValue}), a {@link BigInteger}, a BigDecimal,
   * Boolean as 1 or 0, any Object converted to a String and parsed as
   * BigDecimal, or {@code null}.  Equivalent to SQL {@link Types#NUMERIC}.
   */
  // for some reason numeric is "var len" even though it has a fixed size...
  NUMERIC((byte) 0x10, Types.NUMERIC, 17, true, false, 17, 17, 17,
          true, 0, 0, 28, 1, 18, 28, 1),
  /**
   * Unknown data (seems to be an alternative {@link #OLE} type, used by
   * MSysAccessObjects table).  Handled like a fixed length BINARY/OLE.
   */
  UNKNOWN_11((byte) 0x11, null, 3992),
  /**
   * Complex type corresponds to a special {@link #LONG} autonumber field
   * which is the key for a secondary table which holds the "real" data.
   */
  COMPLEX_TYPE((byte) 0x12, null, 4),
  /**
   * Corresponds to a java {@link Long}.  Accepts any {@link Number} (using
   * {@link Number#longValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#BIGINT}.
   */
  BIG_INT((byte) 0x13, Types.BIGINT, 8),
  /**
   * Corresponds to a java {@link LocalDateTime} (with 7 digits of nanosecond
   * precision).  Accepts a Date, LocalDateTime (or related types), any
   * {@link Number} (using {@link Number#longValue}), or {@code null}.
   * Equivalent to SQL {@link Types#TIMESTAMP}, {@link Types#DATE},
   * {@link Types#TIME}.
   */
  EXT_DATE_TIME((byte) 0x14, null, 42),
  /**
   * Dummy type for a fixed length type which is not currently supported.
   * Handled like a fixed length {@link #BINARY}.
   */
  UNSUPPORTED_FIXEDLEN((byte) 0xFE, null, null),
  /**
   * Placeholder type for a variable length type which is not currently
   * supported.  Handled like {@link #BINARY}.
   */
  UNSUPPORTED_VARLEN((byte) 0xFF, null, null, true, false, 0, 0, 0x3FFFFFFF,
      1);

  /** Map of SQL types to Access data types */
  private static final Map<Integer, DataType[]> SQL_TYPES =
    new HashMap<Integer, DataType[]>();
  /** Alternate map of SQL types to Access data types */
  private static final Map<Integer, DataType> ALT_SQL_TYPES =
    new HashMap<Integer, DataType>();
  static {
    for (DataType type : values()) {
      if (type._sqlType != null) {
        SQL_TYPES.put(type._sqlType, new DataType[]{type});
      }
    }
    SQL_TYPES.put(Types.BIT, new DataType[]{BYTE});
    SQL_TYPES.put(Types.BLOB, new DataType[]{OLE});
    SQL_TYPES.put(Types.CLOB, new DataType[]{MEMO});
    SQL_TYPES.put(Types.BIGINT, new DataType[]{LONG, BIG_INT});
    SQL_TYPES.put(Types.CHAR, new DataType[]{TEXT});
    SQL_TYPES.put(Types.DATE, new DataType[]{SHORT_DATE_TIME});
    SQL_TYPES.put(Types.REAL, new DataType[]{DOUBLE});
    SQL_TYPES.put(Types.TIME, new DataType[]{SHORT_DATE_TIME});
    SQL_TYPES.put(Types.VARBINARY, new DataType[]{BINARY});

    // the "alternate" types allow for larger values
    ALT_SQL_TYPES.put(Types.VARCHAR, MEMO);
    ALT_SQL_TYPES.put(Types.VARBINARY, OLE);
    ALT_SQL_TYPES.put(Types.BINARY, OLE);

    // add newer sql types if available in this jvm
    addNewSqlType("NCHAR", TEXT, null);
    addNewSqlType("NVARCHAR", TEXT, MEMO);
    addNewSqlType("LONGNVARCHAR", MEMO, null);
    addNewSqlType("NCLOB", MEMO, null);
    addNewSqlType("TIME_WITH_TIMEZONE", SHORT_DATE_TIME, null);
    addNewSqlType("TIMESTAMP_WITH_TIMEZONE", SHORT_DATE_TIME, null);
  }

  private static Map<Byte, DataType> DATA_TYPES = new HashMap<Byte, DataType>();
  static {
    for (DataType type : values()) {
      if(type.isUnsupported()) {
        continue;
      }
      DATA_TYPES.put(type._value, type);
    }
  }

  /** is this a variable length field */
  private final boolean _variableLength;
  /** is this a long value field */
  private final boolean _longValue;
  /** does this field have scale/precision */
  private final boolean _hasScalePrecision;
  /** Internal Access value */
  private final byte _value;
  /** Size in bytes of fixed length columns */
  private final Integer _fixedSize;
  /** min in bytes size for var length columns */
  private final int _minSize;
  /** default size in bytes for var length columns */
  private final int _defaultSize;
  /** Max size in bytes for var length columns */
  private final int _maxSize;
  /** SQL type equivalent, or null if none defined */
  private final Integer _sqlType;
  /** min scale value */
  private final int _minScale;
  /** the default scale value */
  private final int _defaultScale;
  /** max scale value */
  private final int _maxScale;
  /** min precision value */
  private final int _minPrecision;
  /** the default precision value */
  private final int _defaultPrecision;
  /** max precision value */
  private final int _maxPrecision;
  /** the number of bytes per "unit" for this data type */
  private final int _unitSize;

  private DataType(byte value) {
    this(value, null, null);
  }

  private DataType(byte value, Integer sqlType, Integer fixedSize) {
    this(value, sqlType, fixedSize, false, false, 0, 0, 0, 1);
  }

  private DataType(byte value, Integer sqlType, Integer fixedSize,
                   boolean variableLength,
                   boolean longValue,
                   int minSize,
                   int defaultSize,
                   int maxSize,
                   int unitSize) {
    this(value, sqlType, fixedSize, variableLength, longValue,
         minSize, defaultSize, maxSize,
         false, 0, 0, 0, 0, 0, 0, unitSize);
  }

  private DataType(byte value, Integer sqlType, Integer fixedSize,
                   boolean variableLength,
                   boolean longValue,
                   int minSize,
                   int defaultSize,
                   int maxSize,
                   boolean hasScalePrecision,
                   int minScale,
                   int defaultScale,
                   int maxScale,
                   int minPrecision,
                   int defaultPrecision,
                   int maxPrecision,
                   int unitSize) {
    _value = value;
    _sqlType = sqlType;
    _fixedSize = fixedSize;
    _variableLength = variableLength;
    _longValue = longValue;
    _minSize = minSize;
    _defaultSize = defaultSize;
    _maxSize = maxSize;
    _hasScalePrecision = hasScalePrecision;
    _minScale = minScale;
    _defaultScale = defaultScale;
    _maxScale = maxScale;
    _minPrecision = minPrecision;
    _defaultPrecision = defaultPrecision;
    _maxPrecision = maxPrecision;
    _unitSize = unitSize;
  }

  public byte getValue() {
    return _value;
  }

  public boolean isVariableLength() {
    return _variableLength;
  }

  public boolean isTrueVariableLength() {
    // some "var len" fields do not really have a variable length,
    // e.g. NUMERIC
    return (isVariableLength() && (getMinSize() != getMaxSize()));
  }

  public boolean isLongValue() {
    return _longValue;
  }

  public boolean getHasScalePrecision() {
    return _hasScalePrecision;
  }

  public int getFixedSize() {
    return getFixedSize(null);
  }

  public int getFixedSize(Short colLength) {
    if(_fixedSize != null) {
      if(colLength != null) {
        return Math.max(_fixedSize, colLength);
      }
      return _fixedSize;
    }
    if(colLength != null) {
      return colLength;
    }
    throw new IllegalArgumentException("Unexpected fixed length column " +
                                       this);
  }

  public int getMinSize() {
    return _minSize;
  }

  public int getDefaultSize() {
    return _defaultSize;
  }

  public int getMaxSize() {
    return _maxSize;
  }

  public int getSQLType() throws IOException {
    if (_sqlType != null) {
      return _sqlType;
    }
    throw new JackcessException("Unsupported data type: " + toString());
  }

  public int getMinScale() {
    return _minScale;
  }

  public int getDefaultScale() {
    return _defaultScale;
  }

  public int getMaxScale() {
    return _maxScale;
  }

  public int getMinPrecision() {
    return _minPrecision;
  }

  public int getDefaultPrecision() {
    return _defaultPrecision;
  }

  public int getMaxPrecision() {
    return _maxPrecision;
  }

  public int getUnitSize() {
    return _unitSize;
  }

  public int getUnitSize(JetFormat format) {
    if((format != null) && isTextual()) {
      return format.SIZE_TEXT_FIELD_UNIT;
    }
    return _unitSize;
  }

  public int toUnitSize(int size) {
    return toUnitSize(size, null);
  }

  public int toUnitSize(int size, JetFormat format) {
    return(size / getUnitSize(format));
  }

  public int fromUnitSize(int unitSize) {
    return fromUnitSize(unitSize, null);
  }

  public int fromUnitSize(int unitSize, JetFormat format) {
    return(unitSize * getUnitSize(format));
  }

  public boolean isValidSize(int size) {
    return isWithinRange(size, getMinSize(), getMaxSize());
  }

  public boolean isValidScale(int scale) {
    return isWithinRange(scale, getMinScale(), getMaxScale());
  }

  public boolean isValidPrecision(int precision) {
    return isWithinRange(precision, getMinPrecision(), getMaxPrecision());
  }

  private static boolean isWithinRange(int value, int minValue, int maxValue) {
    return((value >= minValue) && (value <= maxValue));
  }

  public int toValidSize(int size) {
    return toValidRange(size, getMinSize(), getMaxSize());
  }

  public int toValidScale(int scale) {
    return toValidRange(scale, getMinScale(), getMaxScale());
  }

  public int toValidPrecision(int precision) {
    return toValidRange(precision, getMinPrecision(), getMaxPrecision());
  }

  public boolean isTextual() {
    return ((this == TEXT) || (this == MEMO));
  }

  public boolean mayBeAutoNumber() {
    return((this == LONG) || (this == GUID) || (this == COMPLEX_TYPE));
  }

  public boolean isMultipleAutoNumberAllowed() {
    return (this == COMPLEX_TYPE);
  }

  public boolean isUnsupported() {
    return((this == UNSUPPORTED_FIXEDLEN) || (this == UNSUPPORTED_VARLEN));
  }

  private static int toValidRange(int value, int minValue, int maxValue) {
    return((value > maxValue) ? maxValue :
           ((value < minValue) ? minValue : value));
  }

  public static DataType fromByte(byte b) throws IOException {
    DataType rtn = DATA_TYPES.get(b);
    if (rtn != null) {
      return rtn;
    }
    throw new IOException("Unrecognized data type: " + b);
  }

  public static DataType fromSQLType(int sqlType)
    throws IOException
  {
    return fromSQLType(sqlType, 0, null);
  }

  public static DataType fromSQLType(int sqlType, int lengthInUnits)
    throws IOException
  {
    return fromSQLType(sqlType, lengthInUnits, null);
  }

  public static DataType fromSQLType(int sqlType, int lengthInUnits,
                                     Database.FileFormat fileFormat)
    throws IOException
  {
    DataType[] rtnArr = SQL_TYPES.get(sqlType);
    if(rtnArr == null) {
      throw new JackcessException("Unsupported SQL type: " + sqlType);
    }
    JetFormat format =
      ((fileFormat != null) ?
       DatabaseImpl.getFileFormatDetails(fileFormat).getFormat() :
       null);
    DataType rtn = rtnArr[0];
    if((rtnArr.length > 1) && (format != null)) {
      // there are multiple possibilities, ordered from lowest version to
      // highest version supported.  go in opposite order to find the best
      // type for this format
      for(int i = rtnArr.length - 1; i >= 0; --i) {
        DataType tmp = rtnArr[i];
        if(format.isSupportedDataType(tmp)) {
          rtn = tmp;
          break;
        }
      }
    }

    // make sure size is reasonable
    int size = rtn.fromUnitSize(lengthInUnits, format);
    if(rtn.isVariableLength() && !rtn.isValidSize(size)) {
      // try alternate type.  we always accept alternate "long value" types
      // regardless of the given lengthInUnits
      DataType altRtn = ALT_SQL_TYPES.get(sqlType);
      if((altRtn != null) &&
         (altRtn.isLongValue() || altRtn.isValidSize(size))) {
        // use alternate type
        rtn = altRtn;
      }
    }

    return rtn;
  }

  /**
   * Adds mappings for a sql type which was added after jdk 1.5 (using
   * reflection).
   */
  private static void addNewSqlType(String typeName, DataType type,
                                    DataType altType)
  {
    try {
      Integer value = SqlHelper.INSTANCE.getNewSqlType(typeName);
      SQL_TYPES.put(value, new DataType[]{type});
      if(altType != null) {
        ALT_SQL_TYPES.put(value, altType);
      }
    } catch(Exception ignored) {
      // must not be available
    }
  }

}
