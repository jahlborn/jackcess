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
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Access data type
 * @author Tim McCune
 */
public enum DataType {

  /**
   * Corresponds to a java Boolean.  Accepts Boolean or {@code null} (which is
   * considered {@code false}).  Equivalent to SQL {@link Types#BOOLEAN}.
   */
  BOOLEAN((byte) 0x01, Types.BOOLEAN, 0),
  /**
   * Corresponds to a java Byte.  Accepts any Number (using
   * {@link Number#byteValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#TINYINT}, {@link Types#BIT}.
   */
  BYTE((byte) 0x02, Types.TINYINT, 1),
  /**
   * Corresponds to a java Short.  Accepts any Number (using
   * {@link Number#shortValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#SMALLINT}.
   */
  INT((byte) 0x03, Types.SMALLINT, 2),
  /**
   * Corresponds to a java Integer.  Accepts any Number (using
   * {@link Number#intValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#INTEGER}, {@link Types#BIGINT}.
   */
  LONG((byte) 0x04, Types.INTEGER, 4),
  /**
   * Corresponds to a java BigDecimal with at most 4 decimal places.  Accepts
   * any Number (using {@link Number#doubleValue}), a BigInteger, a BigDecimal
   * (with at most 4 decimal places), Boolean as 1 or 0, any Object converted
   * to a String and parsed as BigDecimal, or {@code null}.  Equivalent to SQL
   * {@link Types#DECIMAL}.
   */
  MONEY((byte) 0x05, Types.DECIMAL, 8),
  /**
   * Corresponds to a java Float.  Accepts any Number (using
   * {@link Number#floatValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#FLOAT}.
   */
  FLOAT((byte) 0x06, Types.FLOAT, 4),
  /**
   * Corresponds to a java Double.  Accepts any Number (using
   * {@link Number#doubleValue}), Boolean as 1 or 0, any Object converted to a
   * String and parsed as Double, or {@code null}.  Equivalent to SQL
   * {@link Types#DOUBLE}, {@link Types#REAL}.
   */
  DOUBLE((byte) 0x07, Types.DOUBLE, 8),
  /**
   * Corresponds to a java Date.  Accepts a Date, any Number (using
   * {@link Number#longValue}), or {@code null}.  Equivalent to SQL
   * {@link Types#TIMESTAMP}, {@link Types#DATE}, {@link Types#TIME}.
   */
  SHORT_DATE_TIME((byte) 0x08, Types.TIMESTAMP, 8),
  /**
   * Corresponds to a java {@code byte[]} of max length 255 bytes.  Accepts a
   * {@code byte[]}, or {@code null}.  Equivalent to SQL {@link Types#BINARY},
   * {@link Types#VARBINARY}.
   */
  BINARY((byte) 0x09, Types.BINARY, null, true, false, 0, 255, 255, 1),
  /**
   * Corresponds to a java String of max length 255 chars.  Accepts any
   * CharSequence, any Object converted to a String , or {@code null}.
   * Equivalent to SQL {@link Types#VARCHAR}, {@link Types#CHAR}.
   */
  TEXT((byte) 0x0A, Types.VARCHAR, null, true, false, 0,
       50 * JetFormat.TEXT_FIELD_UNIT_SIZE,
       (int)JetFormat.TEXT_FIELD_MAX_LENGTH, JetFormat.TEXT_FIELD_UNIT_SIZE),
  /**
   * Corresponds to a java {@code byte[]} of max length 16777215 bytes.
   * Accepts a {@code byte[]}, or {@code null}.  Equivalent to SQL
   * {@link Types#LONGVARBINARY}, {@link Types#BLOB}.
   */
  OLE((byte) 0x0B, Types.LONGVARBINARY, null, true, true, 0, null, 0xFFFFFF,
      1),
  /**
   * Corresponds to a java String of max length 8388607 chars.  Accepts any
   * CharSequence, any Object converted to a String , or {@code null}.
   * Equivalent to SQL {@link Types#LONGVARCHAR}, {@link Types#CLOB}.
   */
  MEMO((byte) 0x0C, Types.LONGVARCHAR, null, true, true, 0, null, 0xFFFFFF,
       JetFormat.TEXT_FIELD_UNIT_SIZE),
  /**
   * Unknown data.  Handled like BINARY.
   */
  UNKNOWN_0D((byte) 0x0D, null, null, true, false, 0, 255, 255, 1),
  /**
   * Corresponds to a java String with the pattern
   * <code>"{xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}"</code>, also known as a
   * "Replication ID" in Access.  Accepts any
   * Object converted to a String matching this pattern (surrounding "{}" are
   * optional, so {@link java.util.UUID}s are supported), or {@code null}.
   */
  GUID((byte) 0x0F, null, 16),
  /**
   * Corresponds to a java BigDecimal.  Accepts any Number (using
   * {@link Number#doubleValue}), a BigInteger, a BigDecimal, Boolean as 1 or
   * 0, any Object converted to a String and parsed as BigDecimal, or
   * {@code null}.  Equivalent to SQL {@link Types#NUMERIC}.
   */
  // for some reason numeric is "var len" even though it has a fixed size...
  NUMERIC((byte) 0x10, Types.NUMERIC, 17, true, false, 17, 17, 17,
          true, 0, 0, 28, 1, 18, 28, 1),
  /**
   * Unknown data (seems to be an alternative OLE type, used by
   * MSysAccessObjects table).  Handled like a fixed length BINARY/OLE.
   */
  UNKNOWN_11((byte) 0x11, null, 3992);

  /** Map of SQL types to Access data types */
  private static Map<Integer, DataType> SQL_TYPES = new HashMap<Integer, DataType>();
  /** Alternate map of SQL types to Access data types */
  private static Map<Integer, DataType> ALT_SQL_TYPES = new HashMap<Integer, DataType>();
  static {
    for (DataType type : DataType.values()) {
      if (type._sqlType != null) {
        SQL_TYPES.put(type._sqlType, type);
      }
    }
    SQL_TYPES.put(Types.BIT, BYTE);
    SQL_TYPES.put(Types.BLOB, OLE);
    SQL_TYPES.put(Types.CLOB, MEMO);
    SQL_TYPES.put(Types.BIGINT, LONG);
    SQL_TYPES.put(Types.CHAR, TEXT);
    SQL_TYPES.put(Types.DATE, SHORT_DATE_TIME);
    SQL_TYPES.put(Types.REAL, DOUBLE);
    SQL_TYPES.put(Types.TIME, SHORT_DATE_TIME);
    SQL_TYPES.put(Types.VARBINARY, BINARY);

    // the "alternate" types allow for larger values
    ALT_SQL_TYPES.put(Types.VARCHAR, MEMO);
    ALT_SQL_TYPES.put(Types.VARBINARY, OLE);
    ALT_SQL_TYPES.put(Types.BINARY, OLE);
  }
  
  private static Map<Byte, DataType> DATA_TYPES = new HashMap<Byte, DataType>();
  static {
    for (DataType type : DataType.values()) {
      DATA_TYPES.put(type._value, type);
    }
  }

  /** is this a variable length field */
  private boolean _variableLength;
  /** is this a long value field */
  private boolean _longValue;
  /** does this field have scale/precision */
  private boolean _hasScalePrecision;
  /** Internal Access value */
  private byte _value;
  /** Size in bytes of fixed length columns */
  private Integer _fixedSize;
  /** min in bytes size for var length columns */
  private Integer _minSize;
  /** default size in bytes for var length columns */
  private Integer _defaultSize;
  /** Max size in bytes for var length columns */
  private Integer _maxSize;
  /** SQL type equivalent, or null if none defined */
  private Integer _sqlType;
  /** min scale value */
  private Integer _minScale;
  /** the default scale value */
  private Integer _defaultScale;
  /** max scale value */
  private Integer _maxScale;
  /** min precision value */
  private Integer _minPrecision;
  /** the default precision value */
  private Integer _defaultPrecision;
  /** max precision value */
  private Integer _maxPrecision;
  /** the number of bytes per "unit" for this data type */
  private int _unitSize;
  
  private DataType(byte value) {
    this(value, null, null);
  }
  
  private DataType(byte value, Integer sqlType, Integer fixedSize) {
    this(value, sqlType, fixedSize, false, false, null, null, null, 1);
  }

  private DataType(byte value, Integer sqlType, Integer fixedSize,
                   boolean variableLength,
                   boolean longValue,
                   Integer minSize,
                   Integer defaultSize,
                   Integer maxSize,
                   int unitSize) {
    this(value, sqlType, fixedSize, variableLength, longValue,
         minSize, defaultSize, maxSize,
         false, null, null, null, null, null, null, unitSize);
  }
  
  private DataType(byte value, Integer sqlType, Integer fixedSize,
                   boolean variableLength,
                   boolean longValue,
                   Integer minSize,
                   Integer defaultSize,
                   Integer maxSize,
                   boolean hasScalePrecision,
                   Integer minScale,
                   Integer defaultScale,
                   Integer maxScale,
                   Integer minPrecision,
                   Integer defaultPrecision,
                   Integer maxPrecision,
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
  
  public int getSQLType() throws SQLException {
    if (_sqlType != null) {
      return _sqlType;
    }
    throw new SQLException("Unsupported data type: " + toString());
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

  public int toUnitSize(int size)
  {
    return(size / getUnitSize());
  }

  public int fromUnitSize(int unitSize)
  {
    return(unitSize * getUnitSize());
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
    throws SQLException
  {
    return fromSQLType(sqlType, 0);
  }
  
  public static DataType fromSQLType(int sqlType, int lengthInUnits)
    throws SQLException
  {
    DataType rtn = SQL_TYPES.get(sqlType);
    if(rtn == null) {
      throw new SQLException("Unsupported SQL type: " + sqlType);
    }

    // make sure size is reasonable
    int size = lengthInUnits * rtn.getUnitSize();
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

}
