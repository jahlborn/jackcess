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
  
  BOOLEAN((byte) 0x01, Types.BOOLEAN, 0),
  BYTE((byte) 0x02, Types.TINYINT, 1),
  INT((byte) 0x03, Types.SMALLINT, 2),
  LONG((byte) 0x04, Types.INTEGER, 4),
  MONEY((byte) 0x05, Types.DECIMAL, 8),
  FLOAT((byte) 0x06, Types.FLOAT, 4),
  DOUBLE((byte) 0x07, Types.DOUBLE, 8),
  SHORT_DATE_TIME((byte) 0x08, Types.TIMESTAMP, 8),
  BINARY((byte) 0x09, Types.BINARY, null, true, false, 255, 255),
  TEXT((byte) 0x0A, Types.VARCHAR, null, true, false, 50 * 2,
       (int)JetFormat.TEXT_FIELD_MAX_LENGTH),
  OLE((byte) 0x0B, Types.LONGVARBINARY, null, true, true, null, 0xFFFFFF),
  MEMO((byte) 0x0C, Types.LONGVARCHAR, null, true, true, null, 0xFFFFFF),
  UNKNOWN_0D((byte) 0x0D),
  GUID((byte) 0x0F, null, 16),
  NUMERIC((byte) 0x10, Types.NUMERIC, 17, false, false, null, null,
          true, 0, 0, 28, 1, 18, 28);

  /** Map of SQL types to Access data types */
  private static Map<Integer, DataType> SQL_TYPES = new HashMap<Integer, DataType>();
  static {
    for (DataType type : DataType.values()) {
      if (type._sqlType != null) {
        SQL_TYPES.put(type._sqlType, type);
      }
    }
    SQL_TYPES.put(Types.BIT, BYTE);
    SQL_TYPES.put(Types.BLOB, OLE);
    SQL_TYPES.put(Types.BIGINT, LONG);
    SQL_TYPES.put(Types.CHAR, TEXT);
    SQL_TYPES.put(Types.DATE, SHORT_DATE_TIME);
    SQL_TYPES.put(Types.REAL, DOUBLE);
    SQL_TYPES.put(Types.TIME, SHORT_DATE_TIME);
    SQL_TYPES.put(Types.VARBINARY, BINARY);
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
  /** default size for var length columns */
  private Integer _defaultSize;
  /** Max size in bytes */
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
  
  private DataType(byte value) {
    this(value, null, null);
  }
  
  private DataType(byte value, Integer sqlType, Integer fixedSize) {
    this(value, sqlType, fixedSize, false, false, null, null);
  }

  private DataType(byte value, Integer sqlType, Integer fixedSize,
                   boolean variableLength,
                   boolean longValue,
                   Integer defaultSize,
                   Integer maxSize) {
    this(value, sqlType, fixedSize, variableLength, longValue, defaultSize,
         maxSize, false, null, null, null, null, null, null);
  }
  
  private DataType(byte value, Integer sqlType, Integer fixedSize,
                   boolean variableLength,
                   boolean longValue,
                   Integer defaultSize,
                   Integer maxSize,
                   boolean hasScalePrecision,
                   Integer minScale,
                   Integer defaultScale,
                   Integer maxScale,
                   Integer minPrecision,
                   Integer defaultPrecision,
                   Integer maxPrecision) {
    _value = value;
    _sqlType = sqlType;
    _fixedSize = fixedSize;
    _variableLength = variableLength;
    _longValue = longValue;
    _defaultSize = defaultSize;
    _maxSize = maxSize;
    _hasScalePrecision = hasScalePrecision;
    _minScale = minScale;
    _defaultScale = defaultScale;
    _maxScale = maxScale;
    _minPrecision = minPrecision;
    _defaultPrecision = defaultPrecision;
    _maxPrecision = maxPrecision;
  }
  
  public byte getValue() {
    return _value;
  }
  
  public boolean isVariableLength() {
    return _variableLength;
  }

  public boolean isLongValue() {
    return _longValue;
  }

  public boolean getHasScalePrecision() {
    return _hasScalePrecision;
  }
  
  public int getFixedSize() {
    if(_fixedSize != null) {
      return _fixedSize;
    } else {
      throw new IllegalArgumentException("FIX ME");
    }
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
    } else {
      throw new SQLException("Unsupported data type: " + toString());
    }
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
  
  public static DataType fromByte(byte b) throws IOException {
    DataType rtn = DATA_TYPES.get(b);
    if (rtn != null) {
      return rtn;
    } else {
      throw new IOException("Unrecognized data type: " + b);
    }
  }
  
  public static DataType fromSQLType(int sqlType) throws SQLException {
    DataType rtn = SQL_TYPES.get(sqlType);
    if (rtn != null) {
      return rtn;
    } else {
      throw new SQLException("Unsupported SQL type: " + sqlType);
    }
  }
  
}
