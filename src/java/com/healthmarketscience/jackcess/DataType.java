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
  BINARY((byte) 0x09, Types.BINARY, 255, true),
  TEXT((byte) 0x0A, Types.VARCHAR, 50 * 2, true),
  OLE((byte) 0x0B, Types.LONGVARBINARY, 12),
  MEMO((byte) 0x0C, Types.LONGVARCHAR, 12),
  UNKNOWN_0D((byte) 0x0D),
  GUID((byte) 0x0F, null, 16),
  NUMERIC((byte) 0x10, Types.NUMERIC, 17);

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
  
  private boolean _variableLength = false;
  /** Internal Access value */
  private byte _value;
  /** Size in bytes */
  private Integer _size;
  /** SQL type equivalent, or null if none defined */
  private Integer _sqlType;
  
  private DataType(byte value) {
    _value = value;
  }
  
  private DataType(byte value, Integer sqlType, Integer size) {
    this(value);
    _sqlType = sqlType;
    _size = size;
  }
  
  private DataType(byte value, Integer sqlType, Integer size,
                   boolean variableLength) {
    this(value, sqlType, size);
    _variableLength = variableLength;
  }
  
  public byte getValue() {
    return _value;
  }
  
  public boolean isVariableLength() {
    return _variableLength;
  }
  
  public int getSize() {
    if (_size != null) {
      return _size;
    } else {
      throw new IllegalArgumentException("FIX ME");
    }
  }
  
  public int getSQLType() throws SQLException {
    if (_sqlType != null) {
      return _sqlType;
    } else {
      throw new SQLException("Unsupported data type: " + toString());
    }
  }
  
  public static DataType fromByte(byte b) throws SQLException {
    DataType rtn = DATA_TYPES.get(b);
    if (rtn != null) {
      return rtn;
    } else {
      throw new SQLException("Unrecognized data type: " + b);
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
