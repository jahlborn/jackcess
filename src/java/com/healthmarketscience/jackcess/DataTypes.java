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
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.collections.BidiMap;

/**
 * Access data types
 * @author Tim McCune
 */
public final class DataTypes {
  
  public static final byte BOOLEAN = 0x01;
  public static final byte BYTE = 0x02;
  public static final byte INT = 0x03;
  public static final byte LONG = 0x04;
  public static final byte MONEY = 0x05;
  public static final byte FLOAT = 0x06;
  public static final byte DOUBLE = 0x07;
  public static final byte SHORT_DATE_TIME = 0x08;
  public static final byte BINARY = 0x09;
  public static final byte TEXT = 0x0A;
  public static final byte OLE = 0x0B;
  public static final byte MEMO = 0x0C;
  public static final byte UNKNOWN_0D = 0x0D;
  public static final byte GUID = 0x0F;
  public static final byte NUMERIC = 0x10;
  
  /** Map of Access data types to SQL data types */
  private static BidiMap SQL_TYPES = new DualHashBidiMap();
  static {
    SQL_TYPES.put(new Byte(BOOLEAN), new Integer(Types.BOOLEAN));
    SQL_TYPES.put(new Byte(BYTE), new Integer(Types.TINYINT));
    SQL_TYPES.put(new Byte(INT), new Integer(Types.SMALLINT));
    SQL_TYPES.put(new Byte(LONG), new Integer(Types.INTEGER));
    SQL_TYPES.put(new Byte(MONEY), new Integer(Types.DECIMAL));
    SQL_TYPES.put(new Byte(FLOAT), new Integer(Types.FLOAT));
    SQL_TYPES.put(new Byte(DOUBLE), new Integer(Types.DOUBLE));
    SQL_TYPES.put(new Byte(SHORT_DATE_TIME), new Integer(Types.TIMESTAMP));
    SQL_TYPES.put(new Byte(BINARY), new Integer(Types.BINARY));
    SQL_TYPES.put(new Byte(TEXT), new Integer(Types.VARCHAR));
    SQL_TYPES.put(new Byte(OLE), new Integer(Types.LONGVARBINARY));
    SQL_TYPES.put(new Byte(MEMO), new Integer(Types.LONGVARCHAR));
  }
  
  private DataTypes() {}
  
  public static int toSQLType(byte dataType) throws SQLException {
    Integer i = (Integer) SQL_TYPES.get(new Byte(dataType));
    if (i != null) {
      return i.intValue();
    } else {
      throw new SQLException("Unsupported data type: " + dataType);
    }
  }
  
  public static byte fromSQLType(int sqlType) throws SQLException {
    Byte b = (Byte) SQL_TYPES.getKey(new Integer(sqlType));
    if (b != null) {
      return b.byteValue();
    } else {
      throw new SQLException("Unsupported SQL type: " + sqlType);
    }
  }
  
}
