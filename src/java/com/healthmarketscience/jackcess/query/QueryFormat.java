/*
Copyright (c) 2008 Health Market Science, Inc.

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

package com.healthmarketscience.jackcess.query;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.DataType;
import org.apache.commons.lang.SystemUtils;

/**
 * Constants used by the query data parsing.
 * 
 * @author James Ahlborn
 */
public class QueryFormat 
{

  private QueryFormat() {}

  public static final int SELECT_QUERY_OBJECT_FLAG = 0;
  public static final int MAKE_TABLE_QUERY_OBJECT_FLAG = 80;
  public static final int APPEND_QUERY_OBJECT_FLAG = 64;
  public static final int UPDATE_QUERY_OBJECT_FLAG = 48;
  public static final int DELETE_QUERY_OBJECT_FLAG = 32;
  public static final int CROSS_TAB_QUERY_OBJECT_FLAG = 16;
  public static final int DATA_DEF_QUERY_OBJECT_FLAG = 96;
  public static final int PASSTHROUGH_QUERY_OBJECT_FLAG = 112;
  public static final int UNION_QUERY_OBJECT_FLAG = 128;
  // dbQSPTBulk = 144
  // dbQCompound = 160
  // dbQProcedure = 224
  // dbQAction = 240

  public static final String COL_ATTRIBUTE = "Attribute";
  public static final String COL_EXPRESSION = "Expression";
  public static final String COL_FLAG = "Flag";
  public static final String COL_EXTRA = "LvExtra";
  public static final String COL_NAME1 = "Name1";
  public static final String COL_NAME2 = "Name2";
  public static final String COL_OBJECTID = "ObjectId";
  public static final String COL_ORDER = "Order";

  public static final Byte START_ATTRIBUTE = 0;
  public static final Byte TYPE_ATTRIBUTE = 1;
  public static final Byte PARAMETER_ATTRIBUTE = 2;
  public static final Byte FLAG_ATTRIBUTE = 3;
  public static final Byte REMOTEDB_ATTRIBUTE = 4;
  public static final Byte TABLE_ATTRIBUTE = 5;
  public static final Byte COLUMN_ATTRIBUTE = 6;
  public static final Byte JOIN_ATTRIBUTE = 7;
  public static final Byte WHERE_ATTRIBUTE = 8;
  public static final Byte GROUPBY_ATTRIBUTE = 9;
  public static final Byte HAVING_ATTRIBUTE = 10;
  public static final Byte ORDERBY_ATTRIBUTE = 11;
  public static final Byte END_ATTRIBUTE = (byte)255;

  public static final short UNION_FLAG = 0x02;

  public static final Short TEXT_FLAG = (short)DataType.TEXT.getValue();

  public static final String DESCENDING_FLAG = "D";

  public static final short SELECT_STAR_SELECT_TYPE = 0x01;
  public static final short DISTINCT_SELECT_TYPE = 0x02;
  public static final short OWNER_ACCESS_SELECT_TYPE = 0x04;
  public static final short DISTINCT_ROW_SELECT_TYPE = 0x08;
  public static final short TOP_SELECT_TYPE = 0x10;
  public static final short PERCENT_SELECT_TYPE = 0x20;

  public static final short APPEND_VALUE_FLAG = (short)0x8000;

  public static final short CROSSTAB_PIVOT_FLAG = 0x01;
  public static final short CROSSTAB_NORMAL_FLAG = 0x02;  

  public static final String UNION_PART1 = "X7YZ_____1";
  public static final String UNION_PART2 = "X7YZ_____2";

  public static final String DEFAULT_TYPE = "";

  public static final Pattern QUOTABLE_CHAR_PAT = Pattern.compile("\\W");

  public static final Pattern IDENTIFIER_SEP_PAT = Pattern.compile("\\.");
  public static final char IDENTIFIER_SEP_CHAR = '.';

  public static final String NEWLINE = SystemUtils.LINE_SEPARATOR;


  public static final Map<Short,String> PARAM_TYPE_MAP = 
    new HashMap<Short,String>();
  static {
    PARAM_TYPE_MAP.put((short)0, "Value");
    PARAM_TYPE_MAP.put((short)DataType.BOOLEAN.getValue(), "Bit");
    PARAM_TYPE_MAP.put((short)DataType.TEXT.getValue(), "Text");
    PARAM_TYPE_MAP.put((short)DataType.BYTE.getValue(), "Byte");
    PARAM_TYPE_MAP.put((short)DataType.INT.getValue(), "Short");
    PARAM_TYPE_MAP.put((short)DataType.LONG.getValue(), "Long");
    PARAM_TYPE_MAP.put((short)DataType.MONEY.getValue(), "Currency");
    PARAM_TYPE_MAP.put((short)DataType.FLOAT.getValue(), "IEEESingle");
    PARAM_TYPE_MAP.put((short)DataType.DOUBLE.getValue(), "IEEEDouble");
    PARAM_TYPE_MAP.put((short)DataType.SHORT_DATE_TIME.getValue(), "DateTime");
    PARAM_TYPE_MAP.put((short)DataType.BINARY.getValue(), "Binary");
    PARAM_TYPE_MAP.put((short)DataType.OLE.getValue(), "LongBinary");
    PARAM_TYPE_MAP.put((short)DataType.GUID.getValue(), "Guid");
  }

  public static final Map<Short,String> JOIN_TYPE_MAP = 
    new HashMap<Short,String>();
  static {
    JOIN_TYPE_MAP.put((short)1, " INNER JOIN ");
    JOIN_TYPE_MAP.put((short)2, " LEFT JOIN ");
    JOIN_TYPE_MAP.put((short)3, " RIGHT JOIN ");
  }

}
