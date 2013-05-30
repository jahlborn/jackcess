/*
Copyright (c) 2013 James Ahlborn

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
*/

package com.healthmarketscience.jackcess.query;

import java.util.List;

import static com.healthmarketscience.jackcess.impl.query.QueryFormat.*;


/**
 * Base interface for classes which encapsulate information about an Access
 * query.  The {@link #toSQLString()} method can be used to convert this
 * object into the actual SQL string which this query data represents.
 * 
 * @author James Ahlborn
 */
public interface Query 
{

  public enum Type 
  {
    SELECT(SELECT_QUERY_OBJECT_FLAG, 1),
    MAKE_TABLE(MAKE_TABLE_QUERY_OBJECT_FLAG, 2),
    APPEND(APPEND_QUERY_OBJECT_FLAG, 3),
    UPDATE(UPDATE_QUERY_OBJECT_FLAG, 4),
    DELETE(DELETE_QUERY_OBJECT_FLAG, 5),
    CROSS_TAB(CROSS_TAB_QUERY_OBJECT_FLAG, 6),
    DATA_DEFINITION(DATA_DEF_QUERY_OBJECT_FLAG, 7),
    PASSTHROUGH(PASSTHROUGH_QUERY_OBJECT_FLAG, 8),
    UNION(UNION_QUERY_OBJECT_FLAG, 9),
    UNKNOWN(-1, -1);

    private final int _objectFlag; 
    private final short _value;

    private Type(int objectFlag, int value) {
      _objectFlag = objectFlag;
      _value = (short)value;
    }

    public int getObjectFlag() {
      return _objectFlag;
    }

    public short getValue() {
      return _value;
    }
  }

  /**
   * Returns the name of the query.
   */
  public String getName();

  /**
   * Returns the type of the query.
   */
  public Type getType();

  /**
   * Returns the unique object id of the query.
   */
  public int getObjectId();

  public int getObjectFlag();

  /**
   * Returns the rows from the system query table from which the query
   * information was derived.
   */
  // public List<Row> getRows();

  public List<String> getParameters();

  public String getOwnerAccessType();

  /**
   * Returns the actual SQL string which this query data represents.
   */
  public String toSQLString();
}
