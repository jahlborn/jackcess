/*
Copyright (c) 2013 James Ahlborn

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
   * Whether or not this query has been marked as hidden.
   * @usage _general_method_
   */
  public boolean isHidden();

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
