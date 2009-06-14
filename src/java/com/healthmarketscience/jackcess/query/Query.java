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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.healthmarketscience.jackcess.query.QueryFormat.*;


/**
 * Base class for classes which encapsulate information about an Access query.
 * The {@link #toSQLString()} method can be used to convert this object into
 * the actual SQL string which this query data represents.
 * 
 * @author James Ahlborn
 */
public abstract class Query 
{
  protected static final Log LOG = LogFactory.getLog(Query.class);  

  private static final Row EMPTY_ROW = 
    new Row(Collections.<String,Object>emptyMap());

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

  private final String _name;
  private final List<Row> _rows;
  private final int _objectId;
  private final Type _type;

  protected Query(String name, List<Row> rows, int objectId, Type type) {
    _name = name;
    _rows = rows;
    _objectId = objectId;
    _type = type;

    if(type != Type.UNKNOWN) {
      short foundType = getShortValue(getQueryType(rows),
                                      _type.getValue());
      if(foundType != _type.getValue()) {
        throw new IllegalStateException("Unexpected query type " + foundType);
      }
    }
  }

  /**
   * Returns the name of the query.
   */
  public String getName() {
    return _name;
  }

  /**
   * Returns the type of the query.
   */
  public Type getType() {
    return _type;
  }

  /**
   * Returns the unique object id of the query.
   */
  public int getObjectId() {
    return _objectId;
  }

  public int getObjectFlag() {
    return getType().getObjectFlag();
  }

  /**
   * Returns the rows from the system query table from which the query
   * information was derived.
   */
  public List<Row> getRows() {
    return _rows;
  }

  protected List<Row> getRowsByAttribute(Byte attribute) {
    return getRowsByAttribute(getRows(), attribute);
  }

  protected Row getRowByAttribute(Byte attribute) {
    return getUniqueRow(getRowsByAttribute(getRows(), attribute));
  }

  protected Row getTypeRow() {
    return getRowByAttribute(TYPE_ATTRIBUTE);
  }

  protected List<Row> getParameterRows() {
    return getRowsByAttribute(PARAMETER_ATTRIBUTE);
  }

  protected Row getFlagRow() {
    return getRowByAttribute(FLAG_ATTRIBUTE);
  }

  protected Row getRemoteDatabaseRow() {
    return getRowByAttribute(REMOTEDB_ATTRIBUTE);
  }

  protected List<Row> getTableRows() {
    return getRowsByAttribute(TABLE_ATTRIBUTE);
  }

  protected List<Row> getColumnRows() {
    return getRowsByAttribute(COLUMN_ATTRIBUTE);
  }

  protected List<Row> getJoinRows() {
    return getRowsByAttribute(JOIN_ATTRIBUTE);
  }

  protected Row getWhereRow() {
    return getRowByAttribute(WHERE_ATTRIBUTE);
  }

  protected List<Row> getGroupByRows() {
    return getRowsByAttribute(GROUPBY_ATTRIBUTE);
  }

  protected Row getHavingRow() {
    return getRowByAttribute(HAVING_ATTRIBUTE);
  }

  protected List<Row> getOrderByRows() {
    return getRowsByAttribute(ORDERBY_ATTRIBUTE);
  }

  protected abstract void toSQLString(StringBuilder builder);

  protected void toSQLParameterString(StringBuilder builder) {
    // handle any parameters
    List<String> params = getParameters();
    if(!params.isEmpty()) {
      builder.append("PARAMETERS ").append(params)
        .append(';').append(NEWLINE);
    }
  }

  public List<String> getParameters() 
  {
    return (new RowFormatter(getParameterRows()) {
        @Override protected void format(StringBuilder builder, Row row) {
          String typeName = PARAM_TYPE_MAP.get(row.flag);
          if(typeName == null) {
            throw new IllegalStateException("Unknown param type " + row.flag);
          }
              
          builder.append(row.name1).append(' ').append(typeName);
          if((TEXT_FLAG.equals(row.flag)) && (getIntValue(row.extra, 0) > 0)) {
            builder.append('(').append(row.extra).append(')');
          }
        }
      }).format();
  }

  protected List<String> getFromTables() 
  {
    List<Join> joinExprs = new ArrayList<Join>();
    for(Row table : getTableRows()) {
      StringBuilder builder = new StringBuilder();

      if(table.expression != null) {
        toQuotedExpr(builder, table.expression).append(IDENTIFIER_SEP_CHAR);
      }
      if(table.name1 != null) {
        toOptionalQuotedExpr(builder, table.name1, true);
      }
      toAlias(builder, table.name2);

      String key = ((table.name2 != null) ? table.name2 : table.name1);
      joinExprs.add(new Join(key, builder.toString()));
    }


    List<Row> joins = getJoinRows();
    if(!joins.isEmpty()) {

      // combine any multi-column joins
      Collection<List<Row>> comboJoins = combineJoins(joins);
      
      for(List<Row> comboJoin : comboJoins) {

        Row join = comboJoin.get(0);
        String joinExpr = join.expression;

        if(comboJoin.size() > 1) {

          // combine all the join expressions with "AND"
          AppendableList<String> comboExprs = new AppendableList<String>() {
            private static final long serialVersionUID = 0L;
            @Override
            protected String getSeparator() {
              return ") AND (";
            }
          };
          for(Row tmpJoin : comboJoin) {
            comboExprs.add(tmpJoin.expression);
          }

          joinExpr = new StringBuilder().append("(")
            .append(comboExprs).append(")").toString();
        }

        String fromTable = join.name1;
        String toTable = join.name2;
      
        Join fromExpr = getJoinExpr(fromTable, joinExprs);
        Join toExpr = getJoinExpr(toTable, joinExprs);
        String joinType = JOIN_TYPE_MAP.get(join.flag);
        if(joinType == null) {
            throw new IllegalStateException("Unknown join type " + join.flag);
        }

        String expr = new StringBuilder().append(fromExpr)
          .append(joinType).append(toExpr).append(" ON ")
          .append(joinExpr).toString();

        fromExpr.join(toExpr, expr);
        joinExprs.add(fromExpr);
      }
    }

    List<String> result = new AppendableList<String>();
    for(Join joinExpr : joinExprs) {
      result.add(joinExpr.expression);
    }

    return result;
  }

  private Join getJoinExpr(String table, List<Join> joinExprs)
  {
    for(Iterator<Join> iter = joinExprs.iterator(); iter.hasNext(); ) {
      Join joinExpr = iter.next();
      if(joinExpr.tables.contains(table)) {
        iter.remove();
        return joinExpr;
      }
    }
    throw new IllegalStateException("Cannot find join table " + table);
  }

  private Collection<List<Row>> combineJoins(List<Row> joins)
  {
    // combine joins with the same to/from tables
    Map<List<String>,List<Row>> comboJoinMap = 
      new LinkedHashMap<List<String>,List<Row>>();
    for(Row join : joins) {
      List<String> key = Arrays.asList(join.name1, join.name2);
      List<Row> comboJoins = comboJoinMap.get(key);
      if(comboJoins == null) {
        comboJoins = new ArrayList<Row>();
        comboJoinMap.put(key, comboJoins);
      } else {
        if((short)comboJoins.get(0).flag != (short)join.flag) {
          throw new IllegalStateException(
              "Mismatched join flags for combo joins");
        }
      }
      comboJoins.add(join);
    }
    return comboJoinMap.values();
  }

  protected String getFromRemoteDbPath() 
  {
    return getRemoteDatabaseRow().name1;
  }

  protected String getFromRemoteDbType() 
  {
    return getRemoteDatabaseRow().expression;
  }

  protected String getWhereExpression()
  {
    return getWhereRow().expression;
  }

  protected List<String> getOrderings() 
  {
    return (new RowFormatter(getOrderByRows()) {
        @Override protected void format(StringBuilder builder, Row row) {
          builder.append(row.expression);
          if(DESCENDING_FLAG.equalsIgnoreCase(row.name1)) {
            builder.append(" DESC");
          }
        }
      }).format();
  }

  public String getOwnerAccessType() {
    return(hasFlag(OWNER_ACCESS_SELECT_TYPE) ?
           "WITH OWNERACCESS OPTION" : DEFAULT_TYPE);
  }

  protected boolean hasFlag(int flagMask)
  {
    return hasFlag(getFlagRow(), flagMask);
  }

  protected boolean supportsStandardClauses() {
    return true;
  }

  /**
   * Returns the actual SQL string which this query data represents.
   */
  public String toSQLString() 
  {
    StringBuilder builder = new StringBuilder();
    if(supportsStandardClauses()) {
      toSQLParameterString(builder);
    }

    toSQLString(builder);

    if(supportsStandardClauses()) {

      String accessType = getOwnerAccessType();
      if(!DEFAULT_TYPE.equals(accessType)) {
        builder.append(NEWLINE).append(accessType);
      }
      
      builder.append(';');
    }
    return builder.toString();
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

  /**
   * Creates a concrete Query instance from the given query data.
   *
   * @param objectFlag the flag indicating the type of the query
   * @param name the name of the query
   * @param rows the rows from the system query table containing the data
   *             describing this query
   * @param objectId the unique object id of this query
   *
   * @return a Query instance for the given query data
   */
  public static Query create(int objectFlag, String name, List<Row> rows, 
                             int objectId)
  {
    try {
      switch(objectFlag) {
      case SELECT_QUERY_OBJECT_FLAG:
        return new SelectQuery(name, rows, objectId);
      case MAKE_TABLE_QUERY_OBJECT_FLAG:
        return new MakeTableQuery(name, rows, objectId);
      case APPEND_QUERY_OBJECT_FLAG:
        return new AppendQuery(name, rows, objectId);
      case UPDATE_QUERY_OBJECT_FLAG:
        return new UpdateQuery(name, rows, objectId);
      case DELETE_QUERY_OBJECT_FLAG:
        return new DeleteQuery(name, rows, objectId);
      case CROSS_TAB_QUERY_OBJECT_FLAG:
        return new CrossTabQuery(name, rows, objectId);
      case DATA_DEF_QUERY_OBJECT_FLAG:
        return new DataDefinitionQuery(name, rows, objectId);
      case PASSTHROUGH_QUERY_OBJECT_FLAG:
        return new PassthroughQuery(name, rows, objectId);
      case UNION_QUERY_OBJECT_FLAG:
        return new UnionQuery(name, rows, objectId);
      default:
        // unknown querytype
        throw new IllegalStateException(
            "unknown query object flag " + objectFlag);
      }
    } catch(IllegalStateException e) {
      LOG.warn("Failed parsing query", e);
    }

    // return unknown query
    return new UnknownQuery(name, rows, objectId, objectFlag);
  }

  private static Short getQueryType(List<Row> rows)
  {
    return getUniqueRow(getRowsByAttribute(rows, TYPE_ATTRIBUTE)).flag;
  }

  private static List<Row> getRowsByAttribute(List<Row> rows, Byte attribute) {
    List<Row> result = new ArrayList<Row>();
    for(Row row : rows) {
      if(attribute.equals(row.attribute)) {
        result.add(row);
      }
    }
    return result;
  }

  protected static Row getUniqueRow(List<Row> rows) {
    if(rows.size() == 1) {
      return rows.get(0);
    }
    if(rows.isEmpty()) {
      return EMPTY_ROW;
    }
    throw new IllegalStateException("Unexpected number of rows for" + rows);
  }

  protected static List<Row> filterRowsByFlag(
      List<Row> rows, final short flag) 
  {
    return new RowFilter() {
        @Override protected boolean keep(Row row) {
          return hasFlag(row, flag);
        }
      }.filter(rows);
  }

  protected static List<Row> filterRowsByNotFlag(
      List<Row> rows, final short flag) 
  {
    return new RowFilter() {
        @Override protected boolean keep(Row row) {
          return !hasFlag(row, flag);
        }
      }.filter(rows);
  }

  protected static boolean hasFlag(Row row, int flagMask)
  {
    return((getShortValue(row.flag, 0) & flagMask) != 0);
  }

  protected static short getShortValue(Short s, int def) {
    return ((s != null) ? (short)s : (short)def);
  }

  protected static int getIntValue(Integer i, int def) {
    return ((i != null) ? (int)i : def);
  }

  protected static StringBuilder toOptionalQuotedExpr(StringBuilder builder, 
                                                      String fullExpr,
                                                      boolean isIdentifier)
  {
    String[] exprs = (isIdentifier ? 
                      IDENTIFIER_SEP_PAT.split(fullExpr) : 
                      new String[]{fullExpr});
    for(int i = 0; i < exprs.length; ++i) {
      String expr = exprs[i];
      if(QUOTABLE_CHAR_PAT.matcher(expr).find()) {
        toQuotedExpr(builder, expr);
      } else {
        builder.append(expr);
      }
      if(i < (exprs.length - 1)) {
        builder.append(IDENTIFIER_SEP_CHAR);
      }
    }
    return builder;
  }

  protected static StringBuilder toQuotedExpr(StringBuilder builder, 
                                              String expr)
  {
    return builder.append('[').append(expr).append(']');
  }

  protected static StringBuilder toRemoteDb(StringBuilder builder,
                                            String remoteDbPath,
                                            String remoteDbType) {
    if((remoteDbPath != null) || (remoteDbType != null)) {
      // note, always include path string, even if empty
      builder.append(" IN '");
      if(remoteDbPath != null) {
        builder.append(remoteDbPath);
      }
      builder.append('\'');
      if(remoteDbType != null) {
        builder.append(" [").append(remoteDbType).append(']');
      }
    }
    return builder;
  }

  protected static StringBuilder toAlias(StringBuilder builder,
                                         String alias) {
    if(alias != null) {
      toOptionalQuotedExpr(builder.append(" AS "), alias, false);
    }
    return builder;
  }

  private static final class UnknownQuery extends Query
  {
    private final int _objectFlag;

    private UnknownQuery(String name, List<Row> rows, int objectId, 
                         int objectFlag) 
    {
      super(name, rows, objectId, Type.UNKNOWN);
      _objectFlag = objectFlag;
    }

    @Override
    public int getObjectFlag() {
      return _objectFlag;
    }

    @Override
    protected void toSQLString(StringBuilder builder) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Struct containing the information from a single row of the system query
   * table.
   */
  public static final class Row
  {
    public final Byte attribute;
    public final String expression;
    public final Short flag;
    public final Integer extra;
    public final String name1;
    public final String name2;
    public final Integer objectId;
    public final byte[] order;

    public Row(Map<String,Object> tableRow) {
      this((Byte)tableRow.get(COL_ATTRIBUTE),
           (String)tableRow.get(COL_EXPRESSION),
           (Short)tableRow.get(COL_FLAG),
           (Integer)tableRow.get(COL_EXTRA),
           (String)tableRow.get(COL_NAME1),
           (String)tableRow.get(COL_NAME2),
           (Integer)tableRow.get(COL_OBJECTID),
           (byte[])tableRow.get(COL_ORDER));
    }

    public Row(Byte attribute, String expression, Short flag,
               Integer extra, String name1, String name2,
               Integer objectId, byte[] order)
    {
      this.attribute = attribute;
      this.expression = expression;
      this.flag = flag;
      this.extra = extra;
      this.name1 = name1;
      this.name2= name2;
      this.objectId = objectId;
      this.order = order;
    }

    public Map<String,Object> toTableRow()
    {
      Map<String,Object> tableRow = new LinkedHashMap<String,Object>();

      tableRow.put(COL_ATTRIBUTE, attribute);
      tableRow.put(COL_EXPRESSION, expression);
      tableRow.put(COL_FLAG, flag);
      tableRow.put(COL_EXTRA, extra);
      tableRow.put(COL_NAME1, name1);
      tableRow.put(COL_NAME2, name2);
      tableRow.put(COL_OBJECTID, objectId);
      tableRow.put(COL_ORDER, order);

      return tableRow;
    }

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this);
    }
  }

  protected static abstract class RowFormatter
  {
    private final List<Row> _list;

    protected RowFormatter(List<Row> list) {
      _list = list;
    }

    public List<String> format() {
      return format(new AppendableList<String>());
    }

    public List<String> format(List<String> strs) {
      for(Row row : _list) {
        StringBuilder builder = new StringBuilder();
        format(builder, row);
        strs.add(builder.toString());
      }
      return strs;
    }

    protected abstract void format(StringBuilder builder, Row row);
  }

  protected static abstract class RowFilter
  {
    protected RowFilter() {
    }

    public List<Row> filter(List<Row> list) {
      for(Iterator<Row> iter = list.iterator(); iter.hasNext(); ) {
        if(!keep(iter.next())) {
          iter.remove();
        }
      }
      return list;
    }

    protected abstract boolean keep(Row row);
  }

  protected static class AppendableList<E> extends ArrayList<E>
  {
    private static final long serialVersionUID = 0L;

    protected AppendableList() {
    }

    protected AppendableList(Collection<? extends E> c) {
      super(c);
    }

    protected String getSeparator() {
      return ", ";
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      for(Iterator<E> iter = iterator(); iter.hasNext(); ) {
        builder.append(iter.next().toString());
        if(iter.hasNext()) {
          builder.append(getSeparator());
        }
      }
      return builder.toString();
    }
  }

  private static final class Join
  {
    public final List<String> tables = new ArrayList<String>();
    public boolean isJoin;
    public String expression;

    private Join(String table, String expr) {
      tables.add(table);
      expression = expr;
    }

    public void join(Join other, String newExpr) {
      tables.addAll(other.tables);
      isJoin = true;
      expression = newExpr;
    }

    @Override
    public String toString() {
      return (isJoin ? ("(" + expression + ")") : expression);
    }
  }

}
