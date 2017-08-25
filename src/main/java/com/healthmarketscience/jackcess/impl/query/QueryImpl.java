/*
Copyright (c) 2008 Health Market Science, Inc.

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

package com.healthmarketscience.jackcess.impl.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.impl.RowImpl;
import static com.healthmarketscience.jackcess.impl.query.QueryFormat.*;
import com.healthmarketscience.jackcess.query.Query;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Base class for classes which encapsulate information about an Access query.
 * The {@link #toSQLString()} method can be used to convert this object into
 * the actual SQL string which this query data represents.
 * 
 * @author James Ahlborn
 */
public abstract class QueryImpl implements Query
{
  protected static final Log LOG = LogFactory.getLog(QueryImpl.class);  

  private static final Row EMPTY_ROW = new Row();

  private final String _name;
  private final List<Row> _rows;
  private final int _objectId;
  private final Type _type;
  private final int _objectFlag;

  protected QueryImpl(String name, List<Row> rows, int objectId, int objectFlag, 
                      Type type) 
  {
    _name = name;
    _rows = rows;
    _objectId = objectId;
    _type = type;
    _objectFlag = objectFlag;

    if(type != Type.UNKNOWN) {
      short foundType = getShortValue(getQueryType(rows),
                                      _type.getValue());
      if(foundType != _type.getValue()) {
        throw new IllegalStateException(withErrorContext(
                "Unexpected query type " + foundType));
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

  public boolean isHidden() {
    return((_objectFlag & DatabaseImpl.HIDDEN_OBJECT_FLAG) != 0);
  }

  /**
   * Returns the unique object id of the query.
   */
  public int getObjectId() {
    return _objectId;
  }

  public int getObjectFlag() {
    return _objectFlag;
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

  public Row getTypeRow() {
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
            throw new IllegalStateException(withErrorContext(
                "Unknown param type " + row.flag));
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
    // grab the list of query tables
    List<TableSource> tableExprs = new ArrayList<TableSource>();
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
      tableExprs.add(new SimpleTable(key, builder.toString()));
    }

    // combine the tables with any query joins
    List<Row> joins = getJoinRows();
    for(Row joinRow : joins) {
        
      String fromTable = joinRow.name1;
      String toTable = joinRow.name2;

      TableSource fromTs = null;
      TableSource toTs = null;

      // combine existing join expressions containing the target tables
      for(Iterator<TableSource> joinIter = tableExprs.iterator(); 
          (joinIter.hasNext() && ((fromTs == null) || (toTs == null))); ) {
        TableSource ts = joinIter.next();

        if((fromTs == null) && ts.containsTable(fromTable)) {
          fromTs = ts;

          // special case adding expr to existing join
          if((toTs == null) && ts.containsTable(toTable)) {
            toTs = ts;
            break;
          }

          joinIter.remove();

        } else if((toTs == null) && ts.containsTable(toTable)) {

          toTs = ts;
          joinIter.remove();
        }
      }

      if(fromTs == null) {
        fromTs = new SimpleTable(fromTable);
      }
      if(toTs == null) {
        toTs = new SimpleTable(toTable);
      }

      if(fromTs == toTs) {

        if(fromTs.sameJoin(joinRow.flag, joinRow.expression)) {
          // easy-peasy, we just added the join expression to existing join,
          // nothing more to do
          continue;
        }

        throw new IllegalStateException(withErrorContext(
                "Inconsistent join types for " + fromTable + " and " + toTable));
      }

      // new join expression
      tableExprs.add(new Join(fromTs, toTs, joinRow.flag, joinRow.expression));
    }

    // convert join objects to SQL strings
    List<String> result = new AppendableList<String>();
    for(TableSource ts : tableExprs) {
      result.add(ts.toString());
    }

    return result;
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
  public static QueryImpl create(int objectFlag, String name, List<Row> rows, 
                                 int objectId)
  {
    // remove other object flags before testing for query type
    int objTypeFlag = objectFlag & OBJECT_FLAG_MASK;

    if(objTypeFlag == 0) {
      // sometimes the query rows tell a different story
      short rowTypeFlag = getShortValue(getQueryType(rows), objTypeFlag);
      Type rowType = TYPE_MAP.get(rowTypeFlag);
      if((rowType != null) && (rowType.getObjectFlag() != objTypeFlag)) {
        // use row type instead of object flag type
        objTypeFlag = rowType.getObjectFlag();
      }
    }

    try {
      switch(objTypeFlag) {
      case SELECT_QUERY_OBJECT_FLAG:
        return new SelectQueryImpl(name, rows, objectId, objectFlag);
      case MAKE_TABLE_QUERY_OBJECT_FLAG:
        return new MakeTableQueryImpl(name, rows, objectId, objectFlag);
      case APPEND_QUERY_OBJECT_FLAG:
        return new AppendQueryImpl(name, rows, objectId, objectFlag);
      case UPDATE_QUERY_OBJECT_FLAG:
        return new UpdateQueryImpl(name, rows, objectId, objectFlag);
      case DELETE_QUERY_OBJECT_FLAG:
        return new DeleteQueryImpl(name, rows, objectId, objectFlag);
      case CROSS_TAB_QUERY_OBJECT_FLAG:
        return new CrossTabQueryImpl(name, rows, objectId, objectFlag);
      case DATA_DEF_QUERY_OBJECT_FLAG:
        return new DataDefinitionQueryImpl(name, rows, objectId, objectFlag);
      case PASSTHROUGH_QUERY_OBJECT_FLAG:
        return new PassthroughQueryImpl(name, rows, objectId, objectFlag);
      case UNION_QUERY_OBJECT_FLAG:
        return new UnionQueryImpl(name, rows, objectId, objectFlag);
      default:
        // unknown querytype
        throw new IllegalStateException(withErrorContext(
                "unknown query object flag " + objTypeFlag, name));
      }
    } catch(IllegalStateException e) {
      LOG.warn(withErrorContext("Failed parsing query", name), e);
    }

    // return unknown query
    return new UnknownQueryImpl(name, rows, objectId, objectFlag);
  }

  private static Short getQueryType(List<Row> rows)
  {
    return getFirstRowByAttribute(rows, TYPE_ATTRIBUTE).flag;
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

  private static Row getFirstRowByAttribute(List<Row> rows, Byte attribute) {
    for(Row row : rows) {
      if(attribute.equals(row.attribute)) {
        return row;
      }
    }
    return EMPTY_ROW;
  }

  protected Row getUniqueRow(List<Row> rows) {
    if(rows.size() == 1) {
      return rows.get(0);
    }
    if(rows.isEmpty()) {
      return EMPTY_ROW;
    }
    throw new IllegalStateException(withErrorContext(
            "Unexpected number of rows for" + rows));
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
    return (!isQuoted(expr) ? 
            builder.append('[').append(expr).append(']') :
            builder.append(expr));
  }

  protected static boolean isQuoted(String expr) {
    return ((expr.length() >= 2) && 
            (expr.charAt(0) == '[') && (expr.charAt(expr.length() - 1) == ']'));
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

  private String withErrorContext(String msg) {
    return withErrorContext(msg, getName());
  }

  private static String withErrorContext(String msg, String queryName) {
    return msg + " (Query: " + queryName + ")";
  }


  private static final class UnknownQueryImpl extends QueryImpl
  {
    private UnknownQueryImpl(String name, List<Row> rows, int objectId, 
                             int objectFlag) 
    {
      super(name, rows, objectId, objectFlag, Type.UNKNOWN);
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
    private final RowId _id;
    public final Byte attribute;
    public final String expression;
    public final Short flag;
    public final Integer extra;
    public final String name1;
    public final String name2;
    public final Integer objectId;
    public final byte[] order;

    private Row() {
      this._id = null;
      this.attribute = null;
      this.expression = null;
      this.flag = null;
      this.extra = null;
      this.name1 = null;
      this.name2= null;
      this.objectId = null;
      this.order = null;
    }

    public Row(com.healthmarketscience.jackcess.Row tableRow) {
      this(tableRow.getId(),
           tableRow.getByte(COL_ATTRIBUTE),
           tableRow.getString(COL_EXPRESSION),
           tableRow.getShort(COL_FLAG),
           tableRow.getInt(COL_EXTRA),
           tableRow.getString(COL_NAME1),
           tableRow.getString(COL_NAME2),
           tableRow.getInt(COL_OBJECTID),
           tableRow.getBytes(COL_ORDER));
    }

    public Row(RowId id, Byte attribute, String expression, Short flag,
               Integer extra, String name1, String name2,
               Integer objectId, byte[] order)
    {
      this._id = id;
      this.attribute = attribute;
      this.expression = expression;
      this.flag = flag;
      this.extra = extra;
      this.name1 = name1;
      this.name2= name2;
      this.objectId = objectId;
      this.order = order;
    }

    public com.healthmarketscience.jackcess.Row toTableRow()
    {
      com.healthmarketscience.jackcess.Row tableRow = new RowImpl((RowIdImpl)_id);

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

  /**
   * Base type of something which provides table data in a query
   */
  private static abstract class TableSource
  {
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb, true);
      return sb.toString();
    }

    protected abstract void toString(StringBuilder sb, boolean isTopLevel);

    public abstract boolean containsTable(String table);

    public abstract boolean sameJoin(short type, String on);
  }

  /**
   * Table data provided by a single table expression.
   */
  private static final class SimpleTable extends TableSource
  {
    private final String _tableName;
    private final String _tableExpr;

    private SimpleTable(String tableName) {
      this(tableName, toOptionalQuotedExpr(
               new StringBuilder(), tableName, true).toString());
    }

    private SimpleTable(String tableName, String tableExpr) {
      _tableName = tableName;
      _tableExpr = tableExpr;
    }

    @Override
    protected void toString(StringBuilder sb, boolean isTopLevel) {
      sb.append(_tableExpr);
    }

    @Override
    public boolean containsTable(String table) {
      return _tableName.equalsIgnoreCase(table);
    }

    @Override
    public boolean sameJoin(short type, String on) {
      return false;
    }
  }

  /**
   * Table data provided by a join expression.
   */
  private final class Join extends TableSource
  {
    private final TableSource _from;
    private final TableSource _to;
    private final short _jType;
    // combine all the join expressions with "AND"
    private final List<String> _on = new AppendableList<String>() {
      private static final long serialVersionUID = 0L;
      @Override
      protected String getSeparator() {
        return ") AND (";
      }
    };

    private Join(TableSource from, TableSource to, short type, String on) {
      _from = from;
      _to = to;
      _jType = type;
      _on.add(on);
    }

    @Override
    protected void toString(StringBuilder sb, boolean isTopLevel) {
      String joinType = JOIN_TYPE_MAP.get(_jType);
      if(joinType == null) {
        throw new IllegalStateException(withErrorContext(
                                            "Unknown join type " + _jType));
      }

      if(!isTopLevel) {
        sb.append("(");
      }

      _from.toString(sb, false);
      sb.append(joinType);
      _to.toString(sb, false);
      sb.append(" ON ");

      boolean multiOnExpr = (_on.size() > 1);
      if(multiOnExpr) {
        sb.append("(");
      }
      sb.append(_on);
      if(multiOnExpr) {
        sb.append(")");
      }

      if(!isTopLevel) {
        sb.append(")");
      }
    }

    @Override
    public boolean containsTable(String table) {
      return _from.containsTable(table) || _to.containsTable(table);
    }

    @Override
    public boolean sameJoin(short type, String on) {
      if(_jType == type) {
        // note, AND conditions are added in _reverse_ order
        _on.add(0, on);
        return true;
      }
      return false;
    }
  }
}
