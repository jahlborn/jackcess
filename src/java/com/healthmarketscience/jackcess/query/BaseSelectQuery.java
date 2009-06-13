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

import java.util.List;

import static com.healthmarketscience.jackcess.query.QueryFormat.*;


/**
 * Base class for queries which represent some form of SELECT statement.
 * 
 * @author James Ahlborn
 */
public abstract class BaseSelectQuery extends Query 
{

  protected BaseSelectQuery(String name, List<Row> rows, int objectId,
                            Type type) {
    super(name, rows, objectId, type);
  }

  protected void toSQLSelectString(StringBuilder builder,
                                   boolean useSelectPrefix) 
  {
    if(useSelectPrefix) {
      builder.append("SELECT ");
      String selectType = getSelectType();
      if(!DEFAULT_TYPE.equals(selectType)) {
        builder.append(selectType).append(' ');
      }
    }

    builder.append(getSelectColumns());
    toSelectInto(builder);

    List<String> fromTables = getFromTables();
    if(!fromTables.isEmpty()) {
      builder.append(NEWLINE).append("FROM ").append(fromTables);
      toRemoteDb(builder, getFromRemoteDbPath(), getFromRemoteDbType());
    }

    String whereExpr = getWhereExpression();
    if(whereExpr != null) {
      builder.append(NEWLINE).append("WHERE ").append(whereExpr);
    }

    List<String> groupings = getGroupings();
    if(!groupings.isEmpty()) {
      builder.append(NEWLINE).append("GROUP BY ").append(groupings);
    }

    String havingExpr = getHavingExpression();
    if(havingExpr != null) {
      builder.append(NEWLINE).append("HAVING ").append(havingExpr);
    }

    List<String> orderings = getOrderings();
    if(!orderings.isEmpty()) {
      builder.append(NEWLINE).append("ORDER BY ").append(orderings);
    }
  }

  public String getSelectType()
  {
    if(hasFlag(DISTINCT_SELECT_TYPE)) {
      return "DISTINCT";
    }

    if(hasFlag(DISTINCT_ROW_SELECT_TYPE)) {
      return "DISTINCTROW";
    }

    if(hasFlag(TOP_SELECT_TYPE)) {
      StringBuilder builder = new StringBuilder();
      builder.append("TOP ").append(getFlagRow().name1);
      if(hasFlag(PERCENT_SELECT_TYPE)) {
        builder.append(" PERCENT");
      }
      return builder.toString();
    }

    return DEFAULT_TYPE;
  }

  public List<String> getSelectColumns() 
  {
    List<String> result = (new RowFormatter(getColumnRows()) {
        @Override protected void format(StringBuilder builder, Row row) {
          // note column expression are always quoted appropriately
          builder.append(row.expression);
          toAlias(builder, row.name1);
        }
      }).format();
    if(hasFlag(SELECT_STAR_SELECT_TYPE)) {
      result.add("*");
    }
    return result;
  }

  protected void toSelectInto(StringBuilder builder)
  {
    // base does nothing
  }

  @Override
  public List<String> getFromTables() 
  {
    return super.getFromTables();
  }

  @Override
  public String getFromRemoteDbPath() 
  {
    return super.getFromRemoteDbPath();
  }

  @Override
  public String getFromRemoteDbType() 
  {
    return super.getFromRemoteDbType();
  }

  @Override
  public String getWhereExpression()
  {
    return super.getWhereExpression();
  }

  public List<String> getGroupings() 
  {
    return (new RowFormatter(getGroupByRows()) {
        @Override protected void format(StringBuilder builder, Row row) {
          builder.append(row.expression);
        }
      }).format();
  }

  public String getHavingExpression()
  {
    return getHavingRow().expression;
  }

  @Override
  public List<String> getOrderings() 
  {
    return super.getOrderings();
  }
  
}
