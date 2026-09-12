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

import java.util.List;

import static com.healthmarketscience.jackcess.impl.query.QueryFormat.*;
import com.healthmarketscience.jackcess.query.UpdateQuery;


/**
 * Concrete Query subclass which represents a row update query, e.g.:
 * {@code UPDATE <table> SET <newValues>}
 * 
 * @author James Ahlborn
 */
public class UpdateQueryImpl extends QueryImpl implements UpdateQuery
{

  public UpdateQueryImpl(String name, List<Row> rows, int objectId, 
                         int objectFlag) {
    super(name, rows, objectId, objectFlag, Type.UPDATE);
  }

  @Override
  public List<String> getTargetTables() 
  {
    return super.getFromTables();
  }

  @Override
  public String getRemoteDbPath() 
  {
    return super.getFromRemoteDbPath();
  }

  @Override
  public String getRemoteDbType() 
  {
    return super.getFromRemoteDbType();
  }

  @Override
  public List<String> getNewValues()
  {
    return (new RowFormatter(getColumnRows()) {
        @Override protected void format(StringBuilder builder, Row row) {
          toOptionalQuotedExpr(builder, row.name2, true)
            .append(" = ").append(row.expression);
        }
      }).format();
  }

  @Override
  public String getWhereExpression()
  {
    return super.getWhereExpression();
  }

  @Override
  protected void toSQLString(StringBuilder builder)
  {
    builder.append("UPDATE ").append(getTargetTables());
    toRemoteDb(builder, getRemoteDbPath(), getRemoteDbType());

    builder.append(NEWLINE).append("SET ").append(getNewValues());

    String whereExpr = getWhereExpression();
    if(whereExpr != null) {
      builder.append(NEWLINE).append("WHERE ").append(whereExpr);
    }
  }  

}
