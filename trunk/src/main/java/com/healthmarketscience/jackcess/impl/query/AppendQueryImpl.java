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
import com.healthmarketscience.jackcess.query.AppendQuery;


/**
 * Concrete Query subclass which represents an append query, e.g.:
 * {@code INSERT INTO <table> VALUES (<values>)}
 * 
 * @author James Ahlborn
 */
public class AppendQueryImpl extends BaseSelectQueryImpl implements AppendQuery
{

  public AppendQueryImpl(String name, List<Row> rows, int objectId, 
                         int objectFlag) {
    super(name, rows, objectId, objectFlag, Type.APPEND);
  }

  @Override
  public String getTargetTable() {
    return getTypeRow().name1;
  }

  @Override
  public List<String> getTargetColumns() {
    return new RowFormatter(getTargetRows()) {
        @Override protected void format(StringBuilder builder, Row row) {
          toOptionalQuotedExpr(builder, row.name2, true);
        }
      }.format();
  }

  @Override
  public String getRemoteDbPath() {
    return getTypeRow().name2;
  }

  @Override
  public String getRemoteDbType() {
    return getTypeRow().expression;
  }

  @Override
  public List<String> getValues() {
    return new RowFormatter(getValueRows()) {
        @Override protected void format(StringBuilder builder, Row row) {
          builder.append(row.expression);
        }
      }.format();
  }

  protected List<Row> getValueRows() {
    return filterRowsByFlag(super.getColumnRows(), APPEND_VALUE_FLAG);
  }

  @Override
  protected List<Row> getColumnRows() {
    return filterRowsByNotFlag(super.getColumnRows(), APPEND_VALUE_FLAG);
  }

  protected List<Row> getTargetRows() {
    return new RowFilter() {
        @Override protected boolean keep(Row row) {
          return (row.name2 != null);
        }      
    }.filter(super.getColumnRows());
  }

  @Override
  protected void toSQLString(StringBuilder builder)
  {
    builder.append("INSERT INTO ");
    toOptionalQuotedExpr(builder, getTargetTable(), true);
    List<String> columns = getTargetColumns();
    if(!columns.isEmpty()) {
      builder.append(" (").append(columns).append(')');
    }
    toRemoteDb(builder, getRemoteDbPath(), getRemoteDbType());
    builder.append(NEWLINE);
    List<String> values = getValues();
    if(!values.isEmpty()) {
      builder.append("VALUES (").append(values).append(')');
    } else {
      toSQLSelectString(builder, true);
    }
  }
  
}
