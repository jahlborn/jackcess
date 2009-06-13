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
 * Concrete Query subclass which represents an append query, e.g.:
 * {@code INSERT INTO <table> VALUES (<values>)}
 * 
 * @author James Ahlborn
 */
public class AppendQuery extends BaseSelectQuery 
{

  public AppendQuery(String name, List<Row> rows, int objectId) {
    super(name, rows, objectId, Type.APPEND);
  }

  public String getTargetTable() {
    return getTypeRow().name1;
  }

  public String getRemoteDbPath() {
    return getTypeRow().name2;
  }

  public String getRemoteDbType() {
    return getTypeRow().expression;
  }

  protected List<Row> getValueRows() {
    return filterRowsByFlag(super.getColumnRows(), APPEND_VALUE_FLAG);
  }

  @Override
  protected List<Row> getColumnRows() {
    return filterRowsByNotFlag(super.getColumnRows(), APPEND_VALUE_FLAG);
  }

  public List<String> getValues() {
    return new RowFormatter(getValueRows()) {
        @Override protected void format(StringBuilder builder, Row row) {
          builder.append(row.expression);
        }
      }.format();
  }

  @Override
  protected void toSQLString(StringBuilder builder)
  {
    builder.append("INSERT INTO ").append(getTargetTable());
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
