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
import com.healthmarketscience.jackcess.query.MakeTableQuery;


/**
 * Concrete Query subclass which represents an table creation query, e.g.:
 * {@code SELECT <query> INTO <newTable>}
 * 
 * @author James Ahlborn
 */
public class MakeTableQueryImpl extends BaseSelectQueryImpl 
  implements MakeTableQuery
{

  public MakeTableQueryImpl(String name, List<Row> rows, int objectId, 
                            int objectFlag) {
    super(name, rows, objectId, objectFlag, Type.MAKE_TABLE);
  }

  @Override
  public String getTargetTable() {
    return getTypeRow().name1;
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
  protected void toSelectInto(StringBuilder builder) 
  {
    builder.append(" INTO ");
    toOptionalQuotedExpr(builder, getTargetTable(), true);
    toRemoteDb(builder, getRemoteDbPath(), getRemoteDbType());
  }

  @Override
  protected void toSQLString(StringBuilder builder)
  {
    toSQLSelectString(builder, true);
  }  

}
