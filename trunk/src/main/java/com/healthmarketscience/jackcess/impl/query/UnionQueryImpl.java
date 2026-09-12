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
import com.healthmarketscience.jackcess.query.UnionQuery;


/**
 * Concrete Query subclass which represents a UNION query, e.g.:
 * {@code SELECT <query1> UNION SELECT <query2>}
 * 
 * @author James Ahlborn
 */
public class UnionQueryImpl extends QueryImpl implements UnionQuery
{
  public UnionQueryImpl(String name, List<Row> rows, int objectId, 
                        int objectFlag) {
    super(name, rows, objectId, objectFlag, Type.UNION);
  }

  @Override
  public String getUnionType() {
    return(hasFlag(UNION_FLAG) ? DEFAULT_TYPE : "ALL");
  }

  @Override
  public String getUnionString1() {
    return getUnionString(UNION_PART1);
  }

  @Override
  public String getUnionString2() {
    return getUnionString(UNION_PART2);
  }

  @Override
  public List<String> getOrderings() {
    return super.getOrderings();
  }

  private String getUnionString(String id) {
    for(Row row : getTableRows()) {
      if(id.equals(row.name2)) {
        return cleanUnionString(row.expression);
      }
    }
    throw new IllegalStateException(
        "Could not find union query with id " + id);
  }

  @Override
  protected void toSQLString(StringBuilder builder)
  {
    builder.append(getUnionString1()).append(NEWLINE)
      .append("UNION ");
    String unionType = getUnionType();
    if(!DEFAULT_TYPE.equals(unionType)) {
      builder.append(unionType).append(' ');
    }
    builder.append(getUnionString2());
    List<String> orderings = getOrderings();
    if(!orderings.isEmpty()) {
      builder.append(NEWLINE).append("ORDER BY ").append(orderings);
    }
  }

  private static String cleanUnionString(String str)
  {
    return str.trim().replaceAll("[\r\n]+", NEWLINE);
  }

}
