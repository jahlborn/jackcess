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
 * Concrete Query subclass which represents a UNION query, e.g.:
 * {@code SELECT <query1> UNION SELECT <query2>}
 * 
 * @author James Ahlborn
 */
public class UnionQuery extends Query 
{
  public UnionQuery(String name, List<Row> rows, int objectId) {
    super(name, rows, objectId, Type.UNION);
  }

  public String getUnionType() {
    return(hasFlag(UNION_FLAG) ? DEFAULT_TYPE : "ALL");
  }

  public String getUnionString1() {
    return getUnionString(UNION_PART1);
  }

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
