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
 * Concrete Query subclass which represents a crosstab/pivot query, e.g.:
 * {@code TRANSFORM <expr> SELECT <query> PIVOT <expr>}
 * 
 * @author James Ahlborn
 */
public class CrossTabQuery extends BaseSelectQuery 
{

  public CrossTabQuery(String name, List<Row> rows, int objectId) {
    super(name, rows, objectId, Type.CROSS_TAB);
  }

  protected Row getTransformRow() {
    return getUniqueRow(
        filterRowsByNotFlag(super.getColumnRows(), 
                            (short)(CROSSTAB_PIVOT_FLAG | 
                                    CROSSTAB_NORMAL_FLAG)));
  }

  @Override
  protected List<Row> getColumnRows() {
    return filterRowsByFlag(super.getColumnRows(), CROSSTAB_NORMAL_FLAG);
  }

  @Override
  protected List<Row> getGroupByRows() {
    return filterRowsByFlag(super.getGroupByRows(), CROSSTAB_NORMAL_FLAG);
  }

  protected Row getPivotRow() {
    return getUniqueRow(filterRowsByFlag(super.getColumnRows(), 
                                         CROSSTAB_PIVOT_FLAG));
  }

  public String getTransformExpression() {
    Row row = getTransformRow();
    if(row.expression == null) {
      return null;
    }
    // note column expression are always quoted appropriately
    StringBuilder builder = new StringBuilder(row.expression);
    return toAlias(builder, row.name1).toString();
  }

  public String getPivotExpression() {
    return getPivotRow().expression;
  }

  @Override
  protected void toSQLString(StringBuilder builder)
  {
    String transformExpr = getTransformExpression();
    if(transformExpr != null) {
      builder.append("TRANSFORM ").append(transformExpr).append(NEWLINE);
    }

    toSQLSelectString(builder, true);

    builder.append(NEWLINE).append("PIVOT ")
      .append(getPivotExpression());
  }

}
