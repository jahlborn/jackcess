/*
Copyright (c) 2007 Health Market Science, Inc.

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
import java.util.Iterator;
import java.util.List;

import com.healthmarketscience.jackcess.query.Query.Row;
import junit.framework.TestCase;
import org.apache.commons.lang.StringUtils;

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static com.healthmarketscience.jackcess.query.QueryFormat.*;


/**
 * @author James Ahlborn
 */
public class QueryTest extends TestCase
{

  public QueryTest(String name) throws Exception {
    super(name);
  }

  public void testUnionQuery() throws Exception
  {
    String expr1 = "Select * from Table1";
    String expr2 = "Select * from Table2";

    UnionQuery query = (UnionQuery)newQuery(
        Query.Type.UNION, 
        newRow(TABLE_ATTRIBUTE, expr1, null, UNION_PART1),
        newRow(TABLE_ATTRIBUTE, expr2, null, UNION_PART2));
    setFlag(query, 3);

    assertEquals(multiline("Select * from Table1",
                           "UNION Select * from Table2;"), 
                 query.toSQLString());

    setFlag(query, 1);

    assertEquals(multiline("Select * from Table1",
                           "UNION ALL Select * from Table2;"), 
                 query.toSQLString());

    query.getRows().add(newRow(ORDERBY_ATTRIBUTE, "Table1.id", 
                               null, null));

    assertEquals(multiline("Select * from Table1",
                           "UNION ALL Select * from Table2",
                           "ORDER BY Table1.id;"),
                 query.toSQLString());

  }

  public void testPassthroughQuery() throws Exception
  {
    String expr = "Select * from Table1";
    String constr = "ODBC;";

    PassthroughQuery query = (PassthroughQuery)newQuery(
        Query.Type.PASSTHROUGH, expr, constr);

    assertEquals(expr, query.toSQLString());
    assertEquals(constr, query.getConnectionString());
  }

  public void testDataDefinitionQuery() throws Exception
  {
    String expr = "Drop table Table1";

    DataDefinitionQuery query = (DataDefinitionQuery)newQuery(
        Query.Type.DATA_DEFINITION, expr, null);

    assertEquals(expr, query.toSQLString());
  }

  public void testUpdateQuery() throws Exception
  {
    UpdateQuery query = (UpdateQuery)newQuery(
        Query.Type.UPDATE, 
        newRow(TABLE_ATTRIBUTE, null, "Table1", null),
        newRow(COLUMN_ATTRIBUTE, "\"some string\"", null, "Table1.id"),
        newRow(COLUMN_ATTRIBUTE, "42", null, "Table1.col1"));

    assertEquals(
        multiline("UPDATE Table1",
                  "SET Table1.id = \"some string\", Table1.col1 = 42;"),
        query.toSQLString());

    query.getRows().add(newRow(WHERE_ATTRIBUTE, "(Table1.col2 < 13)", 
                               null, null));

    assertEquals(
        multiline("UPDATE Table1",
                  "SET Table1.id = \"some string\", Table1.col1 = 42",
                  "WHERE (Table1.col2 < 13);"),
        query.toSQLString());
  }

  private static Query newQuery(Query.Type type, Row... rows)
  {
    return newQuery(type, null, null, rows);
  }

  private static Query newQuery(Query.Type type, String typeExpr,
                                String typeName1, Row... rows)
  {
    List<Row> rowList = new ArrayList<Row>();
    rowList.add(newRow(TYPE_ATTRIBUTE, typeExpr, type.getValue(),
                       null, typeName1, null));
    rowList.addAll(Arrays.asList(rows));
    return Query.create(type.getObjectFlag(), "TestQuery", rowList, 13);
  }

  private static Row newRow(Byte attr, String expr, String name1, String name2)
  {
    return newRow(attr, expr, null, null, name1, name2);
  }

  private static Row newRow(Byte attr, String expr, Number flagNum,
                            String name1, String name2)
  {
    return newRow(attr, expr, flagNum, null, name1, name2);
  }

  private static Row newRow(Byte attr, String expr, Number flagNum,
                            Number extraNum, String name1, String name2)
  {
    Short flag = ((flagNum != null) ? flagNum.shortValue() : null);
    Integer extra = ((extraNum != null) ? extraNum.intValue() : null);
    return new Row(attr, expr, flag, extra, name1, name2, null, null);
  }

  private static void setFlag(Query query, Number newFlagNum)
  {
    removeRows(query, FLAG_ATTRIBUTE);
    query.getRows().add(
        newRow(FLAG_ATTRIBUTE, null, newFlagNum, null, null, null));
  }

  private static void removeRows(Query query, Byte attr)
  {
    for(Iterator<Row> iter = query.getRows().iterator(); iter.hasNext(); ) {
      if(attr.equals(iter.next().attribute)) {
        iter.remove();
      }
    }
  }

  private static String multiline(String... strs)
  {
    return StringUtils.join(strs, LINE_SEPARATOR);
  }

}
