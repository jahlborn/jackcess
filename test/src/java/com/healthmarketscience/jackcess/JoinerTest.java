/*
Copyright (c) 2011 James Ahlborn

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
*/

package com.healthmarketscience.jackcess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.healthmarketscience.jackcess.DatabaseTest.*;
import static com.healthmarketscience.jackcess.JetFormatTest.*;
import junit.framework.TestCase;

/**
 *
 * @author James Ahlborn
 */
public class JoinerTest extends TestCase {

  public JoinerTest(String name) {
    super(name);
  }

  public void testJoiner() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX)) {

      Database db = open(testDB);
      Table t1 = db.getTable("Table1");
      Table t2 = db.getTable("Table2");
      Table t3 = db.getTable("Table3");

      Index t1t2 = t1.getIndex("Table2Table1");
      Index t1t3 = t1.getIndex("Table3Table1");

      Index t2t1 = t1t2.getReferencedIndex();
      assertSame(t2, t2t1.getTable());
      Joiner t2t1Join = Joiner.create(t2t1);

      assertSame(t2, t2t1Join.getFromTable());
      assertSame(t2t1, t2t1Join.getFromIndex());
      assertSame(t1, t2t1Join.getToTable());
      assertSame(t1t2, t2t1Join.getToIndex());
      
      doTestJoiner(t2t1Join, createT2T1Data());
      
      Index t3t1 = t1t3.getReferencedIndex();
      assertSame(t3, t3t1.getTable());
      Joiner t3t1Join = Joiner.create(t3t1);

      assertSame(t3, t3t1Join.getFromTable());
      assertSame(t3t1, t3t1Join.getFromIndex());
      assertSame(t1, t3t1Join.getToTable());
      assertSame(t1t3, t3t1Join.getToIndex());
      
      doTestJoiner(t3t1Join, createT3T1Data());      
    }    
  }

  private void doTestJoiner(Joiner join,
                            Map<Integer,List<Map<String,Object>>> expectedData)
    throws Exception
  {
    final Set<String> colNames = new HashSet<String>(
        Arrays.asList("id", "data"));

    Joiner revJoin = join.createReverse();
    for(Map<String,Object> row : join.getFromTable()) {
      Integer id = (Integer)row.get("id");

      List<Map<String,Object>> joinedRows =
        new ArrayList<Map<String,Object>>();
      for(Map<String,Object> t1Row : join.findRowsIterable(row)) {
        joinedRows.add(t1Row);
      }

      List<Map<String,Object>> expectedRows = expectedData.get(id);
      assertEquals(expectedData.get(id), joinedRows);

      if(!expectedRows.isEmpty()) {
        assertEquals(expectedRows.get(0), join.findFirstRow(row));

        assertEquals(row, revJoin.findFirstRow(expectedRows.get(0)));
      } else {
        assertNull(join.findFirstRow(row));
      }
      
      List<Map<String,Object>> expectedRows2 = new
        ArrayList<Map<String,Object>>();
      for(Map<String,Object> tmpRow : expectedRows) {
        Map<String,Object> tmpRow2 = new HashMap<String,Object>(tmpRow);
        tmpRow2.keySet().retainAll(colNames);
        expectedRows2.add(tmpRow2);
      }
      
      joinedRows = new ArrayList<Map<String,Object>>();
      for(Map<String,Object> t1Row : join.findRowsIterable(row, colNames)) {
        joinedRows.add(t1Row);
      }

      assertEquals(expectedRows2, joinedRows);

      if(!expectedRows2.isEmpty()) {
        assertEquals(expectedRows2.get(0), join.findFirstRow(row, colNames));
      } else {
        assertNull(join.findFirstRow(row, colNames));
      }      
    }
  }

  private static Map<Integer,List<Map<String,Object>>> createT2T1Data()
  {
    Map<Integer,List<Map<String,Object>>> data = new
      HashMap<Integer,List<Map<String,Object>>>();

    data.put(0,
             createExpectedTable(
                 createExpectedRow("id", 0, "otherfk1", 0, "otherfk2", 10,
                                   "data", "baz0", "otherfk3", 0)));

    data.put(1,
             createExpectedTable(
                 createExpectedRow("id", 1, "otherfk1", 1, "otherfk2", 11,
                                   "data", "baz11", "otherfk3", 0),
                 createExpectedRow("id", 2, "otherfk1", 1, "otherfk2", 11,
                                   "data", "baz11-2", "otherfk3", 0)));

    data.put(2,
             createExpectedTable(
                 createExpectedRow("id", 3, "otherfk1", 2, "otherfk2", 13,
                                   "data", "baz13", "otherfk3", 0)));

    return data;
  }
  
  private static Map<Integer,List<Map<String,Object>>> createT3T1Data()
  {
    Map<Integer,List<Map<String,Object>>> data = new
      HashMap<Integer,List<Map<String,Object>>>();

    data.put(10,
             createExpectedTable(
                 createExpectedRow("id", 0, "otherfk1", 0, "otherfk2", 10,
                                   "data", "baz0", "otherfk3", 0)));

    data.put(11,
             createExpectedTable(
                 createExpectedRow("id", 1, "otherfk1", 1, "otherfk2", 11,
                                   "data", "baz11", "otherfk3", 0),
                 createExpectedRow("id", 2, "otherfk1", 1, "otherfk2", 11,
                                   "data", "baz11-2", "otherfk3", 0)));

    data.put(12,
             createExpectedTable());

    data.put(13,
             createExpectedTable(
                 createExpectedRow("id", 3, "otherfk1", 2, "otherfk2", 13,
                                   "data", "baz13", "otherfk3", 0)));

    return data;
  }
  
}
