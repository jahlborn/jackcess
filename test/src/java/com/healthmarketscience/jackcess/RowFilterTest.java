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

package com.healthmarketscience.jackcess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.DatabaseTest.*;

/**
 * @author James Ahlborn
 */
public class RowFilterTest extends TestCase 
{
  private static final String ID_COL = "id";
  private static final String COL1 = "col1";
  private static final String COL2 = "col2";
  private static final String COL3 = "col3";
  

  public RowFilterTest(String name) {
    super(name);
  }

  @SuppressWarnings("unchecked")
  public void testFilter() throws Exception 
  {
    Map<String,Object> row0 = createExpectedRow(ID_COL, 0, COL1, "foo", COL2, 13, COL3, "bar");
    Map<String,Object> row1 = createExpectedRow(ID_COL, 1, COL1, "bar", COL2, 42, COL3, null);
    Map<String,Object> row2 = createExpectedRow(ID_COL, 2, COL1, "foo", COL2, 55, COL3, "bar");
    Map<String,Object> row3 = createExpectedRow(ID_COL, 3, COL1, "baz", COL2, 42, COL3, "bar");
    Map<String,Object> row4 = createExpectedRow(ID_COL, 4, COL1, "foo", COL2, 13, COL3, null);
    Map<String,Object> row5 = createExpectedRow(ID_COL, 5, COL1, "bla", COL2, 13, COL3, "bar");


    List<Map<String,Object>> rows = Arrays.asList(row0, row1, row2, row3, row4, row5);

    ColumnImpl testCol = new ColumnImpl(null, DataType.TEXT, 0, 0, 0);
    testCol.setName(COL1);
    assertEquals(Arrays.asList(row0, row2, row4), 
                 toList(RowFilter.matchPattern(testCol,
                            "foo").apply(rows)));
    assertEquals(Arrays.asList(row1, row3, row5), 
                 toList(RowFilter.invert(
                            RowFilter.matchPattern(
                                testCol,
                                "foo")).apply(rows)));

    assertEquals(Arrays.asList(row0, row2, row4), 
                 toList(RowFilter.matchPattern(
                            createExpectedRow(COL1, "foo"))
                        .apply(rows)));
    assertEquals(Arrays.asList(row0, row2), 
                 toList(RowFilter.matchPattern(
                            createExpectedRow(COL1, "foo", COL3, "bar"))
                        .apply(rows)));
    assertEquals(Arrays.asList(row4), 
                 toList(RowFilter.matchPattern(
                            createExpectedRow(COL1, "foo", COL3, null))
                        .apply(rows)));
    assertEquals(Arrays.asList(row0, row4, row5), 
                 toList(RowFilter.matchPattern(
                            createExpectedRow(COL2, 13))
                        .apply(rows)));
    assertEquals(Arrays.asList(row1), 
                 toList(RowFilter.matchPattern(row1)
                        .apply(rows)));

    assertEquals(rows, toList(RowFilter.apply(null, rows)));
    assertEquals(Arrays.asList(row1), 
                 toList(RowFilter.apply(RowFilter.matchPattern(row1),
                                        rows)));
  }

  static List<Map<String,Object>> toList(Iterable<Map<String,Object>> rows)
  {
    List<Map<String,Object>> rowList = new ArrayList<Map<String,Object>>();
    for(Map<String,Object> row : rows) {
      rowList.add(row);
    }
    return rowList;
  }

}
