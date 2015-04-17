/*
Copyright (c) 2007 Health Market Science, Inc.

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

package com.healthmarketscience.jackcess;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.IndexImpl;

/**
 * @author James Ahlborn
 */
public class CursorBuilderTest extends TestCase {

  public CursorBuilderTest(String name) throws Exception {
    super(name);
  }

  private static void assertCursor(
      Cursor expected, Cursor found)
  {
    assertSame(expected.getTable(), found.getTable());
    if(expected instanceof IndexCursor) {
      assertSame(((IndexCursor)expected).getIndex(), 
                 ((IndexCursor)found).getIndex());
    }

    assertEquals(expected.getSavepoint().getCurrentPosition(),
                 found.getSavepoint().getCurrentPosition());
  }
  
  public void test() throws Exception
  {
    for (final TestDB indexCursorDB : CursorTest.INDEX_CURSOR_DBS) {
      Database db = CursorTest.createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      IndexImpl idx = (IndexImpl)table.getIndexes().get(0);

      Cursor expected = CursorBuilder.createCursor(table);

      Cursor found = new CursorBuilder(table).toCursor();
      assertCursor(expected, found);

      expected = CursorBuilder.createCursor(idx);
      found = new CursorBuilder(table)
        .setIndex(idx)
        .toCursor();
      assertCursor(expected, found);

      expected = CursorBuilder.createCursor(idx);
      found = new CursorBuilder(table)
        .setIndexByName("id")
        .toCursor();
      assertCursor(expected, found);

      try {
        new CursorBuilder(table)
          .setIndexByName("foo");
        fail("IllegalArgumentException should have been thrown");
      } catch(IllegalArgumentException ignored) {
        // success
      }

      expected = CursorBuilder.createCursor(idx);
      found = new CursorBuilder(table)
        .setIndexByColumns(table.getColumn("id"))
        .toCursor();
      assertCursor(expected, found);

      try {
        new CursorBuilder(table)
          .setIndexByColumns(table.getColumn("value"));
        fail("IllegalArgumentException should have been thrown");
      } catch(IllegalArgumentException ignored) {
        // success
      }

      try {
        new CursorBuilder(table)
          .setIndexByColumns(table.getColumn("id"), table.getColumn("value"));
        fail("IllegalArgumentException should have been thrown");
      } catch(IllegalArgumentException ignored) {
        // success
      }

      expected = CursorBuilder.createCursor(table);
      expected.beforeFirst();
      found = new CursorBuilder(table)
        .beforeFirst()
        .toCursor();
      assertCursor(expected, found);

      expected = CursorBuilder.createCursor(table);
      expected.afterLast();
      found = new CursorBuilder(table)
        .afterLast()
        .toCursor();
      assertCursor(expected, found);

      expected = CursorBuilder.createCursor(table);
      expected.moveNextRows(2);
      Cursor.Savepoint sp = expected.getSavepoint();
      found = new CursorBuilder(table)
        .afterLast()
        .restoreSavepoint(sp)
        .toCursor();
      assertCursor(expected, found);

      expected = CursorBuilder.createCursor(idx);
      expected.moveNextRows(2);
      sp = expected.getSavepoint();
      found = new CursorBuilder(table)
        .setIndex(idx)
        .beforeFirst()
        .restoreSavepoint(sp)
        .toCursor();
      assertCursor(expected, found);

      expected = CursorBuilder.createCursor(idx,
                                          idx.constructIndexRowFromEntry(3),
                                          null);
      found = new CursorBuilder(table)
        .setIndex(idx)
        .setStartEntry(3)
        .toCursor();
      assertCursor(expected, found);

      expected = CursorBuilder.createCursor(idx,
                                          idx.constructIndexRowFromEntry(3),
                                          false,
                                          idx.constructIndexRowFromEntry(7),
                                          false);
      found = new CursorBuilder(table)
        .setIndex(idx)
        .setStartEntry(3)
        .setStartRowInclusive(false)
        .setEndEntry(7)
        .setEndRowInclusive(false)
        .toCursor();
      assertCursor(expected, found);



      db.close();
    }
  }
  
}
