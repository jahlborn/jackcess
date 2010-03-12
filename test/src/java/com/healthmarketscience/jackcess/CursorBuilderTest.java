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

package com.healthmarketscience.jackcess;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.JetFormatTest.*;

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
    assertSame(expected.getIndex(), found.getIndex());

    assertEquals(expected.getSavepoint().getCurrentPosition(),
                 found.getSavepoint().getCurrentPosition());
  }
  
  public void test() throws Exception
  {
    for (final TestDB indexCursorDB : CursorTest.INDEX_CURSOR_DBS) {
      Database db = CursorTest.createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor expected = Cursor.createCursor(table);

      Cursor found = new CursorBuilder(table).toCursor();
      assertCursor(expected, found);

      expected = Cursor.createIndexCursor(table, idx);
      found = new CursorBuilder(table)
        .setIndex(idx)
        .toCursor();
      assertCursor(expected, found);

      expected = Cursor.createIndexCursor(table, idx);
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

      expected = Cursor.createIndexCursor(table, idx);
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

      expected = Cursor.createCursor(table);
      expected.beforeFirst();
      found = new CursorBuilder(table)
        .beforeFirst()
        .toCursor();
      assertCursor(expected, found);

      expected = Cursor.createCursor(table);
      expected.afterLast();
      found = new CursorBuilder(table)
        .afterLast()
        .toCursor();
      assertCursor(expected, found);

      expected = Cursor.createCursor(table);
      expected.moveNextRows(2);
      Cursor.Savepoint sp = expected.getSavepoint();
      found = new CursorBuilder(table)
        .afterLast()
        .restoreSavepoint(sp)
        .toCursor();
      assertCursor(expected, found);

      expected = Cursor.createIndexCursor(table, idx);
      expected.moveNextRows(2);
      sp = expected.getSavepoint();
      found = new CursorBuilder(table)
        .setIndex(idx)
        .beforeFirst()
        .restoreSavepoint(sp)
        .toCursor();
      assertCursor(expected, found);

      expected = Cursor.createIndexCursor(table, idx,
                                          idx.constructIndexRowFromEntry(3),
                                          null);
      found = new CursorBuilder(table)
        .setIndex(idx)
        .setStartEntry(3)
        .toCursor();
      assertCursor(expected, found);

      expected = Cursor.createIndexCursor(table, idx,
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
