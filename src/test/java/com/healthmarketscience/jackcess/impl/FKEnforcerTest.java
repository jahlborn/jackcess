/*
Copyright (c) 2012 James Ahlborn

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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import static com.healthmarketscience.jackcess.DatabaseTest.*;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import junit.framework.TestCase;

/**
 *
 * @author James Ahlborn
 */
public class FKEnforcerTest extends TestCase
{

  public FKEnforcerTest(String name) throws Exception {
    super(name);
  }

  public void testNoEnforceForeignKeys() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX)) {

      Database db = openCopy(testDB);
      db.setEnforceForeignKeys(false);
      Table t1 = db.getTable("Table1");
      Table t2 = db.getTable("Table2");
      Table t3 = db.getTable("Table3");

      t1.addRow(20, 0, 20, "some data", 20);

      Cursor c = CursorBuilder.createCursor(t2);
      c.moveToNextRow();
      c.updateCurrentRow(30, "foo30");

      c = CursorBuilder.createCursor(t3);
      c.moveToNextRow();
      c.deleteCurrentRow();

      db.close();
    }
    
  }

  public void testEnforceForeignKeys() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX)) {

      Database db = openCopy(testDB);
      Table t1 = db.getTable("Table1");
      Table t2 = db.getTable("Table2");
      Table t3 = db.getTable("Table3");

      try {
        t1.addRow(20, 0, 20, "some data", 20);
        fail("IOException should have been thrown");
      } catch(IOException ignored) {
        // success
        assertTrue(ignored.getMessage().contains("Table1[otherfk2]"));
      }

      try {
        Cursor c = CursorBuilder.createCursor(t2);
        c.moveToNextRow();
        c.updateCurrentRow(30, "foo30");
        fail("IOException should have been thrown");
      } catch(IOException ignored) {
        // success
        assertTrue(ignored.getMessage().contains("Table2[id]"));
      }

      try {
        Cursor c = CursorBuilder.createCursor(t3);
        c.moveToNextRow();
        c.deleteCurrentRow();
        fail("IOException should have been thrown");
      } catch(IOException ignored) {
        // success
        assertTrue(ignored.getMessage().contains("Table3[id]"));
      }

      Cursor c = CursorBuilder.createCursor(t3);
      Column col = t3.getColumn("id");
      for(Map<String,Object> row : c) {
        int id = (Integer)row.get("id");
        id += 20;
        c.setCurrentRowValue(col, id);
      }

      List<? extends Map<String, Object>> expectedRows =
        createExpectedTable(
            createT1Row(0, 0, 30, "baz0", 0),
            createT1Row(1, 1, 31, "baz11", 0),
            createT1Row(2, 1, 31, "baz11-2", 0),
            createT1Row(3, 2, 33, "baz13", 0));

      assertTable(expectedRows, t1);

      c = CursorBuilder.createCursor(t2);
      for(Iterator<?> iter = c.iterator(); iter.hasNext(); ) {
        iter.next();
        iter.remove();
      }

      assertEquals(0, t1.getRowCount());

      db.close();
    }
    
  }

  private static Row createT1Row(
      int id1, int fk1, int fk2, String data, int fk3)
  {
    return createExpectedRow("id", id1, "otherfk1", fk1, "otherfk2", fk2,
                             "data", data, "otherfk3", fk3);
  }
}
