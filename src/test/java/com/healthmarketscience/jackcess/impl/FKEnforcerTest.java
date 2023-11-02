/*
Copyright (c) 2012 James Ahlborn

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
package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author James Ahlborn
 */
public class FKEnforcerTest
{
  @Test
  public void testNoEnforceForeignKeys() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX))
    {

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

  @Test
  public void testEnforceForeignKeys() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX))
    {
      try (final Database db = openCopy(testDB))
      {
        db.setEvaluateExpressions(false);
        Table t1 = db.getTable("Table1");
        Table t2 = db.getTable("Table2");
        Table t3 = db.getTable("Table3");

        IOException ioe = assertThrows(IOException.class, () -> t1.addRow(20, 0, 20, "some data", 20));
        assertTrue(ioe.getMessage().contains("Table1[otherfk2]"));

        ioe = assertThrows(IOException.class,
                               () ->
                       {
                         Cursor c = CursorBuilder.createCursor(t2);
                         c.moveToNextRow();
                         c.updateCurrentRow(30, "foo30");
                       });
        assertTrue(ioe.getMessage().contains("Table2[id]"));

        ioe = assertThrows(IOException.class,
                               () ->
                       {
                         Cursor c = CursorBuilder.createCursor(t3);
                         c.moveToNextRow();
                         c.deleteCurrentRow();
                         fail("IOException should have been thrown");
                       });
        assertTrue(ioe.getMessage().contains("Table3[id]"));

        t1.addRow(21, null, null, "null fks", null);

        Cursor c = CursorBuilder.createCursor(t3);
        Column col = t3.getColumn("id");
        for (Row row : c)
        {
          int id = row.getInt("id");
          id += 20;
          c.setCurrentRowValue(col, id);
        }

        List<? extends Map<String, Object>> expectedRows
                = createExpectedTable(
                        createT1Row(0, 0, 30, "baz0", 0),
                        createT1Row(1, 1, 31, "baz11", 0),
                        createT1Row(2, 1, 31, "baz11-2", 0),
                        createT1Row(3, 2, 33, "baz13", 0),
                        createT1Row(21, null, null, "null fks", null));

        assertTable(expectedRows, t1);

        c = CursorBuilder.createCursor(t2);
        for (Iterator<?> iter = c.iterator(); iter.hasNext();)
        {
          iter.next();
          iter.remove();
        }

        assertEquals(1, t1.getRowCount());

      }
    }
  }

  private static Row createT1Row(
          int id1, Integer fk1, Integer fk2, String data, Integer fk3)
  {
    return createExpectedRow("id", id1, "otherfk1", fk1, "otherfk2", fk2,
                             "data", data, "otherfk3", fk3);
  }
}
