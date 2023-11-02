/*
Copyright (c) 2015 James Ahlborn

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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.util.RowFilterTest;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author James Ahlborn
 */
public class DatabaseReadWriteTest
{
  @Test
  public void testWriteAndRead() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);
      doTestWriteAndRead(db);
      db.close();
    }
  }

  @Test
  public void testWriteAndReadInMem() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      doTestWriteAndRead(db);
      db.close();
    }
  }

  private static void doTestWriteAndRead(Database db) throws Exception {
      createTestTable(db);
      Object[] row = createTestRow();
      row[3] = null;
      Table table = db.getTable("Test");
      int count = 1000;
      ((DatabaseImpl)db).getPageChannel().startWrite();
      try {
        for (int i = 0; i < count; i++) {
          table.addRow(row);
        }
      } finally {
        ((DatabaseImpl)db).getPageChannel().finishWrite();
      }
      for (int i = 0; i < count; i++) {
        Map<String, Object> readRow = table.getNextRow();
        assertEquals(row[0], readRow.get("A"));
        assertEquals(row[1], readRow.get("B"));
        assertEquals(row[2], readRow.get("C"));
        assertEquals(row[3], readRow.get("D"));
        assertEquals(row[4], readRow.get("E"));
        assertEquals(row[5], readRow.get("F"));
        assertEquals(row[6], readRow.get("G"));
        assertEquals(row[7], readRow.get("H"));
      }
  }

  @Test
  public void testWriteAndReadInBatch() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      createTestTable(db);
      int count = 1000;
      List<Object[]> rows = new ArrayList<Object[]>(count);
      Object[] row = createTestRow();
      for (int i = 0; i < count; i++) {
        rows.add(row);
      }
      Table table = db.getTable("Test");
      table.addRows(rows);
      for (int i = 0; i < count; i++) {
        Map<String, Object> readRow = table.getNextRow();
        assertEquals(row[0], readRow.get("A"));
        assertEquals(row[1], readRow.get("B"));
        assertEquals(row[2], readRow.get("C"));
        assertEquals(row[3], readRow.get("D"));
        assertEquals(row[4], readRow.get("E"));
        assertEquals(row[5], readRow.get("F"));
        assertEquals(row[6], readRow.get("G"));
        assertEquals(row[7], readRow.get("H"));
      }

      db.close();
    }
  }

  @Test
  public void testUpdateRow() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);

      Table t = new TableBuilder("test")
        .addColumn(new ColumnBuilder("name", DataType.TEXT))
        .addColumn(new ColumnBuilder("id", DataType.LONG)
                   .setAutoNumber(true))
        .addColumn(new ColumnBuilder("data", DataType.TEXT)
                   .setLength(JetFormat.TEXT_FIELD_MAX_LENGTH))
        .toTable(db);

      for(int i = 0; i < 10; ++i) {
        t.addRow("row" + i, Column.AUTO_NUMBER, "initial data");
      }

      Cursor c = CursorBuilder.createCursor(t);
      c.reset();
      c.moveNextRows(2);
      Map<String,Object> row = c.getCurrentRow();

      assertEquals(createExpectedRow("name", "row1",
                                     "id", 2,
                                     "data", "initial data"),
                   row);

      Map<String,Object> newRow = createExpectedRow(
          "name", Column.KEEP_VALUE,
          "id", Column.AUTO_NUMBER,
          "data", "new data");
      assertSame(newRow, c.updateCurrentRowFromMap(newRow));
      assertEquals(createExpectedRow("name", "row1",
                                     "id", 2,
                                     "data", "new data"),
                   newRow);

      c.moveNextRows(3);
      row = c.getCurrentRow();

      assertEquals(createExpectedRow("name", "row4",
                                     "id", 5,
                                     "data", "initial data"),
                   row);

      c.updateCurrentRow(Column.KEEP_VALUE, Column.AUTO_NUMBER, "a larger amount of new data");

      c.reset();
      c.moveNextRows(2);
      row = c.getCurrentRow();

      assertEquals(createExpectedRow("name", "row1",
                                     "id", 2,
                                     "data", "new data"),
                   row);

      c.moveNextRows(3);
      row = c.getCurrentRow();

      assertEquals(createExpectedRow("name", "row4",
                                     "id", 5,
                                     "data", "a larger amount of new data"),
                   row);

      t.reset();

      String str = createString(100);
      for(int i = 10; i < 50; ++i) {
        t.addRow("row" + i, Column.AUTO_NUMBER, "big data_" + str);
      }

      c.reset();
      c.moveNextRows(9);
      row = c.getCurrentRow();

      assertEquals(createExpectedRow("name", "row8",
                                     "id", 9,
                                     "data", "initial data"),
                   row);

      String newText = "updated big data_" + createString(200);

      c.setCurrentRowValue(t.getColumn("data"), newText);

      c.reset();
      c.moveNextRows(9);
      row = c.getCurrentRow();

      assertEquals(createExpectedRow("name", "row8",
                                     "id", 9,
                                     "data", newText),
                   row);

      List<Row> rows = RowFilterTest.toList(t);
      assertEquals(50, rows.size());

      for(Row r : rows) {
        r.put("data", "final data " + r.get("id"));
      }

      for(Row r : rows) {
        assertSame(r, t.updateRow(r));
      }

      t.reset();

      for(Row r : t) {
        assertEquals("final data " + r.get("id"), r.get("data"));
      }

      db.close();
    }
  }

  @Test
  public void testDateMath()
  {
    long now = System.currentTimeMillis();

    // test around current time
    doTestDateMath(now);

    // test around the unix epoch
    doTestDateMath(0L);

    // test around the access epoch
    doTestDateMath(-ColumnImpl.MILLIS_BETWEEN_EPOCH_AND_1900);
  }

  private static void doTestDateMath(long testTime)
  {
    final long timeRange = 100000000L;
    final long timeStep = 37L;

    for(long time = testTime - timeRange; time < testTime + timeRange;
        time += timeStep) {
      double accTime = ColumnImpl.toLocalDateDouble(time);
      long newTime = ColumnImpl.fromLocalDateDouble(accTime);
      assertEquals(time, newTime);

      Instant inst = Instant.ofEpochMilli(time);
      LocalDateTime ldt = LocalDateTime.ofInstant(inst, ZoneOffset.UTC);

      accTime = ColumnImpl.toDateDouble(ldt);
      LocalDateTime newLdt = ColumnImpl.ldtFromLocalDateDouble(accTime);
      assertEquals(ldt, newLdt);
    }
  }
}
