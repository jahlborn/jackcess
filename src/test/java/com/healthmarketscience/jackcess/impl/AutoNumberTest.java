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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import static com.healthmarketscience.jackcess.TestUtil.*;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import junit.framework.TestCase;

/**
 *
 * @author James Ahlborn
 */
public class AutoNumberTest extends TestCase
{

  public AutoNumberTest(String name) throws Exception {
    super(name);
  }


  public void testAutoNumber() throws Exception 
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("a", DataType.LONG)
                  .setAutoNumber(true))
        .addColumn(new ColumnBuilder("b", DataType.TEXT))
        .toTable(db);

      doTestAutoNumber(table);

      db.close();
    }
  }  

  public void testAutoNumberPK() throws Exception 
  {
    for (final TestDB testDB : SUPPORTED_DBS_TEST) {
      Database db = openMem(testDB);

      Table table = db.getTable("Table3");

      doTestAutoNumber(table);

      db.close();
    }
  }  

  private static void doTestAutoNumber(Table table) throws Exception
  {
    Object[] row = {null, "row1"};
    assertSame(row, table.addRow(row));
    assertEquals(1, ((Integer)row[0]).intValue());
    row = table.addRow(13, "row2");
    assertEquals(2, ((Integer)row[0]).intValue());
    row = table.addRow("flubber", "row3");
    assertEquals(3, ((Integer)row[0]).intValue());

    table.reset();

    row = table.addRow(Column.AUTO_NUMBER, "row4");
    assertEquals(4, ((Integer)row[0]).intValue());
    row = table.addRow(Column.AUTO_NUMBER, "row5");
    assertEquals(5, ((Integer)row[0]).intValue());

    Object[] smallRow = {Column.AUTO_NUMBER};
    row = table.addRow(smallRow);
    assertNotSame(row, smallRow);
    assertEquals(6, ((Integer)row[0]).intValue());    

    table.reset();

    List<? extends Map<String, Object>> expectedRows =
      createExpectedTable(
          createExpectedRow(
              "a", 1,
              "b", "row1"),
          createExpectedRow(
              "a", 2,
              "b", "row2"),
          createExpectedRow(
              "a", 3,
              "b", "row3"),
          createExpectedRow(
              "a", 4,
              "b", "row4"),
          createExpectedRow(
              "a", 5,
              "b", "row5"),
          createExpectedRow(
              "a", 6,
              "b", null));

    assertTable(expectedRows, table);    
  }

  public void testAutoNumberGuid() throws Exception 
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("a", DataType.GUID)
                  .setAutoNumber(true))
        .addColumn(new ColumnBuilder("b", DataType.TEXT))
        .toTable(db);

      Object[] row = {null, "row1"};
      assertSame(row, table.addRow(row));
      assertTrue(ColumnImpl.isGUIDValue(row[0]));
      row = table.addRow(13, "row2");
      assertTrue(ColumnImpl.isGUIDValue(row[0]));
      row = table.addRow("flubber", "row3");
      assertTrue(ColumnImpl.isGUIDValue(row[0]));

      Object[] smallRow = {Column.AUTO_NUMBER};
      row = table.addRow(smallRow);
      assertNotSame(row, smallRow);
      assertTrue(ColumnImpl.isGUIDValue(row[0]));

      db.close();
    }
  }  

  public void testInsertLongAutoNumber() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("a", DataType.LONG)
                  .setAutoNumber(true))
        .addColumn(new ColumnBuilder("b", DataType.TEXT))
        .toTable(db);

      doTestInsertLongAutoNumber(table);

      db.close();
    }    
  }

  public void testInsertLongAutoNumberPK() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("a", DataType.LONG)
                  .setAutoNumber(true))
        .addColumn(new ColumnBuilder("b", DataType.TEXT))
        .setPrimaryKey("a")
        .toTable(db);

      doTestInsertLongAutoNumber(table);

      db.close();
    }    
  }

  private static void doTestInsertLongAutoNumber(Table table) throws Exception
  {
    assertFalse(table.getDatabase().isAllowAutoNumberInsert());
    assertFalse(table.isAllowAutoNumberInsert());

    Object[] row = {null, "row1"};
    assertSame(row, table.addRow(row));
    assertEquals(1, ((Integer)row[0]).intValue());
    row = table.addRow(13, "row2");
    assertEquals(2, ((Integer)row[0]).intValue());
    row = table.addRow("flubber", "row3");
    assertEquals(3, ((Integer)row[0]).intValue());

    table.reset();

    table.setAllowAutoNumberInsert(true);
    assertFalse(table.getDatabase().isAllowAutoNumberInsert());
    assertTrue(table.isAllowAutoNumberInsert());

    Row row2 = CursorBuilder.findRow(
        table, Collections.singletonMap("a", 2));
    assertEquals("row2", row2.getString("b"));

    table.deleteRow(row2);

    row = table.addRow(Column.AUTO_NUMBER, "row4");
    assertEquals(4, ((Integer)row[0]).intValue());

    assertEquals(4, ((TableImpl)table).getLastLongAutoNumber());

    row = table.addRow(2, "row2-redux");
    assertEquals(2, ((Integer)row[0]).intValue());

    assertEquals(4, ((TableImpl)table).getLastLongAutoNumber());

    row2 = CursorBuilder.findRow(
        table, Collections.singletonMap("a", 2));
    assertEquals("row2-redux", row2.getString("b"));

    row = table.addRow(13, "row13-mindthegap");
    assertEquals(13, ((Integer)row[0]).intValue());

    assertEquals(13, ((TableImpl)table).getLastLongAutoNumber());
    
    try {
      table.addRow("not a number", "nope");
      fail("NumberFormatException should have been thrown");
    } catch(NumberFormatException e) {
      // success
    }

    assertEquals(13, ((TableImpl)table).getLastLongAutoNumber());

    try {
      table.addRow(-10, "uh-uh");
      fail("IOException should have been thrown");
    } catch(IOException e) {
      // success
    }

    row = table.addRow(Column.AUTO_NUMBER, "row14");
    assertEquals(14, ((Integer)row[0]).intValue());

    Row row13 = CursorBuilder.findRow(
        table, Collections.singletonMap("a", 13));
    assertEquals("row13-mindthegap", row13.getString("b"));

    row13.put("a", "45");
    row13 = table.updateRow(row13);
    assertEquals(45, row13.get("a"));

    assertEquals(45, ((TableImpl)table).getLastLongAutoNumber());

    row13.put("a", -1);

    try {
      table.updateRow(row13);
      fail("IOException should have been thrown");
    } catch(IOException e) {
      // success
    }

    assertEquals(45, ((TableImpl)table).getLastLongAutoNumber());

    row13.put("a", 55);

    table.setAllowAutoNumberInsert(null);

    row13 = table.updateRow(row13);
    assertEquals(45, row13.get("a"));

    assertEquals(45, ((TableImpl)table).getLastLongAutoNumber());
    
  }

  public void testInsertComplexAutoNumber() throws Exception
  {
    for(final TestDB testDB : TestDB.getSupportedForBasename(Basename.COMPLEX)) {
      
      Database db = openMem(testDB);

      Table t1 = db.getTable("Table1");

      assertFalse(t1.isAllowAutoNumberInsert());

      int lastAutoNum = ((TableImpl)t1).getLastComplexTypeAutoNumber();

      Object[] row = t1.addRow("arow");
      ++lastAutoNum;
      checkAllComplexAutoNums(lastAutoNum, row);

      assertEquals(lastAutoNum, ((TableImpl)t1).getLastComplexTypeAutoNumber());

      db.setAllowAutoNumberInsert(true);
      assertTrue(db.isAllowAutoNumberInsert());
      assertTrue(t1.isAllowAutoNumberInsert());

      row = t1.addRow("anotherrow");
      ++lastAutoNum;
      checkAllComplexAutoNums(lastAutoNum, row);

      assertEquals(lastAutoNum, ((TableImpl)t1).getLastComplexTypeAutoNumber());
      
      row = t1.addRow("row5", 5, null, null, 5, 5);
      checkAllComplexAutoNums(5, row);

      assertEquals(lastAutoNum, ((TableImpl)t1).getLastComplexTypeAutoNumber());

      row = t1.addRow("row13", 13, null, null, 13, 13);
      checkAllComplexAutoNums(13, row);

      assertEquals(13, ((TableImpl)t1).getLastComplexTypeAutoNumber());

      try {
        t1.addRow("nope", "not a number");
        fail("NumberFormatException should have been thrown");
      } catch(NumberFormatException e) {
        // success
      }

      assertEquals(13, ((TableImpl)t1).getLastComplexTypeAutoNumber());

      try {
        t1.addRow("uh-uh", -10);
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // success
      }

      assertEquals(13, ((TableImpl)t1).getLastComplexTypeAutoNumber());

      try {
        t1.addRow("wut", 6, null, null, 40, 42);
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // success
      }

      row = t1.addRow("morerows");
      checkAllComplexAutoNums(14, row);

      assertEquals(14, ((TableImpl)t1).getLastComplexTypeAutoNumber());
      
      Row row13 = CursorBuilder.findRow(
          t1, Collections.singletonMap("id", "row13"));

      row13.put("VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880", "45");
      row13.put("multi-value-data", "45");
      row13.put("attach-data", "45");
      row13 = t1.updateRow(row13);
      checkAllComplexAutoNums(45, row13);

      assertEquals(45, ((TableImpl)t1).getLastComplexTypeAutoNumber());
      
      row13.put("attach-data", -1);

      try {
        t1.updateRow(row13);
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // success
      }

      assertEquals(45, ((TableImpl)t1).getLastComplexTypeAutoNumber());

      row13.put("attach-data", 55);

      try {
        t1.updateRow(row13);
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // success
      }

      assertEquals(45, ((TableImpl)t1).getLastComplexTypeAutoNumber());

      row13.put("VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880", 55);
      row13.put("multi-value-data", 55);

      db.setAllowAutoNumberInsert(null);

      row13 = t1.updateRow(row13);
      checkAllComplexAutoNums(45, row13);

      assertEquals(45, ((TableImpl)t1).getLastComplexTypeAutoNumber());

      db.close();
    }
  }

  private static void checkAllComplexAutoNums(int expected, Object[] row)
  {
    assertEquals(expected, ((ComplexValueForeignKey)row[1]).get());
    assertEquals(expected, ((ComplexValueForeignKey)row[4]).get());
    assertEquals(expected, ((ComplexValueForeignKey)row[5]).get());
  }

  private static void checkAllComplexAutoNums(int expected, Row row)
  {
      assertEquals(expected, ((Number)row.get("VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880")).intValue());
      assertEquals(expected, ((Number)row.get("multi-value-data")).intValue());
      assertEquals(expected, ((Number)row.get("attach-data")).intValue());
  }

  public void testInsertGuidAutoNumber() throws Exception 
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("a", DataType.GUID)
                  .setAutoNumber(true))
        .addColumn(new ColumnBuilder("b", DataType.TEXT))
        .toTable(db);

      db.setAllowAutoNumberInsert(true);
      table.setAllowAutoNumberInsert(false);
      assertFalse(table.isAllowAutoNumberInsert());

      Object[] row = {null, "row1"};
      assertSame(row, table.addRow(row));
      assertTrue(ColumnImpl.isGUIDValue(row[0]));
      row = table.addRow(13, "row2");
      assertTrue(ColumnImpl.isGUIDValue(row[0]));
      row = table.addRow("flubber", "row3");
      assertTrue(ColumnImpl.isGUIDValue(row[0]));

      Object[] smallRow = {Column.AUTO_NUMBER};
      row = table.addRow(smallRow);
      assertNotSame(row, smallRow);
      assertTrue(ColumnImpl.isGUIDValue(row[0]));

      table.setAllowAutoNumberInsert(null);
      assertTrue(table.isAllowAutoNumberInsert());
      
      Row row2 = CursorBuilder.findRow(
          table, Collections.singletonMap("b", "row2"));
      assertEquals("row2", row2.getString("b"));
      
      String row2Guid = row2.getString("a");
      table.deleteRow(row2);

      row = table.addRow(Column.AUTO_NUMBER, "row4");
      assertTrue(ColumnImpl.isGUIDValue(row[0]));

      row = table.addRow(row2Guid, "row2-redux");
      assertEquals(row2Guid, row[0]);

      row2 = CursorBuilder.findRow(
          table, Collections.singletonMap("a", row2Guid));
      assertEquals("row2-redux", row2.getString("b"));

      try {
        table.addRow("not a guid", "nope");
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // success
      }

      row = table.addRow(Column.AUTO_NUMBER, "row5");
      assertTrue(ColumnImpl.isGUIDValue(row[0]));

      row2Guid = UUID.randomUUID().toString();
      row2.put("a", row2Guid);

      row2 = table.updateRow(row2);
      assertEquals(row2Guid, row2.get("a"));

      row2.put("a", "not a guid");

      try {
        table.updateRow(row2);
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // success
      }

      table.setAllowAutoNumberInsert(false);

      row2 = table.updateRow(row2);
      assertTrue(ColumnImpl.isGUIDValue(row2.get("a")));
      assertFalse(row2Guid.equals(row2.get("a")));

      db.close();
    }
  }  

}
