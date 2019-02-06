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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;

import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.impl.RowImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.util.LinkResolver;
import com.healthmarketscience.jackcess.util.RowFilterTest;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.TestUtil.*;


/**
 * @author Tim McCune
 */
@SuppressWarnings("deprecation")
public class DatabaseTest extends TestCase
{
  public DatabaseTest(String name) throws Exception {
    super(name);
  }

  public void testInvalidTableDefs() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      try {
        new TableBuilder("test").toTable(db);
        fail("created table with no columns?");
      } catch(IllegalArgumentException e) {
        // success
      }

      try {
        new TableBuilder("test")
          .addColumn(new ColumnBuilder("A", DataType.TEXT))
          .addColumn(new ColumnBuilder("a", DataType.MEMO))
          .toTable(db);
        fail("created table with duplicate column names?");
      } catch(IllegalArgumentException e) {
        // success
      }

      try {
        new TableBuilder("test")
          .addColumn(new ColumnBuilder("A", DataType.TEXT)
                     .setLengthInUnits(352))
          .toTable(db);
        fail("created table with invalid column length?");
      } catch(IllegalArgumentException e) {
        // success
      }

      try {
        new TableBuilder("test")
          .addColumn(new ColumnBuilder("A_" + createString(70), DataType.TEXT))
          .toTable(db);
        fail("created table with too long column name?");
      } catch(IllegalArgumentException e) {
        // success
      }

      new TableBuilder("test")
        .addColumn(new ColumnBuilder("A", DataType.TEXT))
        .toTable(db);


      try {
        new TableBuilder("Test")
          .addColumn(new ColumnBuilder("A", DataType.TEXT))
          .toTable(db);
        fail("create duplicate tables?");
      } catch(IllegalArgumentException e) {
        // success
      }

      db.close();
    }
  }

  public void testReadDeletedRows() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.DEL, true)) {
      Table table = open(testDB).getTable("Table");
      int rows = 0;
      while (table.getNextRow() != null) {
        rows++;
      }
      assertEquals(2, rows);
      table.getDatabase().close();
    }
  }

  public void testGetColumns() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {

      List<? extends Column> columns = open(testDB).getTable("Table1").getColumns();
      assertEquals(9, columns.size());
      checkColumn(columns, 0, "A", DataType.TEXT);
      checkColumn(columns, 1, "B", DataType.TEXT);
      checkColumn(columns, 2, "C", DataType.BYTE);
      checkColumn(columns, 3, "D", DataType.INT);
      checkColumn(columns, 4, "E", DataType.LONG);
      checkColumn(columns, 5, "F", DataType.DOUBLE);
      checkColumn(columns, 6, "G", DataType.SHORT_DATE_TIME);
      checkColumn(columns, 7, "H", DataType.MONEY);
      checkColumn(columns, 8, "I", DataType.BOOLEAN);
    }
  }

  private static void checkColumn(
      List<? extends Column> columns, int columnNumber, String name,
      DataType dataType)
    throws Exception
  {
    Column column = columns.get(columnNumber);
    assertEquals(name, column.getName());
    assertEquals(dataType, column.getType());
  }

  public void testGetNextRow() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {
      final Database db = open(testDB);
      assertEquals(4, db.getTableNames().size());
      final Table table = db.getTable("Table1");

      Row row1 = table.getNextRow();
      Row row2 = table.getNextRow();

      if(!"abcdefg".equals(row1.get("A"))) {
        Row tmpRow = row1;
        row1 = row2;
        row2 = tmpRow;
      }

      checkTestDBTable1RowABCDEFG(testDB, table, row1);
      checkTestDBTable1RowA(testDB, table, row2);

      db.close();
    }
  }

  public void testCreate() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);
      assertEquals(0, db.getTableNames().size());
      db.close();
    }
  }

  public void testDeleteCurrentRow() throws Exception {

    // make sure correct row is deleted
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      createTestTable(db);
      Map<String,Object> row1 = createTestRowMap("Tim1");
      Map<String,Object> row2 = createTestRowMap("Tim2");
      Map<String,Object> row3 = createTestRowMap("Tim3");
      Table table = db.getTable("Test");
      @SuppressWarnings("unchecked")
      List<Map<String,Object>> rows = Arrays.asList(row1, row2, row3);
      table.addRowsFromMaps(rows);
      assertRowCount(3, table);

      table.reset();
      table.getNextRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();

      table.reset();

      Map<String, Object> outRow = table.getNextRow();
      assertEquals("Tim1", outRow.get("A"));
      outRow = table.getNextRow();
      assertEquals("Tim3", outRow.get("A"));
      assertRowCount(2, table);

      db.close();

      // test multi row delete/add
      db = createMem(fileFormat);
      createTestTable(db);
      Object[] row = createTestRow();
      table = db.getTable("Test");
      for (int i = 0; i < 10; i++) {
        row[3] = i;
        table.addRow(row);
      }
      row[3] = 1974;
      assertRowCount(10, table);
      table.reset();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      assertRowCount(9, table);
      table.reset();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      assertRowCount(8, table);
      table.reset();
      for (int i = 0; i < 8; i++) {
        table.getNextRow();
      }
      table.getDefaultCursor().deleteCurrentRow();
      assertRowCount(7, table);
      table.addRow(row);
      assertRowCount(8, table);
      table.reset();
      for (int i = 0; i < 3; i++) {
        table.getNextRow();
      }
      table.getDefaultCursor().deleteCurrentRow();
      assertRowCount(7, table);
      table.reset();
      assertEquals(2, table.getNextRow().get("D"));

      db.close();
    }
  }

  public void testDeleteRow() throws Exception {

    // make sure correct row is deleted
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      createTestTable(db);
      Table table = db.getTable("Test");
      for(int i = 0; i < 10; ++i) {
        table.addRowFromMap(createTestRowMap("Tim" + i));
      }
      assertRowCount(10, table);

      table.reset();

      List<Row> rows = RowFilterTest.toList(table);

      Row r1 = rows.remove(7);
      Row r2 = rows.remove(3);
      assertEquals(8, rows.size());

      assertSame(r2, table.deleteRow(r2));
      assertSame(r1, table.deleteRow(r1));

      assertTable(rows, table);

      table.deleteRow(r2);
      table.deleteRow(r1);

      assertTable(rows, table);
    }
  }

  public void testMissingFile() throws Exception {
    File bogusFile = new File("fooby-dooby.mdb");
    assertTrue(!bogusFile.exists());
    try {
      new DatabaseBuilder(bogusFile).setReadOnly(true).
        setAutoSync(getTestAutoSync()).open();
      fail("FileNotFoundException should have been thrown");
    } catch(FileNotFoundException e) {
    }
    assertTrue(!bogusFile.exists());
  }

  public void testReadWithDeletedCols() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.DEL_COL, true)) {
      Table table = open(testDB).getTable("Table1");

      Map<String, Object> expectedRow0 = new LinkedHashMap<String, Object>();
      expectedRow0.put("id", 0);
      expectedRow0.put("id2", 2);
      expectedRow0.put("data", "foo");
      expectedRow0.put("data2", "foo2");

      Map<String, Object> expectedRow1 = new LinkedHashMap<String, Object>();
      expectedRow1.put("id", 3);
      expectedRow1.put("id2", 5);
      expectedRow1.put("data", "bar");
      expectedRow1.put("data2", "bar2");

      int rowNum = 0;
      Map<String, Object> row = null;
      while ((row = table.getNextRow()) != null) {
        if(rowNum == 0) {
          assertEquals(expectedRow0, row);
        } else if(rowNum == 1) {
          assertEquals(expectedRow1, row);
        } else if(rowNum >= 2) {
          fail("should only have 2 rows");
        }
        rowNum++;
      }

      table.getDatabase().close();
    }
  }

  public void testCurrency() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("A", DataType.MONEY))
        .toTable(db);

      table.addRow(new BigDecimal("-2341234.03450"));
      table.addRow(37L);
      table.addRow("10000.45");

      table.reset();

      List<Object> foundValues = new ArrayList<Object>();
      Map<String, Object> row = null;
      while((row = table.getNextRow()) != null) {
        foundValues.add(row.get("A"));
      }

      assertEquals(Arrays.asList(
                       new BigDecimal("-2341234.0345"),
                       new BigDecimal("37.0000"),
                       new BigDecimal("10000.4500")),
                   foundValues);

      try {
        table.addRow(new BigDecimal("342523234145343543.3453"));
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // ignored
      }

      db.close();
    }
  }

  public void testGUID() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("A", DataType.GUID))
        .toTable(db);

      table.addRow("{32A59F01-AA34-3E29-453F-4523453CD2E6}");
      table.addRow("{32a59f01-aa34-3e29-453f-4523453cd2e6}");
      table.addRow("{11111111-1111-1111-1111-111111111111}");
      table.addRow("   {FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF}   ");
      table.addRow(UUID.fromString("32a59f01-1234-3e29-4aaf-4523453cd2e6"));

      table.reset();

      List<Object> foundValues = new ArrayList<Object>();
      Map<String, Object> row = null;
      while((row = table.getNextRow()) != null) {
        foundValues.add(row.get("A"));
      }

      assertEquals(Arrays.asList(
                       "{32A59F01-AA34-3E29-453F-4523453CD2E6}",
                       "{32A59F01-AA34-3E29-453F-4523453CD2E6}",
                       "{11111111-1111-1111-1111-111111111111}",
                       "{FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF}",
                       "{32A59F01-1234-3E29-4AAF-4523453CD2E6}"),
                   foundValues);

      try {
        table.addRow("3245234");
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // ignored
      }

      db.close();
    }
  }

  public void testNumeric() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      ColumnBuilder col = new ColumnBuilder("A", DataType.NUMERIC)
        .setScale(4).setPrecision(8).toColumn();
      assertTrue(col.getType().isVariableLength());

      Table table = new TableBuilder("test")
        .addColumn(col)
        .addColumn(new ColumnBuilder("B", DataType.NUMERIC)
                   .setScale(8).setPrecision(28))
        .toTable(db);

      table.addRow(new BigDecimal("-1234.03450"),
                   new BigDecimal("23923434453436.36234219"));
      table.addRow(37L, 37L);
      table.addRow("1000.45", "-3452345321000");

      table.reset();

      List<Object> foundSmallValues = new ArrayList<Object>();
      List<Object> foundBigValues = new ArrayList<Object>();
      Map<String, Object> row = null;
      while((row = table.getNextRow()) != null) {
        foundSmallValues.add(row.get("A"));
        foundBigValues.add(row.get("B"));
      }

      assertEquals(Arrays.asList(
                       new BigDecimal("-1234.0345"),
                       new BigDecimal("37.0000"),
                       new BigDecimal("1000.4500")),
                   foundSmallValues);
      assertEquals(Arrays.asList(
                       new BigDecimal("23923434453436.36234219"),
                       new BigDecimal("37.00000000"),
                       new BigDecimal("-3452345321000.00000000")),
                   foundBigValues);

      try {
        table.addRow(new BigDecimal("3245234.234"),
                     new BigDecimal("3245234.234"));
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // ignored
      }

      db.close();
    }
  }

  public void testFixedNumeric() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.FIXED_NUMERIC)) {
      Database db = openCopy(testDB);
      Table t = db.getTable("test");

      boolean first = true;
      for(Column col : t.getColumns()) {
        if(first) {
          assertTrue(col.isVariableLength());
          assertEquals(DataType.MEMO, col.getType());
          first = false;
        } else {
          assertFalse(col.isVariableLength());
          assertEquals(DataType.NUMERIC, col.getType());
        }
      }

      Map<String, Object> row = t.getNextRow();
      assertEquals("some data", row.get("col1"));
      assertEquals(new BigDecimal("1"), row.get("col2"));
      assertEquals(new BigDecimal("0"), row.get("col3"));
      assertEquals(new BigDecimal("0"), row.get("col4"));
      assertEquals(new BigDecimal("4"), row.get("col5"));
      assertEquals(new BigDecimal("-1"), row.get("col6"));
      assertEquals(new BigDecimal("1"), row.get("col7"));

      Object[] tmpRow = new Object[]{
        "foo", new BigDecimal("1"), new BigDecimal(3), new BigDecimal("13"),
        new BigDecimal("-17"), new BigDecimal("0"), new BigDecimal("8734")};
      t.addRow(tmpRow);
      t.reset();

      t.getNextRow();
      row = t.getNextRow();
      assertEquals(tmpRow[0], row.get("col1"));
      assertEquals(tmpRow[1], row.get("col2"));
      assertEquals(tmpRow[2], row.get("col3"));
      assertEquals(tmpRow[3], row.get("col4"));
      assertEquals(tmpRow[4], row.get("col5"));
      assertEquals(tmpRow[5], row.get("col6"));
      assertEquals(tmpRow[6], row.get("col7"));

      db.close();
    }
  }

  public void testMultiPageTableDef() throws Exception
  {
    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {
      List<? extends Column> columns = open(testDB).getTable("Table2").getColumns();
      assertEquals(89, columns.size());
    }
  }

  public void testOverflow() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.OVERFLOW, true)) {
      Database mdb = open(testDB);
      Table table = mdb.getTable("Table1");

      // 7 rows, 3 and 5 are overflow
      table.getNextRow();
      table.getNextRow();

      Map<String, Object> row = table.getNextRow();
      assertEquals(Arrays.<Object>asList(
                       null, "row3col3", null, null, null, null, null,
                       "row3col9", null),
                   new ArrayList<Object>(row.values()));

      table.getNextRow();

      row = table.getNextRow();
      assertEquals(Arrays.<Object>asList(
                       null, "row5col2", null, null, null, null, null, null,
                       null),
                   new ArrayList<Object>(row.values()));

      table.reset();
      assertRowCount(7, table);

      mdb.close();
    }
  }


  public void testUsageMapPromotion() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.PROMOTION)) {
      Database db = openMem(testDB);
      Table t = db.getTable("jobDB1");

      assertTrue(((TableImpl)t).getOwnedPagesCursor().getUsageMap().toString()
                 .startsWith("InlineHandler"));

      String lval = createNonAsciiString(255); // "--255 chars long text--";

      ((DatabaseImpl)db).getPageChannel().startWrite();
      try {
        for(int i = 0; i < 1000; ++i) {
          t.addRow(i, 13, 57, lval, lval, lval, lval, lval, lval, 47.0d);
        }
      } finally {
        ((DatabaseImpl)db).getPageChannel().finishWrite();
      }

      Set<Integer> ids = new HashSet<Integer>();
      for(Row row : t) {
        ids.add(row.getInt("ID"));
      }
      assertEquals(1000, ids.size());

      assertTrue(((TableImpl)t).getOwnedPagesCursor().getUsageMap().toString()
                 .startsWith("ReferenceHandler"));

      db.close();
    }
  }


  public void testLargeTableDef() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      final int numColumns = 90;

      List<ColumnBuilder> columns = new ArrayList<ColumnBuilder>();
      List<String> colNames = new ArrayList<String>();
      for(int i = 0; i < numColumns; ++i) {
        String colName = "MyColumnName" + i;
        colNames.add(colName);
        columns.add(new ColumnBuilder(colName, DataType.TEXT).toColumn());
      }

      Table t = new TableBuilder("test")
        .addColumns(columns)
        .toTable(db);

      List<String> row = new ArrayList<String>();
      Map<String,Object> expectedRowData = new LinkedHashMap<String, Object>();
      for(int i = 0; i < numColumns; ++i) {
        String value = "" + i + " some row data";
        row.add(value);
        expectedRowData.put(colNames.get(i), value);
      }

      t.addRow(row.toArray());

      t.reset();
      assertEquals(expectedRowData, t.getNextRow());

      db.close();
    }
  }

  public void testWriteAndReadDate() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("name", DataType.TEXT))
        .addColumn(new ColumnBuilder("date", DataType.SHORT_DATE_TIME))
        .toTable(db);

      // since jackcess does not really store millis, shave them off before
      // storing the current date/time
      long curTimeNoMillis = (System.currentTimeMillis() / 1000L);
      curTimeNoMillis *= 1000L;

      DateFormat df = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
      List<Date> dates =
        new ArrayList<Date>(
            Arrays.asList(
                df.parse("19801231 00:00:00"),
                df.parse("19930513 14:43:27"),
                null,
                df.parse("20210102 02:37:00"),
                new Date(curTimeNoMillis)));

      Calendar c = Calendar.getInstance();
      for(int year = 1801; year < 2050; year +=3) {
        for(int month = 0; month <= 12; ++month) {
          for(int day = 1; day < 29; day += 3) {
            c.clear();
            c.set(Calendar.YEAR, year);
            c.set(Calendar.MONTH, month);
            c.set(Calendar.DAY_OF_MONTH, day);
            dates.add(c.getTime());
          }
        }
      }

      ((DatabaseImpl)db).getPageChannel().startWrite();
      try {
        for(Date d : dates) {
          table.addRow("row " + d, d);
        }
      } finally {
        ((DatabaseImpl)db).getPageChannel().finishWrite();
      }

      List<Date> foundDates = new ArrayList<Date>();
      for(Row row : table) {
        foundDates.add(row.getDate("date"));
      }

      assertEquals(dates.size(), foundDates.size());
      for(int i = 0; i < dates.size(); ++i) {
        Date expected = dates.get(i);
        Date found = foundDates.get(i);
        assertSameDate(expected, found);
      }

      db.close();
    }
  }

  public void testAncientDates() throws Exception
  {
    TimeZone tz = TimeZone.getTimeZone("America/New_York");
    SimpleDateFormat sdf = DatabaseBuilder.createDateFormat("yyyy-MM-dd");
    sdf.getCalendar().setTimeZone(tz);

    List<String> dates = Arrays.asList("1582-10-15", "1582-10-14",
                                       "1492-01-10", "1392-01-10");


    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      db.setTimeZone(tz);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("name", DataType.TEXT))
        .addColumn(new ColumnBuilder("date", DataType.SHORT_DATE_TIME))
        .toTable(db);

      for(String dateStr : dates) {
        Date d = sdf.parse(dateStr);
        table.addRow("row " + dateStr, d);
      }

      List<String> foundDates = new ArrayList<String>();
      for(Row row : table) {
        foundDates.add(sdf.format(row.getDate("date")));
      }

      assertEquals(dates, foundDates);

      db.close();
    }

    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.OLD_DATES)) {
      Database db = openCopy(testDB);

      Table t = db.getTable("Table1");

      List<String> foundDates = new ArrayList<String>();
      for(Row row : t) {
        foundDates.add(sdf.format(row.getDate("DateField")));
      }

      assertEquals(dates, foundDates);

      db.close();
    }

  }

  public void testSystemTable() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Set<String> sysTables = new TreeSet<String>(
          String.CASE_INSENSITIVE_ORDER);
      sysTables.addAll(
          Arrays.asList("MSysObjects", "MSysQueries", "MSysACES",
                        "MSysRelationships"));

      if (fileFormat == FileFormat.GENERIC_JET4) {
        assertNull("file format: " + fileFormat, db.getSystemTable("MSysAccessObjects"));
      } else if (fileFormat.ordinal() < FileFormat.V2003.ordinal()) {
        assertNotNull("file format: " + fileFormat, db.getSystemTable("MSysAccessObjects"));
        sysTables.add("MSysAccessObjects");
      } else {
        // v2003+ template files have no "MSysAccessObjects" table
        assertNull("file format: " + fileFormat, db.getSystemTable("MSysAccessObjects"));
        sysTables.addAll(
            Arrays.asList("MSysNavPaneGroupCategories",
                          "MSysNavPaneGroups", "MSysNavPaneGroupToObjects",
                          "MSysNavPaneObjectIDs", "MSysAccessStorage"));
        if(fileFormat.ordinal() >= FileFormat.V2007.ordinal()) {
          sysTables.addAll(
              Arrays.asList(
                  "MSysComplexColumns", "MSysComplexType_Attachment",
                  "MSysComplexType_Decimal", "MSysComplexType_GUID",
                  "MSysComplexType_IEEEDouble", "MSysComplexType_IEEESingle",
                  "MSysComplexType_Long", "MSysComplexType_Short",
                  "MSysComplexType_Text", "MSysComplexType_UnsignedByte"));
        }
        if(fileFormat.ordinal() >= FileFormat.V2010.ordinal()) {
          sysTables.add("f_12D7448B56564D8AAE333BCC9B3718E5_Data");
          sysTables.add("MSysResources");
        }
      }

      assertEquals(sysTables, db.getSystemTableNames());

      assertNotNull(db.getSystemTable("MSysObjects"));
      assertNotNull(db.getSystemTable("MSysQueries"));
      assertNotNull(db.getSystemTable("MSysACES"));
      assertNotNull(db.getSystemTable("MSysRelationships"));

      assertNull(db.getSystemTable("MSysBogus"));

      TableMetaData tmd = db.getTableMetaData("MSysObjects");
      assertEquals("MSysObjects", tmd.getName());
      assertFalse(tmd.isLinked());
      assertTrue(tmd.isSystem());

      db.close();
    }
  }

  public void testFixedText() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.FIXED_TEXT)) {
      Database db = openCopy(testDB);

      Table t = db.getTable("users");
      Column c = t.getColumn("c_flag_");
      assertEquals(DataType.TEXT, c.getType());
      assertEquals(false, c.isVariableLength());
      assertEquals(2, c.getLength());

      Map<String,Object> row = t.getNextRow();
      assertEquals("N", row.get("c_flag_"));

      t.addRow(3, "testFixedText", "boo", "foo", "bob", 3, 5, 9, "Y",
               new Date());

      t.getNextRow();
      row = t.getNextRow();
      assertEquals("testFixedText", row.get("c_user_login"));
      assertEquals("Y", row.get("c_flag_"));

      db.close();
    }
  }

  public void testDbSortOrder() throws Exception {

    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {

      Database db = open(testDB);
      assertEquals(((DatabaseImpl)db).getFormat().DEFAULT_SORT_ORDER,
                   ((DatabaseImpl)db).getDefaultSortOrder());
      db.close();
    }
  }

  public void testUnsupportedColumns() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.UNSUPPORTED)) {

      Database db = open(testDB);
      Table t = db.getTable("Test");
      Column varCol = t.getColumn("UnknownVar");
      assertEquals(DataType.UNSUPPORTED_VARLEN, varCol.getType());
      Column fixCol = t.getColumn("UnknownFix");
      assertEquals(DataType.UNSUPPORTED_FIXEDLEN, fixCol.getType());

      List<String> varVals = Arrays.asList(
          "RawData[(10) FF FE 73 6F  6D 65 64 61  74 61]",
          "RawData[(12) FF FE 6F 74  68 65 72 20  64 61 74 61]",
          null);
      List<String> fixVals = Arrays.asList("RawData[(4) 37 00 00 00]",
                                           "RawData[(4) F3 FF FF FF]",
                                           "RawData[(4) 02 00 00 00]");

      int idx = 0;
      for(Map<String,Object> row : t) {
        checkRawValue(varVals.get(idx), varCol.getRowValue(row));
        checkRawValue(fixVals.get(idx), fixCol.getRowValue(row));
        ++idx;
      }
      db.close();
    }
  }

  public void testLinkedTables() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.LINKED)) {
      Database db = openCopy(testDB);

      try {
        db.getTable("Table2");
        fail("FileNotFoundException should have been thrown");
      } catch(FileNotFoundException e) {
        // success
      }

      TableMetaData tmd = db.getTableMetaData("Table2");
      assertEquals("Table2", tmd.getName());
      assertTrue(tmd.isLinked());
      assertFalse(tmd.isSystem());
      assertEquals("Table1", tmd.getLinkedTableName());
      assertEquals("Z:\\jackcess_test\\linkeeTest.accdb", tmd.getLinkedDbName());

      tmd = db.getTableMetaData("FooTable");
      assertNull(tmd);

      assertTrue(db.getLinkedDatabases().isEmpty());

      final String linkeeDbName = "Z:\\jackcess_test\\linkeeTest.accdb";
      final File linkeeFile = new File("src/test/data/linkeeTest.accdb");
      db.setLinkResolver(new LinkResolver() {
        public Database resolveLinkedDatabase(Database linkerdb, String dbName)
          throws IOException {
          assertEquals(linkeeDbName, dbName);
          return DatabaseBuilder.open(linkeeFile);
        }
      });

      Table t2 = db.getTable("Table2");

      assertEquals(1, db.getLinkedDatabases().size());
      Database linkeeDb = db.getLinkedDatabases().get(linkeeDbName);
      assertNotNull(linkeeDb);
      assertEquals(linkeeFile, linkeeDb.getFile());
      assertEquals("linkeeTest.accdb", ((DatabaseImpl)linkeeDb).getName());

      List<? extends Map<String, Object>> expectedRows =
        createExpectedTable(
            createExpectedRow(
                "ID", 1,
                "Field1", "bar"));

      assertTable(expectedRows, t2);

      db.createLinkedTable("FooTable", linkeeDbName, "Table2");

      tmd = db.getTableMetaData("FooTable");
      assertEquals("FooTable", tmd.getName());
      assertTrue(tmd.isLinked());
      assertFalse(tmd.isSystem());
      assertEquals("Table2", tmd.getLinkedTableName());
      assertEquals("Z:\\jackcess_test\\linkeeTest.accdb", tmd.getLinkedDbName());

      Table t3 = db.getTable("FooTable");

      assertEquals(1, db.getLinkedDatabases().size());

      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "ID", 1,
                "Field1", "buzz"));

      assertTable(expectedRows, t3);

      tmd = db.getTableMetaData("Table1");
      assertEquals("Table1", tmd.getName());
      assertFalse(tmd.isLinked());
      assertFalse(tmd.isSystem());
      assertNull(tmd.getLinkedTableName());
      assertNull(tmd.getLinkedDbName());

      Table t1 = tmd.open(db);

      assertFalse(db.isLinkedTable(null));
      assertTrue(db.isLinkedTable(t2));
      assertTrue(db.isLinkedTable(t3));
      assertFalse(db.isLinkedTable(t1));

      List<Table> tables = getTables(db.newIterable());
      assertEquals(3, tables.size());
      assertTrue(tables.contains(t1));
      assertTrue(tables.contains(t2));
      assertTrue(tables.contains(t3));
      assertFalse(tables.contains(((DatabaseImpl)db).getSystemCatalog()));

      tables = getTables(db.newIterable().setIncludeNormalTables(false));
      assertEquals(2, tables.size());
      assertFalse(tables.contains(t1));
      assertTrue(tables.contains(t2));
      assertTrue(tables.contains(t3));
      assertFalse(tables.contains(((DatabaseImpl)db).getSystemCatalog()));

      tables = getTables(db.newIterable().withLocalUserTablesOnly());
      assertEquals(1, tables.size());
      assertTrue(tables.contains(t1));
      assertFalse(tables.contains(t2));
      assertFalse(tables.contains(t3));
      assertFalse(tables.contains(((DatabaseImpl)db).getSystemCatalog()));

      tables = getTables(db.newIterable().withSystemTablesOnly());
      assertTrue(tables.size() > 5);
      assertFalse(tables.contains(t1));
      assertFalse(tables.contains(t2));
      assertFalse(tables.contains(t3));
      assertTrue(tables.contains(((DatabaseImpl)db).getSystemCatalog()));

      db.close();
    }
  }

  private static List<Table> getTables(Iterable<Table> tableIter)
  {
    List<Table> tableList = new ArrayList<Table>();
    for(Table t : tableIter) {
      tableList.add(t);
    }
    return tableList;
  }

  public void testTimeZone() throws Exception
  {
    TimeZone tz = TimeZone.getTimeZone("America/New_York");
    doTestTimeZone(tz);

    tz = TimeZone.getTimeZone("Australia/Sydney");
    doTestTimeZone(tz);
  }

  private static void doTestTimeZone(final TimeZone tz) throws Exception
  {
    ColumnImpl col = new ColumnImpl(null, null, DataType.SHORT_DATE_TIME, 0, 0, 0) {
      @Override
      public TimeZone getTimeZone() { return tz; }
      @Override
      public ZoneId getZoneId() { return null; }
      @Override
      public ColumnImpl.DateTimeFactory getDateTimeFactory() {
        return getDateTimeFactory(DateTimeType.DATE);
      }
    };

    SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd");
    df.setTimeZone(tz);

    long startDate = df.parse("2012.01.01").getTime();
    long endDate = df.parse("2013.01.01").getTime();

    Calendar curCal = Calendar.getInstance(tz);
    curCal.setTimeInMillis(startDate);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
    sdf.setTimeZone(tz);

    while(curCal.getTimeInMillis() < endDate) {
      Date curDate = curCal.getTime();
      Date newDate = new Date(col.fromDateDouble(col.toDateDouble(curDate)));
      if(curDate.getTime() != newDate.getTime()) {
        assertEquals(sdf.format(curDate), sdf.format(newDate));
      }
      curCal.add(Calendar.MINUTE, 30);
    }
  }

  public void testToString()
  {
    RowImpl row = new RowImpl(new RowIdImpl(1, 1));
    row.put("id", 37);
    row.put("data", null);
    assertEquals("Row[1:1][{id=37,data=<null>}]", row.toString());
  }

  private static void checkRawValue(String expected, Object val)
  {
    if(expected != null) {
      assertTrue(ColumnImpl.isRawData(val));
      assertEquals(expected, val.toString());
    } else {
      assertNull(val);
    }
  }
}
