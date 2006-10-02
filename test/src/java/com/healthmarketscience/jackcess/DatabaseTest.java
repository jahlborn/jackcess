// Copyright (c) 2004 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

/**
 * @author Tim McCune
 */
public class DatabaseTest extends TestCase {
  
  public DatabaseTest(String name) throws Exception {
    super(name);
  }
   
  static Database open() throws Exception {
    return Database.open(new File("test/data/test.mdb"));
  }
  
  static Database create() throws Exception {
    return create(false);
  }

  static Database create(boolean keep) throws Exception {
    File tmp = File.createTempFile("databaseTest", ".mdb");
    if(keep) {
      System.out.println("Created " + tmp);
    } else {
      tmp.deleteOnExit();
    }
    return Database.create(tmp);
  }

  public void testInvalidTableDefs() throws Exception {
    Database db = create();

    try {
      db.createTable("test", Collections.<Column>emptyList());
      fail("created table with no columns?");
    } catch(IllegalArgumentException e) {
      // success
    }
    
    List<Column> columns = new ArrayList<Column>();
    Column col = new Column();
    col.setName("A");
    col.setType(DataType.TEXT);
    columns.add(col);
    col = new Column();
    col.setName("a");
    col.setType(DataType.MEMO);
    columns.add(col);

    try {
      db.createTable("test", columns);
      fail("created table with duplicate column names?");
    } catch(IllegalArgumentException e) {
      // success
    }

    columns = new ArrayList<Column>();
    col = new Column();
    col.setName("A");
    col.setType(DataType.TEXT);
    col.setLength((short)(352 * 2));
    columns.add(col);
    
    try {
      db.createTable("test", columns);
      fail("created table with invalid column length?");
    } catch(IllegalArgumentException e) {
      // success
    }

    columns = new ArrayList<Column>();
    col = new Column();
    col.setName("A");
    col.setType(DataType.TEXT);
    columns.add(col);
    db.createTable("test", columns);
    
    try {
      db.createTable("Test", columns);
      fail("create duplicate tables?");
    } catch(IllegalArgumentException e) {
      // success
    }

  }
      
  public void testReadDeletedRows() throws Exception {
    Table table = Database.open(new File("test/data/delTest.mdb")).getTable("Table");
    int rows = 0;
    while (table.getNextRow() != null) {
      rows++;
    }
    assertEquals(2, rows); 
  }
  
  public void testGetColumns() throws Exception {
    List columns = open().getTable("Table1").getColumns();
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
  
  static void checkColumn(List columns, int columnNumber, String name,
      DataType dataType)
    throws Exception
  {
    Column column = (Column) columns.get(columnNumber);
    assertEquals(name, column.getName());
    assertEquals(dataType, column.getType());
  }
  
  public void testGetNextRow() throws Exception {
    Database db = open();
    assertEquals(2, db.getTableNames().size());
    Table table = db.getTable("Table1");
    
    Map<String, Object> row = table.getNextRow();
    assertEquals("abcdefg", row.get("A"));
    assertEquals("hijklmnop", row.get("B"));
    assertEquals(new Byte((byte) 2), row.get("C"));
    assertEquals(new Short((short) 222), row.get("D"));
    assertEquals(new Integer(333333333), row.get("E"));
    assertEquals(new Double(444.555d), row.get("F"));
    Calendar cal = Calendar.getInstance();
    cal.setTime((Date) row.get("G"));
    assertEquals(Calendar.SEPTEMBER, cal.get(Calendar.MONTH));
    assertEquals(21, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(1974, cal.get(Calendar.YEAR));
    assertEquals(Boolean.TRUE, row.get("I"));
    
    row = table.getNextRow();
    assertEquals("a", row.get("A"));
    assertEquals("b", row.get("B"));
    assertEquals(new Byte((byte) 0), row.get("C"));
    assertEquals(new Short((short) 0), row.get("D"));
    assertEquals(new Integer(0), row.get("E"));
    assertEquals(new Double(0d), row.get("F"));
    cal = Calendar.getInstance();
    cal.setTime((Date) row.get("G"));
    assertEquals(Calendar.DECEMBER, cal.get(Calendar.MONTH));
    assertEquals(12, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(1981, cal.get(Calendar.YEAR));
    assertEquals(Boolean.FALSE, row.get("I"));
  }
  
  public void testCreate() throws Exception {
    Database db = create();
    assertEquals(0, db.getTableNames().size());
  }
  
  public void testWriteAndRead() throws Exception {
    Database db = create();
    createTestTable(db);
    Object[] row = createTestRow();
    row[3] = null;
    Table table = db.getTable("Test");
    int count = 1000;
    for (int i = 0; i < count; i++) { 
      table.addRow(row);
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
  
  public void testWriteAndReadInBatch() throws Exception {
    Database db = create();
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
  }

  public void testDeleteCurrentRow() throws Exception {

    // make sure correct row is deleted
    Database db = create();
    createTestTable(db);
    Object[] row1 = createTestRow("Tim1");
    Object[] row2 = createTestRow("Tim2");
    Object[] row3 = createTestRow("Tim3");
    Table table = db.getTable("Test");
    table.addRows(Arrays.asList(row1, row2, row3));

    table.reset();
    table.getNextRow();
    table.getNextRow();
    table.deleteCurrentRow();

    table.reset();

    Map<String, Object> outRow = table.getNextRow();
    assertEquals("Tim1", outRow.get("A"));
    outRow = table.getNextRow();
    assertEquals("Tim3", outRow.get("A"));

    // test multi row delete/add
    db = create();
    createTestTable(db);
    Object[] row = createTestRow();
    table = db.getTable("Test");
    for (int i = 0; i < 10; i++) {
      row[3] = i;
      table.addRow(row);
    }
    row[3] = 1974;
    assertEquals(10, countRows(table));
    table.reset();
    table.getNextRow();
    table.deleteCurrentRow();
    assertEquals(9, countRows(table));
    table.reset();
    table.getNextRow();
    table.deleteCurrentRow();
    assertEquals(8, countRows(table));
    table.reset();
    for (int i = 0; i < 8; i++) {
      table.getNextRow();
    }
    table.deleteCurrentRow();
    assertEquals(7, countRows(table));
    table.addRow(row);
    assertEquals(8, countRows(table));
    table.reset();
    for (int i = 0; i < 3; i++) {
      table.getNextRow();
    }
    table.deleteCurrentRow();
    assertEquals(7, countRows(table));
    table.reset();
    assertEquals(2, table.getNextRow().get("D"));
  }

  public void testReadLongValue() throws Exception {

    Database db = Database.open(new File("test/data/test2.mdb"));
    Table table = db.getTable("MSP_PROJECTS");
    Map<String, Object> row = table.getNextRow();
    assertEquals("Jon Iles this is a a vawesrasoih aksdkl fas dlkjflkasjd flkjaslkdjflkajlksj dfl lkasjdf lkjaskldfj lkas dlk lkjsjdfkl; aslkdf lkasjkldjf lka skldf lka sdkjfl;kasjd falksjdfljaslkdjf laskjdfk jalskjd flkj aslkdjflkjkjasljdflkjas jf;lkasjd fjkas dasdf asd fasdf asdf asdmhf lksaiyudfoi jasodfj902384jsdf9 aw90se fisajldkfj lkasj dlkfslkd jflksjadf as", row.get("PROJ_PROP_AUTHOR"));
    assertEquals("T", row.get("PROJ_PROP_COMPANY"));
    assertEquals("Standard", row.get("PROJ_INFO_CAL_NAME"));
    assertEquals("Project1", row.get("PROJ_PROP_TITLE"));
    byte[] foundBinaryData = (byte[])row.get("RESERVED_BINARY_DATA");
    System.out.println("FOO found len " + foundBinaryData.length);
    byte[] expectedBinaryData =
      toByteArray(new File("test/data/test2BinData.dat"));
    assertTrue(Arrays.equals(expectedBinaryData, foundBinaryData));
  }

  public void testWriteLongValue() throws Exception {

    Database db = create();

    List<Column> columns = new ArrayList<Column>();
    Column col = new Column();
    col.setName("A");
    col.setType(DataType.TEXT);
    columns.add(col);
    col = new Column();
    col.setName("B");
    col.setType(DataType.MEMO);
    columns.add(col);
    col = new Column();
    col.setName("C");
    col.setType(DataType.OLE);
    columns.add(col);
    db.createTable("test", columns);

    String testStr = "This is a test";
    StringBuilder strBuf = new StringBuilder();
    for(int i = 0; i < 2030; ++i) {
      char c = (char)('a' + (i % 26));
      strBuf.append(c);
    }
    String longMemo = strBuf.toString();
    byte[] oleValue = toByteArray(new File("test/data/test2BinData.dat"));
    
    
    Table table = db.getTable("Test");
    table.addRow(testStr, testStr, null);
    table.addRow(testStr, longMemo, oleValue);

    table.reset();

    Map<String, Object> row = table.getNextRow();

    assertEquals(testStr, row.get("A"));
    assertEquals(testStr, row.get("B"));
    assertNull(row.get("C"));

    row = table.getNextRow();
    
    assertEquals(testStr, row.get("A"));
    assertEquals(longMemo, row.get("B"));
    assertTrue(Arrays.equals(oleValue, (byte[])row.get("C")));
    
  }

  public void testMissingFile() throws Exception {
    File bogusFile = new File("fooby-dooby.mdb");
    assertTrue(!bogusFile.exists());
    try {
      Database db = Database.open(bogusFile);
      fail("FileNotFoundException should have been thrown");
    } catch(FileNotFoundException e) {
    }
    assertTrue(!bogusFile.exists());
  }

  public void testReadWithDeletedCols() throws Exception {
    Table table = Database.open(new File("test/data/delColTest.mdb")).getTable("Table1");

    Map<String, Object> expectedRow0 = new HashMap<String, Object>();
    expectedRow0.put("id", 0);
    expectedRow0.put("id2", 2);
    expectedRow0.put("data", "foo");
    expectedRow0.put("data2", "foo2");

    Map<String, Object> expectedRow1 = new HashMap<String, Object>();
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
  }

  public void testCurrency() throws Exception {
    Database db = create();

    List<Column> columns = new ArrayList<Column>();
    Column col = new Column();
    col.setName("A");
    col.setType(DataType.MONEY);
    columns.add(col);
    db.createTable("test", columns);

    Table table = db.getTable("Test");
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
  }

  public void testGUID() throws Exception
  {
    Database db = create();

    List<Column> columns = new ArrayList<Column>();
    Column col = new Column();
    col.setName("A");
    col.setType(DataType.GUID);
    columns.add(col);
    db.createTable("test", columns);

    Table table = db.getTable("Test");
    table.addRow("{32A59F01-AA34-3E29-453F-4523453CD2E6}");
    table.addRow("{32a59f01-aa34-3e29-453f-4523453cd2e6}");
    table.addRow("{11111111-1111-1111-1111-111111111111}");
    table.addRow("{FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF}");

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
                     "{FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF}"),
                 foundValues);

    try {
      table.addRow("3245234");
      fail("IOException should have been thrown");
    } catch(IOException e) {
      // ignored
    }
  }

  public void testNumeric() throws Exception
  {
    Database db = create();

    List<Column> columns = new ArrayList<Column>();
    Column col = new Column();
    col.setName("A");
    col.setType(DataType.NUMERIC);
    col.setScale((byte)4);
    col.setPrecision((byte)8);
    columns.add(col);
    
    col = new Column();
    col.setName("B");
    col.setType(DataType.NUMERIC);
    col.setScale((byte)8);
    col.setPrecision((byte)28);
    columns.add(col);
    db.createTable("test", columns);

    Table table = db.getTable("Test");
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
  }

  public void testMultiPageTableDef() throws Exception
  {
    List<Column> columns = open().getTable("Table2").getColumns();
    assertEquals(89, columns.size());
  }

  public void testOverflow() throws Exception
  {
    Database mdb = Database.open(new File("test/data/overflowTest.mdb"));
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
    assertEquals(7, countRows(table));
    
  }
  
  static Object[] createTestRow(String col1Val) {
    return new Object[] {col1Val, "R", "McCune", 1234, (byte) 0xad, 555.66d,
        777.88f, (short) 999, new Date()};
  }

  static Object[] createTestRow() {
    return createTestRow("Tim");
  }
  
  static void createTestTable(Database db) throws Exception {
    List<Column> columns = new ArrayList<Column>();
    Column col = new Column();
    col.setName("A");
    col.setType(DataType.TEXT);
    columns.add(col);
    col = new Column();
    col.setName("B");
    col.setType(DataType.TEXT);
    columns.add(col);
    col = new Column();
    col.setName("C");
    col.setType(DataType.TEXT);
    columns.add(col);
    col = new Column();
    col.setName("D");
    col.setType(DataType.LONG);
    columns.add(col);
    col = new Column();
    col.setName("E");
    col.setType(DataType.BYTE);
    columns.add(col);
    col = new Column();
    col.setName("F");
    col.setType(DataType.DOUBLE);
    columns.add(col);
    col = new Column();
    col.setName("G");
    col.setType(DataType.FLOAT);
    columns.add(col);
    col = new Column();
    col.setName("H");
    col.setType(DataType.INT);
    columns.add(col);
    col = new Column();
    col.setName("I");
    col.setType(DataType.SHORT_DATE_TIME);
    columns.add(col);
    db.createTable("test", columns);
  }

  static int countRows(Table table) throws Exception {
    int rtn = 0;
    for(Map<String, Object> row : table) {
      rtn++;
    }
    return rtn;
  }

  static void dumpDatabase(Database mdb) throws Exception {
    dumpDatabase(mdb, new PrintWriter(System.out, true));
  }

  static void dumpTable(Table table) throws Exception {
    dumpTable(table, new PrintWriter(System.out, true));
  }

  static void dumpDatabase(Database mdb, PrintWriter writer) throws Exception {
    writer.println("DATABASE:");
    for(Table table : mdb) {
      dumpTable(table, writer);
    }
  }

  static void dumpTable(Table table, PrintWriter writer) throws Exception {
    writer.println("TABLE: " + table.getName());
    List<String> colNames = new ArrayList<String>();
    for(Column col : table.getColumns()) {
      colNames.add(col.getName());
    }
    writer.println("COLUMNS: " + colNames);
    for(Map<String, Object> row : table) {

      // make byte[] printable
      for(Map.Entry<String, Object> entry : row.entrySet()) {
        Object v = entry.getValue();
        if(v instanceof byte[]) {
          byte[] bv = (byte[])v;
          entry.setValue(ByteUtil.toHexString(ByteBuffer.wrap(bv), bv.length));
        }
      }
      
      writer.println(row);
    }
  }

  static void copyFile(File srcFile, File dstFile)
    throws IOException
  {
    // FIXME should really be using commons io FileUtils here, but don't want
    // to add dep for one simple test method
    byte[] buf = new byte[1024];
    OutputStream ostream = new FileOutputStream(dstFile);
    InputStream istream = new FileInputStream(srcFile);
    try {
      int numBytes = 0;
      while((numBytes = istream.read(buf)) >= 0) {
        ostream.write(buf, 0, numBytes);
      }
    } finally {
      ostream.close();
    }
  }

  static byte[] toByteArray(File file)
    throws IOException
  {
    // FIXME should really be using commons io IOUtils here, but don't want
    // to add dep for one simple test method
    FileInputStream istream = new FileInputStream(file);
    try {
      byte[] bytes = new byte[(int)file.length()];
      istream.read(bytes);
      return bytes;
    } finally {
      istream.close();
    }
  }
  
}
