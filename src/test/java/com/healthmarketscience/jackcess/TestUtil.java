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

package com.healthmarketscience.jackcess;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.StreamSupport;

import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.impl.*;
import com.healthmarketscience.jackcess.impl.JetFormatTest.TestDB;
import com.healthmarketscience.jackcess.util.MemFileChannel;
import org.junit.Assert;

/**
 * Utilty code for the test cases.
 *
 * @author James Ahlborn
 */
@SuppressWarnings("deprecation")
public class TestUtil
{
  public static final TimeZone TEST_TZ =
    TimeZone.getTimeZone("America/New_York");

  private static final ThreadLocal<Boolean> _autoSync =
    new ThreadLocal<Boolean>();

  private TestUtil() {}

  static void setTestAutoSync(boolean autoSync) {
    _autoSync.set(autoSync);
  }

  static void clearTestAutoSync() {
    _autoSync.remove();
  }

  static boolean getTestAutoSync() {
    Boolean autoSync = _autoSync.get();
    return ((autoSync != null) ? autoSync : Database.DEFAULT_AUTO_SYNC);
  }

  public static Database open(FileFormat fileFormat, File file)
    throws Exception
  {
    return open(fileFormat, file, false);
  }

  public static Database open(FileFormat fileFormat, File file, boolean inMem)
    throws Exception
  {
    return open(fileFormat, file, inMem, null);
  }

  public static Database open(FileFormat fileFormat, File file, boolean inMem,
                              Charset charset)
    throws Exception
  {
    return openDB(fileFormat, file, inMem, charset, true);
  }

  public static Database open(TestDB testDB) throws Exception {
    return open(testDB.getExpectedFileFormat(), testDB.getFile(), false,
                testDB.getExpectedCharset());
  }

  public static Database openMem(TestDB testDB) throws Exception {
    return openDB(testDB.getExpectedFileFormat(), testDB.getFile(), true,
                    testDB.getExpectedCharset(), false);
  }

  public static Database create(FileFormat fileFormat) throws Exception {
    return create(fileFormat, false);
  }

  public static Database create(FileFormat fileFormat, boolean keep)
    throws Exception
  {
    return create(fileFormat, keep, true);
  }

  public static Database createMem(FileFormat fileFormat) throws Exception {
    return create(fileFormat);
  }

  public static Database createFile(FileFormat fileFormat) throws Exception {
    return create(fileFormat, false, false);
  }

  private static Database create(FileFormat fileFormat, boolean keep,
                                 boolean inMem)
    throws Exception
  {

    FileChannel channel = ((inMem && !keep) ? MemFileChannel.newChannel() :
                           null);

    if (fileFormat == FileFormat.GENERIC_JET4) {
      // while we don't support creating GENERIC_JET4 as a jackcess feature,
      // we do want to be able to test these types of dbs
      InputStream inStream = null;
      OutputStream outStream = null;
      try {
        inStream = TestUtil.class.getClassLoader()
          .getResourceAsStream("emptyJet4.mdb");
        File f = createTempFile(keep);
        if (channel != null) {
          JetFormatTest.transferDbFrom(channel, inStream);
        } else {
          ByteUtil.copy(inStream, outStream = new FileOutputStream(f));
          outStream.close();
        }
        return new DatabaseBuilder(f)
          .setAutoSync(getTestAutoSync()).setChannel(channel).open();
      } finally {
        ByteUtil.closeQuietly(inStream);
        ByteUtil.closeQuietly(outStream);
      }
    }

    return new DatabaseBuilder(createTempFile(keep)).setFileFormat(fileFormat)
        .setAutoSync(getTestAutoSync()).setChannel(channel).create();
  }


  public static Database openCopy(TestDB testDB) throws Exception {
    return openCopy(testDB, false);
  }

  public static Database openCopy(TestDB testDB, boolean keep)
    throws Exception
  {
    return openCopy(testDB.getExpectedFileFormat(), testDB.getFile(), keep);
  }

  public static Database openCopy(FileFormat fileFormat, File file)
    throws Exception
  {
    return openCopy(fileFormat, file, false);
  }

  public static Database openCopy(FileFormat fileFormat, File file,
                                  boolean keep)
    throws Exception
  {
    File tmp = createTempFile(keep);
    copyFile(file, tmp);
    return openDB(fileFormat, tmp, false, null, false);
  }

  private static Database openDB(
      FileFormat fileFormat, File file, boolean inMem, Charset charset,
      boolean readOnly)
    throws Exception
  {
    FileChannel channel = (inMem ? MemFileChannel.newChannel(
                               file, MemFileChannel.RW_CHANNEL_MODE)
                           : null);
    final Database db = new DatabaseBuilder(file).setReadOnly(readOnly)
      .setAutoSync(getTestAutoSync()).setChannel(channel)
      .setCharset(charset).open();
    if(fileFormat != null) {
      Assert.assertEquals(
          "Wrong JetFormat.",
          DatabaseImpl.getFileFormatDetails(fileFormat).getFormat(),
          ((DatabaseImpl)db).getFormat());
      Assert.assertEquals(
          "Wrong FileFormat.", fileFormat, db.getFileFormat());
    }
    return db;
  }

  static Object[] createTestRow(String col1Val) {
    return new Object[] {col1Val, "R", "McCune", 1234, (byte) 0xad, 555.66d,
        777.88f, (short) 999, new Date()};
  }

  public static Object[] createTestRow() {
    return createTestRow("Tim");
  }

  static Map<String,Object> createTestRowMap(String col1Val) {
    return createExpectedRow("A", col1Val, "B", "R", "C", "McCune",
                             "D", 1234, "E", (byte) 0xad, "F", 555.66d,
                             "G", 777.88f, "H", (short) 999, "I", new Date());
  }

  public static void createTestTable(Database db) throws Exception {
    new TableBuilder("test")
      .addColumn(new ColumnBuilder("A", DataType.TEXT))
      .addColumn(new ColumnBuilder("B", DataType.TEXT))
      .addColumn(new ColumnBuilder("C", DataType.TEXT))
      .addColumn(new ColumnBuilder("D", DataType.LONG))
      .addColumn(new ColumnBuilder("E", DataType.BYTE))
      .addColumn(new ColumnBuilder("F", DataType.DOUBLE))
      .addColumn(new ColumnBuilder("G", DataType.FLOAT))
      .addColumn(new ColumnBuilder("H", DataType.INT))
      .addColumn(new ColumnBuilder("I", DataType.SHORT_DATE_TIME))
      .toTable(db);
  }

  public static String createString(int len) {
    return createString(len, 'a');
  }

  static String createNonAsciiString(int len) {
    return createString(len, '\u0CC0');
  }

  private static String createString(int len, char firstChar) {
    StringBuilder builder = new StringBuilder(len);
    for(int i = 0; i < len; ++i) {
      builder.append((char)(firstChar + (i % 26)));
    }
    return builder.toString();
  }

  static void assertRowCount(int expectedRowCount, Table table)
    throws Exception
  {
    Assert.assertEquals(expectedRowCount, countRows(table));
    Assert.assertEquals(expectedRowCount, table.getRowCount());
  }

  public static int countRows(Table table) throws Exception {
    Cursor cursor = CursorBuilder.createCursor(table);
    return (int) StreamSupport.stream(cursor.spliterator(), false).count();
  }

  public static void assertTable(
      List<? extends Map<String, Object>> expectedTable,
      Table table)
    throws IOException
  {
    assertCursor(expectedTable, CursorBuilder.createCursor(table));
  }

  public static void assertCursor(
      List<? extends Map<String, Object>> expectedTable,
      Cursor cursor)
  {
    List<Map<String, Object>> foundTable =
      new ArrayList<Map<String, Object>>();
    for(Map<String, Object> row : cursor) {
      foundTable.add(row);
    }
    Assert.assertEquals(expectedTable.size(), foundTable.size());
    for(int i = 0; i < expectedTable.size(); ++i) {
      Assert.assertEquals(expectedTable.get(i), foundTable.get(i));
    }
  }

  public static RowImpl createExpectedRow(Object... rowElements) {
    RowImpl row = new RowImpl((RowIdImpl)null);
    for(int i = 0; i < rowElements.length; i += 2) {
      row.put((String)rowElements[i],
              rowElements[i + 1]);
    }
    return row;
  }

  public static List<Row> createExpectedTable(Row... rows) {
    return Arrays.<Row>asList(rows);
  }

  public static void dumpDatabase(Database mdb) throws Exception {
    dumpDatabase(mdb, false);
  }

  public static void dumpDatabase(Database mdb, boolean systemTables)
    throws Exception
  {
    dumpDatabase(mdb, systemTables, new PrintWriter(System.out, true));
  }

  public static void dumpTable(Table table) throws Exception {
    dumpTable(table, new PrintWriter(System.out, true));
  }

  public static void dumpProperties(Table table) throws Exception {
    System.out.println("TABLE_PROPS: " + table.getName() + ": " +
                       table.getProperties());
    for(Column c : table.getColumns()) {
      System.out.println("COL_PROPS: " + c.getName() + ": " +
                         c.getProperties());
    }
  }

  static void dumpDatabase(Database mdb, boolean systemTables,
                           PrintWriter writer) throws Exception
  {
    writer.println("DATABASE:");
    for(Table table : mdb) {
      dumpTable(table, writer);
    }
    if(systemTables) {
      for(String sysTableName : mdb.getSystemTableNames()) {
        dumpTable(mdb.getSystemTable(sysTableName), writer);
      }
    }
  }

  static void dumpTable(Table table, PrintWriter writer) throws Exception {
    // make sure all indexes are read
    for(Index index : table.getIndexes()) {
      ((IndexImpl)index).initialize();
    }

    writer.println("TABLE: " + table.getName());
    List<String> colNames = new ArrayList<String>();
    for(Column col : table.getColumns()) {
      colNames.add(col.getName());
    }
    writer.println("COLUMNS: " + colNames);
    for(Map<String, Object> row : CursorBuilder.createCursor(table)) {
      writer.println(massageRow(row));
    }
  }

  private static Map<String,Object> massageRow(Map<String, Object> row)
    throws IOException
  {
      for(Map.Entry<String, Object> entry : row.entrySet()) {
        Object v = entry.getValue();
        if(v instanceof byte[]) {
          // make byte[] printable
          byte[] bv = (byte[])v;
          entry.setValue(ByteUtil.toHexString(ByteBuffer.wrap(bv), bv.length));
        } else if(v instanceof ComplexValueForeignKey) {
          // deref complex values
          String str = "ComplexValue(" + v + ")" +
            ((ComplexValueForeignKey)v).getValues();
          entry.setValue(str);
        }
      }

      return row;
  }

  static void dumpIndex(Index index) throws Exception {
    dumpIndex(index, new PrintWriter(System.out, true));
  }

  static void dumpIndex(Index index, PrintWriter writer) throws Exception {
    writer.println("INDEX: " + index);
    IndexData.EntryCursor ec = ((IndexImpl)index).cursor();
    IndexData.Entry lastE = ec.getLastEntry();
    IndexData.Entry e = null;
    while((e = ec.getNextEntry()) != lastE) {
      writer.println(e);
    }
  }

  static void assertSameDate(Date expected, Date found)
  {
    if(expected == found) {
      return;
    }
    if((expected == null) || (found == null)) {
      throw new AssertionError("Expected " + expected + ", found " + found);
    }
    long expTime = expected.getTime();
    long foundTime = found.getTime();
    // there are some rounding issues due to dates being stored as doubles,
    // but it results in a 1 millisecond difference, so i'm not going to worry
    // about it
    if((expTime != foundTime) && (Math.abs(expTime - foundTime) > 1)) {
      throw new AssertionError("Expected " + expTime + " (" + expected +
                               "), found " + foundTime + " (" + found + ")");
    }
  }

  static void assertSameDate(Date expected, LocalDateTime found)
  {
    if((expected == null) && (found == null)) {
      return;
    }
    if((expected == null) || (found == null)) {
      throw new AssertionError("Expected " + expected + ", found " + found);
    }

    LocalDateTime expectedLdt = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(expected.getTime()),
        ZoneId.systemDefault());

    Assert.assertEquals(expectedLdt, found);
  }

  public static void copyFile(File srcFile, File dstFile)
    throws IOException
  {
    // FIXME should really be using commons io FileUtils here, but don't want
    // to add dep for one simple test method
    OutputStream ostream = new FileOutputStream(dstFile);
    InputStream istream = new FileInputStream(srcFile);
    try {
      copyStream(istream, ostream);
    } finally {
      ostream.close();
    }
  }

  static void copyStream(InputStream istream, OutputStream ostream)
    throws IOException
  {
    // FIXME should really be using commons io FileUtils here, but don't want
    // to add dep for one simple test method
    byte[] buf = new byte[1024];
    int numBytes = 0;
    while((numBytes = istream.read(buf)) >= 0) {
      ostream.write(buf, 0, numBytes);
    }
  }

  public static File createTempFile(boolean keep) throws Exception {
    File tmp = File.createTempFile("databaseTest", ".mdb");
    if(keep) {
      System.out.println("Created " + tmp);
    } else {
      tmp.deleteOnExit();
    }
    return tmp;
  }

  public static void clearTableCache(Database db) throws Exception
  {
    Field f = db.getClass().getDeclaredField("_tableCache");
    f.setAccessible(true);
    Object val = f.get(db);
    f = val.getClass().getDeclaredField("_tables");
    f.setAccessible(true);
    val = f.get(val);
    ((Map<?,?>)val).clear();
  }

  public static byte[] toByteArray(File file)
    throws IOException
  {
    return toByteArray(new FileInputStream(file), file.length());
  }

  public static byte[] toByteArray(InputStream in, long length)
    throws IOException
  {
    // FIXME should really be using commons io IOUtils here, but don't want
    // to add dep for one simple test method
    try {
      DataInputStream din = new DataInputStream(in);
      byte[] bytes = new byte[(int)length];
      din.readFully(bytes);
      return bytes;
    } finally {
      in.close();
    }
  }

  static void checkTestDBTable1RowABCDEFG(final TestDB testDB, final Table table, final Row row)
          throws IOException {
    Assert.assertEquals("testDB: " + testDB + "; table: " + table, "abcdefg", row.get("A"));
    Assert.assertEquals("hijklmnop", row.get("B"));
    Assert.assertEquals(new Byte((byte) 2), row.get("C"));
    Assert.assertEquals(new Short((short) 222), row.get("D"));
    Assert.assertEquals(new Integer(333333333), row.get("E"));
    Assert.assertEquals(new Double(444.555d), row.get("F"));
    final Calendar cal = Calendar.getInstance();
    cal.setTime(row.getDate("G"));
    Assert.assertEquals(Calendar.SEPTEMBER, cal.get(Calendar.MONTH));
    Assert.assertEquals(21, cal.get(Calendar.DAY_OF_MONTH));
    Assert.assertEquals(1974, cal.get(Calendar.YEAR));
    Assert.assertEquals(0, cal.get(Calendar.HOUR_OF_DAY));
    Assert.assertEquals(0, cal.get(Calendar.MINUTE));
    Assert.assertEquals(0, cal.get(Calendar.SECOND));
    Assert.assertEquals(0, cal.get(Calendar.MILLISECOND));
    Assert.assertEquals(Boolean.TRUE, row.get("I"));
  }

  static void checkTestDBTable1RowA(final TestDB testDB, final Table table, final Row row)
          throws IOException {
    Assert.assertEquals("testDB: " + testDB + "; table: " + table, "a", row.get("A"));
    Assert.assertEquals("b", row.get("B"));
    Assert.assertEquals(new Byte((byte) 0), row.get("C"));
    Assert.assertEquals(new Short((short) 0), row.get("D"));
    Assert.assertEquals(new Integer(0), row.get("E"));
    Assert.assertEquals(new Double(0d), row.get("F"));
    final Calendar cal = Calendar.getInstance();
    cal.setTime(row.getDate("G"));
    Assert.assertEquals(Calendar.DECEMBER, cal.get(Calendar.MONTH));
    Assert.assertEquals(12, cal.get(Calendar.DAY_OF_MONTH));
    Assert.assertEquals(1981, cal.get(Calendar.YEAR));
    Assert.assertEquals(0, cal.get(Calendar.HOUR_OF_DAY));
    Assert.assertEquals(0, cal.get(Calendar.MINUTE));
    Assert.assertEquals(0, cal.get(Calendar.SECOND));
    Assert.assertEquals(0, cal.get(Calendar.MILLISECOND));
    Assert.assertEquals(Boolean.FALSE, row.get("I"));
  }

}
