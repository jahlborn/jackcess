/*
Copyright (c) 2008 Health Market Science, Inc.

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

import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 * @author James Ahlborn
 */
public class IndexCodesTest {

  private static final Map<Character,String> SPECIAL_CHARS =
    new HashMap<Character,String>();
  static {
    SPECIAL_CHARS.put('\b', "\\b");
    SPECIAL_CHARS.put('\t', "\\t");
    SPECIAL_CHARS.put('\n', "\\n");
    SPECIAL_CHARS.put('\f', "\\f");
    SPECIAL_CHARS.put('\r', "\\r");
    SPECIAL_CHARS.put('\"', "\\\"");
    SPECIAL_CHARS.put('\'', "\\'");
    SPECIAL_CHARS.put('\\', "\\\\");
  }

  @Test
  public void testIndexCodes() throws Exception
  {
    doTestDb(Basename.INDEX_CODES);
  }

  @Test
  public void testEmoticons() throws Exception
  {
    doTestDb(Basename.EMOTICONS);
  }

  /**
   * Tests the "international ext" characters, whose codes end with a suffix
   * whose length grows with the number of characters which took that path.
   * The expected keys were read back out of the index pages Access built for
   * U+3041 (crazy flag set) and U+3042 (crazy flag clear).
   */
  @Test
  public void testInternationalExtCodes() throws Exception
  {
    try(Database db = create(Database.FileFormat.V2010)) {

      IndexData.ColumnDescriptor col = getTextIndexColumn(db);

      // one suffix repeat covers up to 7 chars, so these are the two
      // boundaries either side of the first extra repeat
      assertIndexKey("7f7f02010101a0ff0280ff8000", col, repeat('\u3041', 1));
      assertIndexKey("7f7f02010101ff0280ff8000", col, repeat('\u3042', 1));
      assertIndexKey(
          "7f7f027f027f027f027f027f027f027f02010101aaaaa8ff028080ff808000",
          col, repeat('\u3041', 8));
      assertIndexKey(
          "7f7f027f027f027f027f027f027f027f02010101ff028080ff808000",
          col, repeat('\u3042', 8));
      assertIndexKey(
          "7f7f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f02" +
          "010101aaaaaaaaaaa0ff02808080ff80808000",
          col, repeat('\u3041', 16));
    }
  }

  /**
   * Tests the surrogate chars in the general collation.  The weight table
   * gives the high surrogates four runs, one of which has no weight at all,
   * and the low surrogates one piecewise run.  The expected keys were read
   * back out of the index Access built.
   */
  @Test
  public void testGeneralSurrogates() throws Exception
  {
    try(Database db = create(Database.FileFormat.V2010)) {

      IndexData.ColumnDescriptor col = getTextIndexColumn(db);

      // U+D800 to U+D83F, primary = cp - 10238, extra byte 0x3f
      assertIndexKey("7fb002b4f80e020e02013f3f00", col, pair(0x10000) + "aa");
      assertIndexKey("7fb03fb6fc0e020e02013f3f00", col, pair(0x1F600) + "aa");

      // U+D840 to U+D87F, primary = cp + 9666, extra byte 0x3e
      assertIndexKey("7ffe02b4f80e020e02013e3f00", col, pair(0x20000) + "aa");
      assertIndexKey("7ffe41b8ff0e020e02013e3f00", col, pair(0x2FFFF) + "aa");

      // U+D880 to U+DB7F, no weight, so the high surrogate writes nothing at
      // all and the tail carries one extra byte rather than two
      assertIndexKey("7fb4f80e020e02013f00", col, pair(0x30000) + "aa");
      assertIndexKey("7fb8ff0e020e02013f00", col, pair(0xEFFFF) + "aa");

      // U+DB80 to U+DBFF, primary = cp + 9090, extra byte 0x3e
      assertIndexKey("7fff02b4f80e020e02013e3f00", col, pair(0xF0000) + "aa");
      assertIndexKey("7fff81b8ff0e020e02013e3f00", col, pair(0x10FFFF) + "aa");

      // the pair in the other positions, which moves the separator
      assertIndexKey("7f0e02fe02b4f80e0201023e3f00", col,
                     "a" + pair(0x20000) + "a");
      assertIndexKey("7f0e020e02b4f80e020102023f00", col,
                     "aa" + pair(0x30000) + "a");
    }
  }

  /**
   * Tests the surrogate chars in the general legacy collation, which gives
   * them no weight at all, so both halves of a pair are ignored.  The
   * expected keys were read back out of the index Access built.
   */
  @Test
  public void testGeneralLegacySurrogates() throws Exception
  {
    try(Database db = create(Database.FileFormat.V2003)) {

      IndexData.ColumnDescriptor col = getTextIndexColumn(db);

      // 4a is the legacy code for 'a'.  every one of these is the key for
      // "aa", whatever the code point and wherever it sits in the string
      for(int cp : new int[]{0x10000, 0x20000, 0x30000, 0xF0000, 0x10FFFF}) {
        assertIndexKey("7f4a4a0100", col, pair(cp) + "aa");
        assertIndexKey("7f4a4a0100", col, "a" + pair(cp) + "a");
        assertIndexKey("7f4a4a0100", col, "aa" + pair(cp));
        assertIndexKey("7f4a4a4a0100", col, "aa" + pair(cp) + "a");
      }
    }
  }

  private static String pair(int codePoint) {
    return new String(Character.toChars(codePoint));
  }

  /**
   * Tests that a key longer than the maximum is truncated the way Access
   * truncates it, with a digest of the discarded bytes.  The expected key was
   * read back out of the index page Access built.
   */
  @Test
  public void testTruncatedKey() throws Exception
  {
    try(Database db = create(Database.FileFormat.V2010)) {

      IndexData.ColumnDescriptor col = getTextIndexColumn(db);

      // 200 chars of U+3041 encode to more than the maximum key length, so
      // Access keeps the leading 508 bytes and ends the key with a digest of
      // everything it discarded
      byte[] fullKey = encodeIndexKey(col, repeat('\u3041', 200));
      assertEquals(533, fullKey.length);

      // the truncated form is what Access actually stores
      assertEquals(
          "7f7f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f02" +
          "7f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f" +
          "027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f02" +
          "7f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f" +
          "027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f02" +
          "7f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f" +
          "027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f02" +
          "7f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f" +
          "027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f02" +
          "7f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f" +
          "027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f02" +
          "7f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f027f" +
          "027f027f02010101aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
          "aaaaaaaaaaaaaaaaa8ff0280808080808080808080808080808080808080808080" +
          "80808080808080ff8080808080d06b",
          toCompactHex(IndexData.truncateEntryBytes(fullKey)));
    }
  }

  private static IndexData.ColumnDescriptor getTextIndexColumn(Database db)
    throws Exception
  {
    Table t = new TableBuilder("test")
      .addColumn(new ColumnBuilder("data", DataType.MEMO))
      .addIndex(new IndexBuilder("dataidx").addColumns("data"))
      .toTable(db);
    IndexImpl idx = (IndexImpl)t.getIndex("dataidx");
    return idx.getIndexData().getColumns().get(0);
  }

  private static void assertIndexKey(String expected,
                                     IndexData.ColumnDescriptor col,
                                     String value)
    throws Exception
  {
    assertEquals(expected, toCompactHex(encodeIndexKey(col, value)),
                 "key for " + toUnicodeStr(value));
  }

  private static byte[] encodeIndexKey(IndexData.ColumnDescriptor col,
                                       String value)
    throws Exception
  {
    ByteUtil.ByteStream bout = new ByteUtil.ByteStream();
    col.writeValue(value, bout);
    return bout.toByteArray();
  }

  private static String repeat(char c, int num)
  {
    StringBuilder sb = new StringBuilder(num);
    for(int i = 0; i < num; ++i) {
      sb.append(c);
    }
    return sb.toString();
  }

  private static String toCompactHex(byte[] bytes)
  {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for(byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  private static void doTestDb(Basename dbBaseName) throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(dbBaseName, true)) {
      try (Database db = openMem(testDB)) {
        db.setDateTimeType(DateTimeType.DATE);

        for(Table t : db) {
          for(Index index : t.getIndexes()) {
    //         System.out.println("Checking " + t.getName() + "." + index.getName());
            checkIndexEntries(testDB, t, index);
          }
        }
      }
    }
  }

  public static void checkIndexEntries(final TestDB testDB, Table t, Index index) throws Exception
  {
//         index.initialize();
//         System.out.println("Ind " + index);

    Cursor cursor = CursorBuilder.createCursor(index);
    while(cursor.moveToNextRow()) {

      Row row = cursor.getCurrentRow();

      Object data = row.get("data");
      if((testDB.getExpectedFileFormat() == Database.FileFormat.V1997) &&
         (data instanceof String) && ((String)data).contains("\uFFFD")) {
        // this row has a character not supported in the v1997 charset
        continue;
      }

      Cursor.Position curPos = cursor.getSavepoint().getCurrentPosition();
      boolean success = false;
      try {
        findRow(testDB, t, index, row, curPos);
        success = true;
      } finally {
        if(!success) {
          System.out.println("CurPos: " + curPos);
          System.out.println("Value: " + row + ": " +
                             toUnicodeStr(row.get("data")));
        }
      }
    }

  }

  private static void findRow(final TestDB testDB, Table t, Index index,
                              Row expectedRow,
                              Cursor.Position expectedPos)
    throws Exception
  {
    Object[] idxRow = ((IndexImpl)index).constructIndexRow(expectedRow);
    Cursor cursor = CursorBuilder.createCursor(index, idxRow, idxRow);

    Cursor.Position startPos = cursor.getSavepoint().getCurrentPosition();

    cursor.beforeFirst();
    while(cursor.moveToNextRow()) {
      Row row = cursor.getCurrentRow();
      if(expectedRow.equals(row)) {
        // verify that the entries are indeed equal
        Cursor.Position curPos = cursor.getSavepoint().getCurrentPosition();
        assertEquals(entryToString(expectedPos), entryToString(curPos));
        return;
      }
    }

    fail("testDB: " + testDB + ";\nCould not find expected row " + expectedRow + " starting at " +
         entryToString(startPos));
  }


  //////
  //
  // The code below is for use in reverse engineering index entries.
  //
  //////

  @Test
  public void testNothing() throws Exception {
    // keep this so build doesn't fail if other tests are disabled
  }

  public void x_testCreateIsoFile() throws Exception
  {
    try (Database db = create(Database.FileFormat.V2000, true)) {

      Table t = new TableBuilder("test")
        .addColumn(new ColumnBuilder("row", DataType.TEXT))
        .addColumn(new ColumnBuilder("data", DataType.TEXT))
        .toTable(db);

      for(int i = 0; i < 256; ++i) {
        String str = "AA" + ((char)i) + "AA";
        t.addRow("row" + i, str);
      }
    }
  }

  public void x_testCreateAltIsoFile() throws Exception
  {
    try (Database db = openCopy(Database.FileFormat.V2000, new File("/tmp/test_ind.mdb"), true)) {

      Table t = db.getTable("Table1");

      for(int i = 0; i < 256; ++i) {
        String str = "AA" + ((char)i) + "AA";
        t.addRow("row" + i, str,
                 (byte)42 + i, (short)53 + i, 13 * i,
                 (6.7d / i), null, null, true);
      }
    }
  }

  public void x_testWriteAllCodesMdb() throws Exception
  {
    try (Database db = create(Database.FileFormat.V2000, true)) {

//     Table t = new TableBuilder("Table1")
//       .addColumn(new ColumnBuilder("key", DataType.TEXT))
//       .addColumn(new ColumnBuilder("data", DataType.TEXT))
//       .toTable(db);

//     for(int i = 0; i <= 0xFFFF; ++i) {
//       // skip non-char chars
//       char c = (char)i;
//       if(Character.isHighSurrogate(c) || Character.isLowSurrogate(c)) {
//         continue;
//       }
//       String key = toUnicodeStr(c);
//       String str = "AA" + c + "AA";
//       t.addRow(key, str);
//     }

      Table t = new TableBuilder("Table5")
        .addColumn(new ColumnBuilder("name", DataType.TEXT))
        .addColumn(new ColumnBuilder("data", DataType.TEXT))
        .toTable(db);

      char c = (char)0x3041;   // crazy 7F 02 ... A0
      char c2 = (char)0x30A2;  // crazy 7F 02 ...
      char c3 = (char)0x2045;  // inat 27 ... 1C
      char c4 = (char)0x3043;  // crazy 7F 03 ... A0
      char c5 = (char)0x3046;  // crazy 7F 04 ...
      char c6 = (char)0x30F6;  // crazy 7F 0D ... A0
      char c7 = (char)0x3099;  // unprint 03
      char c8 = (char)0x0041;  // A
      char c9 = (char)0x002D;  // - (unprint)
      char c10 = (char)0x20E1; // unprint F2
      char c11 = (char)0x309A; // unprint 04
      char c12 = (char)0x01C4; // (long extra)
      char c13 = (char)0x005F; // _ (long inline)
      char c14 = (char)0xFFFE; // removed

      char[] cs = new char[]{c7, c8, c3, c12, c13, c14, c, c2, c9};
      addCombos(t, 0, "", cs, 5);

//     t = new TableBuilder("Table2")
//       .addColumn(new ColumnBuilder("data", DataType.TEXT))
//       .toTable(db);

//     writeChars(0x0000, t);

//     t = new TableBuilder("Table3")
//       .addColumn(new ColumnBuilder("data", DataType.TEXT))
//       .toTable(db);

//     writeChars(0x0400, t);

    }
  }

  public void x_testReadAllCodesMdb() throws Exception
  {
//     Database db = openCopy(new File("/data2/jackcess_test/testAllIndexCodes.mdb"));
//     Database db = openCopy(new File("/data2/jackcess_test/testAllIndexCodes_orig.mdb"));
//     Database db = openCopy(new File("/data2/jackcess_test/testSomeMoreCodes.mdb"));
    try (Database db = openCopy(Database.FileFormat.V2000, new File("/data2/jackcess_test/testStillMoreCodes.mdb"))) {
      Table t = db.getTable("Table5");

      Index ind = t.getIndexes().iterator().next();
      ((IndexImpl)ind).initialize();

      System.out.println("Ind " + ind);

      Cursor cursor = CursorBuilder.createCursor(ind);
      while(cursor.moveToNextRow()) {
        System.out.println("=======");
        String entryStr =
          entryToString(cursor.getSavepoint().getCurrentPosition());
        System.out.println("Entry Bytes: " + entryStr);
        System.out.println("Value: " + cursor.getCurrentRow() + "; " +
                           toUnicodeStr(cursor.getCurrentRow().get("data")));
      }
    }
  }

  private int addCombos(Table t, int rowNum, String s, char[] cs, int len)
    throws Exception
  {
    if(s.length() >= len) {
      return rowNum;
    }

    for(int i = 0; i < cs.length; ++i) {
      String name = "row" + (rowNum++);
      String ss = s + cs[i];
      t.addRow(name, ss);
      rowNum = addCombos(t, rowNum, ss, cs, len);
    }

    return rowNum;
  }

  private void writeChars(int hibyte, Table t) throws Exception
  {
    char other = (char)(hibyte | 0x41);
    for(int i = 0; i < 0xFF; ++i) {
      char c = (char)(hibyte | i);
      String str = "" + other + c + other;
      t.addRow(str);
    }
  }

  public void x_testReadIsoMdb() throws Exception
  {
//     Database db = open(new File("/tmp/test_ind.mdb"));
//     Database db = open(new File("/tmp/test_ind2.mdb"));
    try (Database db = open(Database.FileFormat.V2000, new File("/tmp/test_ind3.mdb"))) {
//     Database db = open(new File("/tmp/test_ind4.mdb"));

      Table t = db.getTable("Table1");
      Index index = t.getIndex("B");
      ((IndexImpl)index).initialize();
      System.out.println("Ind " + index);

      Cursor cursor = CursorBuilder.createCursor(index);
      while(cursor.moveToNextRow()) {
        System.out.println("=======");
        System.out.println("Savepoint: " + cursor.getSavepoint());
        System.out.println("Value: " + cursor.getCurrentRow());
      }
    }
  }

  public void x_testReverseIsoMdb2010() throws Exception
  {
    try (Database db = open(Database.FileFormat.V2010, new File("/data2/jackcess_test/testAllIndexCodes3_2010.accdb"))) {

      Table t = db.getTable("Table1");
      Index index = t.getIndexes().iterator().next();
      ((IndexImpl)index).initialize();
      System.out.println("Ind " + index);

      Pattern inlinePat = Pattern.compile("7F 0E 02 0E 02 (.*)0E 02 0E 02 01 00");
      Pattern unprintPat = Pattern.compile("01 01 01 80 (.+) 06 (.+) 00");
      Pattern unprint2Pat = Pattern.compile("0E 02 0E 02 0E 02 0E 02 01 02 (.+) 00");
      Pattern inatPat = Pattern.compile("7F 0E 02 0E 02 (.*)0E 02 0E 02 01 02 02 (.+) 00");
      Pattern inat2Pat = Pattern.compile("7F 0E 02 0E 02 (.*)0E 02 0E 02 01 (02 02 (.+))?01 01 (.*)FF 02 80 FF 80 00");

      Map<Character,String[]> inlineCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> unprintCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> unprint2Codes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inatInlineCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inatExtraCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inat2Codes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inat2ExtraCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inat2CrazyCodes = new TreeMap<Character,String[]>();


      Cursor cursor = CursorBuilder.createCursor(index);
      while(cursor.moveToNextRow()) {
//       System.out.println("=======");
//       System.out.println("Savepoint: " + cursor.getSavepoint());
//       System.out.println("Value: " + cursor.getCurrentRow());
        Cursor.Savepoint savepoint = cursor.getSavepoint();
        String entryStr = entryToString(savepoint.getCurrentPosition());

        Row row = cursor.getCurrentRow();
        String value = row.getString("data");
        String key = row.getString("key");
        char c = value.charAt(2);

        System.out.println("=======");
        System.out.println("RowId: " +
                           savepoint.getCurrentPosition().getRowId());
        System.out.println("Entry: " + entryStr);
//         System.out.println("Row: " + row);
        System.out.println("Value: (" + key + ")" + value);
        System.out.println("Char: " + c + ", " + (int)c + ", " +
                           toUnicodeStr(c));

        String type = null;
        if(entryStr.endsWith("01 00")) {

          // handle inline codes
          type = "INLINE";
          Matcher m = inlinePat.matcher(entryStr);
          m.find();
          handleInlineEntry(m.group(1), c, inlineCodes);

        } else if(entryStr.contains("01 01 01 80")) {

          // handle most unprintable codes
          type = "UNPRINTABLE";
          Matcher m = unprintPat.matcher(entryStr);
          m.find();
          handleUnprintableEntry(m.group(2), c, unprintCodes);

        } else if(entryStr.contains("01 02 02") &&
                  !entryStr.contains("FF 02 80 FF 80")) {

          // handle chars w/ symbols
          type = "CHAR_WITH_SYMBOL";
          Matcher m = inatPat.matcher(entryStr);
          m.find();
          handleInternationalEntry(m.group(1), m.group(2), c,
                                   inatInlineCodes, inatExtraCodes);

        } else if(entryStr.contains("0E 02 0E 02 0E 02 0E 02 01 02")) {

          // handle chars w/ symbols
          type = "UNPRINTABLE_2";
          Matcher m = unprint2Pat.matcher(entryStr);
          m.find();
          handleUnprintable2Entry(m.group(1), c, unprint2Codes);

        } else if(entryStr.contains("FF 02 80 FF 80")) {

          type = "CRAZY_INAT";
          Matcher m = inat2Pat.matcher(entryStr);
          m.find();
          handleInternational2Entry(m.group(1), m.group(3), m.group(4), c,
                                    inat2Codes, inat2ExtraCodes,
                                    inat2CrazyCodes);

        } else {

          // throw new RuntimeException("unhandled " + entryStr);
          System.out.println("unhandled " + entryStr);
        }

        System.out.println("Type: " + type);
      }

      System.out.println("\n***CODES");
      for(int i = 0; i <= 0xFFFF; ++i) {

        if(i == 256) {
          System.out.println("\n***EXTENDED CODES");
        }

        // skip non-char chars
        char c = (char)i;
        if(Character.isHighSurrogate(c) || Character.isLowSurrogate(c)) {
          continue;
        }

        if(c == (char)0xFFFE) {
          // this gets replaced with FFFD, treat it the same
          c = (char)0xFFFD;
        }

        Character cc = c;
        String[] chars = inlineCodes.get(cc);
        if(chars != null) {
          if((chars.length == 1) && (chars[0].length() == 0)) {
            System.out.println("X");
          } else {
            System.out.println("S" + toByteString(chars));
          }
          continue;
        }

        chars = inatInlineCodes.get(cc);
        if(chars != null) {
          String[] extra = inatExtraCodes.get(cc);
          System.out.println("I" + toByteString(chars) + "," +
                             toByteString(extra));
          continue;
        }

        chars = unprintCodes.get(cc);
        if(chars != null) {
          System.out.println("U" + toByteString(chars));
          continue;
        }

        chars = unprint2Codes.get(cc);
        if(chars != null) {
          if(chars.length > 1) {
            throw new RuntimeException("long unprint codes");
          }
          int val = Integer.parseInt(chars[0], 16) - 2;
          String valStr = ByteUtil.toHexString(new byte[]{(byte)val}).trim();
          System.out.println("P" + valStr);
          continue;
        }

        chars = inat2Codes.get(cc);
        if(chars != null) {
          String [] crazyCodes = inat2CrazyCodes.get(cc);
          String crazyCode = "";
          if(crazyCodes != null) {
            if((crazyCodes.length != 1) || !"A0".equals(crazyCodes[0])) {
              throw new RuntimeException("CC " + Arrays.asList(crazyCodes));
            }
            crazyCode = "1";
          }

          String[] extra = inat2ExtraCodes.get(cc);
          System.out.println("Z" + toByteString(chars) + "," +
                             toByteString(extra) + "," +
                             crazyCode);
          continue;
        }

        throw new RuntimeException("Unhandled char " + toUnicodeStr(c));
      }
      System.out.println("\n***END CODES");
    }
  }

  public void x_testReverseIsoMdb() throws Exception
  {
    try (Database db = open(Database.FileFormat.V2000, new File("/data2/jackcess_test/testAllIndexCodes3.mdb"))) {

      Table t = db.getTable("Table1");
      Index index = t.getIndexes().iterator().next();
      ((IndexImpl)index).initialize();
      System.out.println("Ind " + index);

      Pattern inlinePat = Pattern.compile("7F 4A 4A (.*)4A 4A 01 00");
      Pattern unprintPat = Pattern.compile("01 01 01 80 (.+) 06 (.+) 00");
      Pattern unprint2Pat = Pattern.compile("4A 4A 4A 4A 01 02 (.+) 00");
      Pattern inatPat = Pattern.compile("7F 4A 4A (.*)4A 4A 01 02 02 (.+) 00");
      Pattern inat2Pat = Pattern.compile("7F 4A 4A (.*)4A 4A 01 (02 02 (.+))?01 01 (.*)FF 02 80 FF 80 00");

      Map<Character,String[]> inlineCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> unprintCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> unprint2Codes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inatInlineCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inatExtraCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inat2Codes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inat2ExtraCodes = new TreeMap<Character,String[]>();
      Map<Character,String[]> inat2CrazyCodes = new TreeMap<Character,String[]>();


      Cursor cursor = CursorBuilder.createCursor(index);
      while(cursor.moveToNextRow()) {
//       System.out.println("=======");
//       System.out.println("Savepoint: " + cursor.getSavepoint());
//       System.out.println("Value: " + cursor.getCurrentRow());
        Cursor.Savepoint savepoint = cursor.getSavepoint();
        String entryStr = entryToString(savepoint.getCurrentPosition());

        Row row = cursor.getCurrentRow();
        String value = row.getString("data");
        String key = row.getString("key");
        char c = value.charAt(2);
        System.out.println("=======");
        System.out.println("RowId: " +
                           savepoint.getCurrentPosition().getRowId());
        System.out.println("Entry: " + entryStr);
//         System.out.println("Row: " + row);
        System.out.println("Value: (" + key + ")" + value);
        System.out.println("Char: " + c + ", " + (int)c + ", " +
                           toUnicodeStr(c));

        String type = null;
        if(entryStr.endsWith("01 00")) {

          // handle inline codes
          type = "INLINE";
          Matcher m = inlinePat.matcher(entryStr);
          m.find();
          handleInlineEntry(m.group(1), c, inlineCodes);

        } else if(entryStr.contains("01 01 01 80")) {

          // handle most unprintable codes
          type = "UNPRINTABLE";
          Matcher m = unprintPat.matcher(entryStr);
          m.find();
          handleUnprintableEntry(m.group(2), c, unprintCodes);

        } else if(entryStr.contains("01 02 02") &&
                  !entryStr.contains("FF 02 80 FF 80")) {

          // handle chars w/ symbols
          type = "CHAR_WITH_SYMBOL";
          Matcher m = inatPat.matcher(entryStr);
          m.find();
          handleInternationalEntry(m.group(1), m.group(2), c,
                                   inatInlineCodes, inatExtraCodes);

        } else if(entryStr.contains("4A 4A 4A 4A 01 02")) {

          // handle chars w/ symbols
          type = "UNPRINTABLE_2";
          Matcher m = unprint2Pat.matcher(entryStr);
          m.find();
          handleUnprintable2Entry(m.group(1), c, unprint2Codes);

        } else if(entryStr.contains("FF 02 80 FF 80")) {

          type = "CRAZY_INAT";
          Matcher m = inat2Pat.matcher(entryStr);
          m.find();
          handleInternational2Entry(m.group(1), m.group(3), m.group(4), c,
                                    inat2Codes, inat2ExtraCodes,
                                    inat2CrazyCodes);

        } else {

          throw new RuntimeException("unhandled " + entryStr);
        }

        System.out.println("Type: " + type);
      }

      System.out.println("\n***CODES");
      for(int i = 0; i <= 0xFFFF; ++i) {

        if(i == 256) {
          System.out.println("\n***EXTENDED CODES");
        }

        // skip non-char chars
        char c = (char)i;
        if(Character.isHighSurrogate(c) || Character.isLowSurrogate(c)) {
          continue;
        }

        if(c == (char)0xFFFE) {
          // this gets replaced with FFFD, treat it the same
          c = (char)0xFFFD;
        }

        Character cc = c;
        String[] chars = inlineCodes.get(cc);
        if(chars != null) {
          if((chars.length == 1) && (chars[0].length() == 0)) {
            System.out.println("X");
          } else {
            System.out.println("S" + toByteString(chars));
          }
          continue;
        }

        chars = inatInlineCodes.get(cc);
        if(chars != null) {
          String[] extra = inatExtraCodes.get(cc);
          System.out.println("I" + toByteString(chars) + "," +
                             toByteString(extra));
          continue;
        }

        chars = unprintCodes.get(cc);
        if(chars != null) {
          System.out.println("U" + toByteString(chars));
          continue;
        }

        chars = unprint2Codes.get(cc);
        if(chars != null) {
          if(chars.length > 1) {
            throw new RuntimeException("long unprint codes");
          }
          int val = Integer.parseInt(chars[0], 16) - 2;
          String valStr = ByteUtil.toHexString(new byte[]{(byte)val}).trim();
          System.out.println("P" + valStr);
          continue;
        }

        chars = inat2Codes.get(cc);
        if(chars != null) {
          String [] crazyCodes = inat2CrazyCodes.get(cc);
          String crazyCode = "";
          if(crazyCodes != null) {
            if((crazyCodes.length != 1) || !"A0".equals(crazyCodes[0])) {
              throw new RuntimeException("CC " + Arrays.asList(crazyCodes));
            }
            crazyCode = "1";
          }

          String[] extra = inat2ExtraCodes.get(cc);
          System.out.println("Z" + toByteString(chars) + "," +
                             toByteString(extra) + "," +
                             crazyCode);
          continue;
        }

        throw new RuntimeException("Unhandled char " + toUnicodeStr(c));
      }
      System.out.println("\n***END CODES");
    }
  }

  private static String toByteString(String[] chars)
  {
    String str = join(chars, "", "");
    if(str.length() > 0 && str.charAt(0) == '0') {
      str = str.substring(1);
    }
    return str;
  }

  private static void handleInlineEntry(
      String entryCodes, char c, Map<Character,String[]> inlineCodes)
    throws Exception
  {
    inlineCodes.put(c, entryCodes.trim().split(" "));
  }

  private static void handleUnprintableEntry(
      String entryCodes, char c, Map<Character,String[]> unprintCodes)
    throws Exception
  {
    unprintCodes.put(c, entryCodes.trim().split(" "));
  }

  private static void handleUnprintable2Entry(
      String entryCodes, char c, Map<Character,String[]> unprintCodes)
    throws Exception
  {
    unprintCodes.put(c, entryCodes.trim().split(" "));
  }

  private static void handleInternationalEntry(
      String inlineCodes, String entryCodes, char c,
      Map<Character,String[]> inatInlineCodes,
      Map<Character,String[]> inatExtraCodes)
    throws Exception
  {
    inatInlineCodes.put(c, inlineCodes.trim().split(" "));
    inatExtraCodes.put(c, entryCodes.trim().split(" "));
  }

  private static void handleInternational2Entry(
      String inlineCodes, String entryCodes, String crazyCodes, char c,
      Map<Character,String[]> inatInlineCodes,
      Map<Character,String[]> inatExtraCodes,
      Map<Character,String[]> inatCrazyCodes)
    throws Exception
  {
    inatInlineCodes.put(c, inlineCodes.trim().split(" "));
    if(entryCodes != null) {
      inatExtraCodes.put(c, entryCodes.trim().split(" "));
    }
    if((crazyCodes != null) && (crazyCodes.length() > 0)) {
      inatCrazyCodes.put(c, crazyCodes.trim().split(" "));
    }
  }

  public static String toUnicodeStr(Object obj) {
    StringBuilder sb = new StringBuilder();
    for(char c : obj.toString().toCharArray()) {
      sb.append(toUnicodeStr(c)).append(" ");
    }
    return sb.toString();
  }

  private static String toUnicodeStr(char c) {
    String specialStr = SPECIAL_CHARS.get(c);
    if(specialStr != null) {
      return specialStr;
    }

    String digits = Integer.toHexString(c).toUpperCase();
    while(digits.length() < 4) {
      digits = "0" + digits;
    }
    return "\\u" + digits;
  }

  private static String join(String[] strs, String joinStr, String prefixStr) {
    if(strs == null) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < strs.length; ++i) {
      if(strs[i].length() == 0) {
        continue;
      }
      builder.append(prefixStr).append(strs[i]);
      if(i < (strs.length - 1)) {
        builder.append(joinStr);
      }
    }
    return builder.toString();
  }

  public static String entryToString(Cursor.Position curPos)
    throws Exception
  {
    Field eField = curPos.getClass().getDeclaredField("_entry");
    eField.setAccessible(true);
    IndexData.Entry entry = (IndexData.Entry)eField.get(curPos);
    Field ebField = entry.getClass().getDeclaredField("_entryBytes");
    ebField.setAccessible(true);
    byte[] entryBytes = (byte[])ebField.get(entry);

    return ByteUtil.toHexString(ByteBuffer.wrap(entryBytes),
                                0, entryBytes.length, false);
  }

}
