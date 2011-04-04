/*
Copyright (c) 2008 Health Market Science, Inc.

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

import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.DatabaseTest.*;
import static com.healthmarketscience.jackcess.JetFormatTest.*;


/**
 * @author James Ahlborn
 */
public class IndexCodesTest extends TestCase {

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
  
  public IndexCodesTest(String name) throws Exception {
    super(name);
  }

  public void testIndexCodes() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX_CODES)) {
      Database db = open(testDB);

      for(Table t : db) {
        for(Index index : t.getIndexes()) {
  //         System.out.println("Checking " + t.getName() + "." + index.getName());
          checkIndexEntries(testDB, t, index);
        }
      }

      db.close();
    }
  }

  private static void checkIndexEntries(final TestDB testDB, Table t, Index index) throws Exception
  {
//         index.initialize();
//         System.out.println("Ind " + index);

    Cursor cursor = Cursor.createIndexCursor(t, index);
    while(cursor.moveToNextRow()) {

      Map<String,Object> row = cursor.getCurrentRow();
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
                              Map<String,Object> expectedRow,
                              Cursor.Position expectedPos)
    throws Exception
  {
    Object[] idxRow = index.constructIndexRow(expectedRow);
    Cursor cursor = Cursor.createIndexCursor(t, index, idxRow, idxRow);

    Cursor.Position startPos = cursor.getSavepoint().getCurrentPosition();
    
    cursor.beforeFirst();
    while(cursor.moveToNextRow()) {
      Map<String,Object> row = cursor.getCurrentRow();
      if(expectedRow.equals(row)) {
        // verify that the entries are indeed equal
        Cursor.Position curPos = cursor.getSavepoint().getCurrentPosition();
        assertEquals(entryToString(expectedPos), entryToString(curPos));
        return;
      }
    }

    // TODO long rows not handled completely yet in V2010
    // seems to truncate entry at 508 bytes with some trailing 2 byte seq
    if(testDB.getExpectedFileFormat() == Database.FileFormat.V2010) {
      String rowId = (String)expectedRow.get("name");
      String tName = t.getName();
      if(("Table11".equals(tName) || "Table11_desc".equals(tName)) &&
         ("row10".equals(rowId) || "row11".equals(rowId) || 
          "row12".equals(rowId))) {
        System.out.println(
            "TODO long rows not handled completely yet in V2010: " + tName +
                           ", " + rowId);
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
  
  public void testNothing() throws Exception {
    // keep this so build doesn't fail if other tests are disabled
  }

  public void x_testCreateIsoFile() throws Exception
  {
    Database db = create(Database.FileFormat.V2000, true);

    Table t = new TableBuilder("test")
      .addColumn(new ColumnBuilder("row", DataType.TEXT))
      .addColumn(new ColumnBuilder("data", DataType.TEXT))
      .toTable(db);
    
    for(int i = 0; i < 256; ++i) {
      String str = "AA" + ((char)i) + "AA";
      t.addRow("row" + i, str);
    }

    db.close();
  }

  public void x_testCreateAltIsoFile() throws Exception
  {
    Database db = openCopy(Database.FileFormat.V2000, new File("/tmp/test_ind.mdb"), true);

    Table t = db.getTable("Table1");

    for(int i = 0; i < 256; ++i) {
      String str = "AA" + ((char)i) + "AA";
      t.addRow("row" + i, str,
               (byte)42 + i, (short)53 + i, 13 * i,
               (6.7d / i), null, null, true);
    }
    
    db.close();
  }

  public void x_testWriteAllCodesMdb() throws Exception
  {
    Database db = create(Database.FileFormat.V2000, true);

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


    db.close();
  }

  public void x_testReadAllCodesMdb() throws Exception
  {
//     Database db = openCopy(new File("/data2/jackcess_test/testAllIndexCodes.mdb"));
//     Database db = openCopy(new File("/data2/jackcess_test/testAllIndexCodes_orig.mdb"));
//     Database db = openCopy(new File("/data2/jackcess_test/testSomeMoreCodes.mdb"));
    Database db = openCopy(Database.FileFormat.V2000, new File("/data2/jackcess_test/testStillMoreCodes.mdb"));
    Table t = db.getTable("Table5");

    Index ind = t.getIndexes().iterator().next();
    ind.initialize();
    
    System.out.println("Ind " + ind);

    Cursor cursor = Cursor.createIndexCursor(t, ind);
    while(cursor.moveToNextRow()) {
      System.out.println("=======");
      String entryStr = 
        entryToString(cursor.getSavepoint().getCurrentPosition());
      System.out.println("Entry Bytes: " + entryStr);
      System.out.println("Value: " + cursor.getCurrentRow() + "; " +
                         toUnicodeStr(cursor.getCurrentRow().get("data")));
    }

    db.close();
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
    Database db = open(Database.FileFormat.V2000, new File("/tmp/test_ind3.mdb"));
//     Database db = open(new File("/tmp/test_ind4.mdb"));

    Table t = db.getTable("Table1");
    Index index = t.getIndex("B");
    index.initialize();
    System.out.println("Ind " + index);

    Cursor cursor = Cursor.createIndexCursor(t, index);
    while(cursor.moveToNextRow()) {
      System.out.println("=======");
      System.out.println("Savepoint: " + cursor.getSavepoint());
      System.out.println("Value: " + cursor.getCurrentRow());
    }
    
    db.close();
  }
  
  public void x_testReverseIsoMdb2010() throws Exception
  {
    Database db = open(Database.FileFormat.V2010, new File("/data2/jackcess_test/testAllIndexCodes3_2010.accdb"));

    Table t = db.getTable("Table1");
    Index index = t.getIndexes().iterator().next();
    index.initialize();
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
    
    
    Cursor cursor = Cursor.createIndexCursor(t, index);
    while(cursor.moveToNextRow()) {
//       System.out.println("=======");
//       System.out.println("Savepoint: " + cursor.getSavepoint());
//       System.out.println("Value: " + cursor.getCurrentRow());
      Cursor.Savepoint savepoint = cursor.getSavepoint();
      String entryStr = entryToString(savepoint.getCurrentPosition());

      Map<String,Object> row = cursor.getCurrentRow();
      String value = (String)row.get("data");
      String key = (String)row.get("key");
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
    
    db.close();
  }
 
  public void x_testReverseIsoMdb() throws Exception
  {
    Database db = open(Database.FileFormat.V2000, new File("/data2/jackcess_test/testAllIndexCodes3.mdb"));

    Table t = db.getTable("Table1");
    Index index = t.getIndexes().iterator().next();
    index.initialize();
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
    
    
    Cursor cursor = Cursor.createIndexCursor(t, index);
    while(cursor.moveToNextRow()) {
//       System.out.println("=======");
//       System.out.println("Savepoint: " + cursor.getSavepoint());
//       System.out.println("Value: " + cursor.getCurrentRow());
      Cursor.Savepoint savepoint = cursor.getSavepoint();
      String entryStr = entryToString(savepoint.getCurrentPosition());

      Map<String,Object> row = cursor.getCurrentRow();
      String value = (String)row.get("data");
      String key = (String)row.get("key");
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
    
    db.close();
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

  private static String toUnicodeStr(Object obj) throws Exception {
    StringBuilder sb = new StringBuilder();
    for(char c : obj.toString().toCharArray()) {
      sb.append(toUnicodeStr(c)).append(" ");
    }
    return sb.toString();
  }
  
  private static String toUnicodeStr(char c) throws Exception {
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
  
  static String entryToString(Cursor.Position curPos)
    throws Exception
  {
    Field eField = curPos.getClass().getDeclaredField("_entry");
    eField.setAccessible(true);
    IndexData.Entry entry = (IndexData.Entry)eField.get(curPos);
    Field ebField = entry.getClass().getDeclaredField("_entryBytes");
    ebField.setAccessible(true);
    byte[] entryBytes = (byte[])ebField.get(entry);

    return ByteUtil.toHexString(ByteBuffer.wrap(entryBytes),
                                entryBytes.length)
      .trim().replaceAll("\\p{Space}+", " ");
  }
  
}
