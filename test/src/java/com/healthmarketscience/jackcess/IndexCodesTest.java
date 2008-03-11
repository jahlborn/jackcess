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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.DatabaseTest.*;


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
    Database db = Database.open(new File("test/data/testIndexCodes.mdb"));

    for(Table t : db) {
      for(Index index : t.getIndexes()) {
//         System.out.println("Checking " + t.getName() + "." + index.getName());
        checkIndexEntries(t, index);
      }
    }
    
    db.close();
  }

  private static void checkIndexEntries(Table t, Index index) throws Exception
  {
//         index.initialize();
//         System.out.println("Ind " + index);

    Cursor cursor = Cursor.createIndexCursor(t, index);
    while(cursor.moveToNextRow()) {

      Map<String,Object> row = cursor.getCurrentRow();
      Cursor.Position curPos = cursor.getSavepoint().getCurrentPosition();
      boolean success = false;
      try {
        findRow(t, index, row, curPos);
        success = true;
      } finally {
        if(!success) {
          System.out.println("CurPos: " + curPos);
          System.out.println("Value: " + row);
        }          
      }
    }
    
  }
  
  private static void findRow(Table t, Index index,
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
    fail("Could not find expected row " + expectedRow + " starting at " +
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
    Database db = create(true);

    List<Column> columns = new ArrayList<Column>();
    Column col = new Column();
    col.setName("row");
    col.setType(DataType.TEXT);
    columns.add(col);
    col = new Column();
    col.setName("data");
    col.setType(DataType.TEXT);
    columns.add(col);
    
    db.createTable("test", columns);

    Table t = db.getTable("test");

    for(int i = 0; i < 256; ++i) {
      String str = "AA" + ((char)i) + "AA";
      t.addRow("row" + i, str);
    }

    db.close();
  }

  public void x_testCreateAltIsoFile() throws Exception
  {
    Database db = openCopy(new File("/tmp/test_ind.mdb"), true);

    Table t = db.getTable("Table1");

    for(int i = 0; i < 256; ++i) {
      String str = "AA" + ((char)i) + "AA";
      t.addRow("row" + i, str,
               (byte)42 + i, (short)53 + i, 13 * i,
               (6.7d / i), null, null, true);
    }
    
    db.close();
  }

  public void x_testReadIsoMdb() throws Exception
  {
//     Database db = Database.open(new File("/tmp/test_ind.mdb"));
//     Database db = Database.open(new File("/tmp/test_ind2.mdb"));
    Database db = Database.open(new File("/tmp/test_ind3.mdb"));
//     Database db = Database.open(new File("/tmp/test_ind4.mdb"));

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
    
  public void x_testReverseIsoMdb() throws Exception
  {
//     Database db = Database.open(new File("/tmp/test_ind.mdb"));
    Database db = Database.open(new File("/tmp/test_ind2.mdb"));
//     Database db = Database.open(new File("/tmp/databaseTest14366_ind.mdb"));
//     Database db = Database.open(new File("/tmp/databaseTest56165_ind.mdb"));
//     Database db = Database.open(new File("/tmp/databaseTest53970_ind.mdb"));

    Table t = db.getTable("Table1");
    Index index = t.getIndex("B");
    index.initialize();
    System.out.println("Ind " + index);

    Pattern inlinePat = Pattern.compile("7F 4A 4A (.*)4A 4A 01 00");
    Pattern unprintPat = Pattern.compile("01 01 01 80 (.+) 06 (.+) 00");
    Pattern inatPat = Pattern.compile("7F 4A 4A (.*)4A 4A 01 02 02 (.+) 00");

    Map<Character,String[]> inlineCodes = new TreeMap<Character,String[]>();
    Map<Character,String[]> unprintCodes = new TreeMap<Character,String[]>();
    Map<Character,String[]> inatInlineCodes = new TreeMap<Character,String[]>();
    Map<Character,String[]> inatExtraCodes = new TreeMap<Character,String[]>();
    
    Cursor cursor = Cursor.createIndexCursor(t, index);
    while(cursor.moveToNextRow()) {
//       System.out.println("=======");
//       System.out.println("Savepoint: " + cursor.getSavepoint());
//       System.out.println("Value: " + cursor.getCurrentRow());
      Cursor.Savepoint savepoint = cursor.getSavepoint();
      String entryStr = entryToString(savepoint.getCurrentPosition());

      Map<String,Object> row = cursor.getCurrentRow();
      String value = (String)row.get("B");
      char c = value.charAt(2);
      System.out.println("=======");
      System.out.println("RowId: " +
                         savepoint.getCurrentPosition().getRowId());
      System.out.println("Entry: " + entryStr);
//         System.out.println("Row: " + row);
      System.out.println("Value: " + value);
      System.out.println("Char: " + c + ", " + (int)c + ", " +
                         toUnicodeStr(c));

      String type = null;
      if(entryStr.endsWith("01 00")) {

        // handle inline codes
        type = "INLINE";
        Matcher m = inlinePat.matcher(entryStr);
        m.find();
        handleInlineEntry(m.group(1), c, inlineCodes);

      } else if(entryStr.contains("01 01 01")) {
        
        // handle most unprintable codes
        type = "UNPRINTABLE";
        Matcher m = unprintPat.matcher(entryStr);
        m.find();
        handleUnprintableEntry(m.group(2), c, unprintCodes);

      } else if(entryStr.contains("01 02 02")) {

        // handle chars w/ symbols
        type = "CHAR_WITH_SYMBOL";
        Matcher m = inatPat.matcher(entryStr);
        m.find();
        handleInternationalEntry(m.group(1), m.group(2), c,
                                 inatInlineCodes, inatExtraCodes);
        
      } else {

        throw new RuntimeException("unhandled " + entryStr);
      }      
        
      System.out.println("Type: " + type);
    }

//     System.out.println("Normal " + inlineCodes);
//     System.out.println("Unprintable " + unprintCodes);
//     System.out.println("International " + inatCodes);
    System.out.println("\n***INLINE");
    for(Map.Entry<Character,String[]> e : inlineCodes.entrySet()) {
      System.out.println(
          generateCodeString("registerCodes", e.getKey(), e.getValue(),
                             null));
    }
    System.out.println("\n***UNPRINTABLE");
    for(Map.Entry<Character,String[]> e : unprintCodes.entrySet()) {
      System.out.println(
          generateCodeString("registerUnprintableCodes",
                             e.getKey(), e.getValue(), null));
    }
    System.out.println("\n***INTERNATIONAL");
    for(Map.Entry<Character,String[]> e : inatInlineCodes.entrySet()) {
      
      System.out.println(
          generateCodeString("registerInternationalCodes",
                             e.getKey(), e.getValue(),
                             inatExtraCodes.get(e.getKey())));
    }
    
    db.close();
  }

  private static String generateCodeString(String methodName,
                                    char c,
                                    String[] charStrs1,
                                    String[] charStrs2)
  {
    StringBuilder builder = new StringBuilder()
      .append(methodName).append("('").append(toUnicodeStr(c))
      .append("', new byte[]{")
      .append(join(charStrs1, ", ", "(byte)0x"))
      .append("}");
    if(charStrs2 != null) {
      builder.append(",\nnew byte[]{")
        .append(join(charStrs2, ", ", "(byte)0x"))
        .append("}");
    }
    builder.append(");");
    return builder.toString();
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
  
  private static void handleInternationalEntry(
      String inlineCodes, String entryCodes, char c,
      Map<Character,String[]> inatInlineCodes,
      Map<Character,String[]> inatExtraCodes)
    throws Exception
  {
    inatInlineCodes.put(c, inlineCodes.trim().split(" "));
    inatExtraCodes.put(c, entryCodes.trim().split(" "));
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
    Index.Entry entry = (Index.Entry)eField.get(curPos);
    Field ebField = entry.getClass().getDeclaredField("_entryBytes");
    ebField.setAccessible(true);
    byte[] entryBytes = (byte[])ebField.get(entry);

    return ByteUtil.toHexString(ByteBuffer.wrap(entryBytes),
                                entryBytes.length)
      .trim().replaceAll("\\p{Space}+", " ");
  }
  
}
