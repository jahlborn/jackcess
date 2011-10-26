// Copyright (c) 2011 Boomi, Inc.

package com.healthmarketscience.jackcess;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.healthmarketscience.jackcess.JetFormatTest.*;
import static com.healthmarketscience.jackcess.DatabaseTest.*;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.complex.SingleValue;
import com.healthmarketscience.jackcess.complex.Version;
import junit.framework.TestCase;


/**
 *
 * @author James Ahlborn
 */
public class ComplexColumnTest extends TestCase 
{

  public ComplexColumnTest(String name) {
    super(name);
  }

  public void testVersions() throws Exception
  {
    for(final TestDB testDB : TestDB.getSupportedForBasename(Basename.COMPLEX)) {
      
      Database db = openCopy(testDB);
      db.setTimeZone(TEST_TZ);

      Table t1 = db.getTable("Table1");
      Column col = t1.getColumn("append-memo-data");
      assertTrue(col.isAppendOnly());
      Column verCol = col.getVersionHistoryColumn();
      assertNotNull(verCol);
      assertEquals(ComplexDataType.VERSION_HISTORY,
                   verCol.getComplexInfo().getType());

      for(Map<String,Object> row : t1) {
        String rowId = (String)row.get("id");
        ComplexValueForeignKey complexValueFk =
          (ComplexValueForeignKey)verCol.getRowValue(row);

        String curValue = (String)col.getRowValue(row);
      
        if(rowId.equals("row1")) {
          checkVersions(1, complexValueFk, curValue);
        } else if(rowId.equals("row2")) {
          checkVersions(2, complexValueFk, curValue,
                        "row2-memo", new Date(1315876862334L));
        } else if(rowId.equals("row3")) {
          checkVersions(3, complexValueFk, curValue,
                        "row3-memo-again", new Date(1315876965382L),
                        "row3-memo-revised", new Date(1315876953077L),
                        "row3-memo", new Date(1315876879126L));
        } else if(rowId.equals("row4")) {
          checkVersions(4, complexValueFk, curValue,
                        "row4-memo", new Date(1315876945758L));
        } else {
          assertTrue(false);
        }
      }

      Object[] row8 = {"row8", Column.AUTO_NUMBER, "some-data", "row8-memo",
                       Column.AUTO_NUMBER, Column.AUTO_NUMBER};
      t1.addRow(row8);

      ComplexValueForeignKey row8ValFk = (ComplexValueForeignKey)
        verCol.getRowValue(row8);
      Date upTime = new Date();
      row8ValFk.addVersion("row8-memo", upTime);
      checkVersions(8, row8ValFk, "row8-memo",
                    "row8-memo", upTime);    

      Cursor cursor = Cursor.createCursor(t1);
      assertTrue(cursor.findRow(t1.getColumn("id"), "row3"));
      ComplexValueForeignKey row3ValFk = (ComplexValueForeignKey)
        cursor.getCurrentRowValue(verCol);
      cursor.setCurrentRowValue(col, "new-value");
      Version v = row3ValFk.addVersion("new-value", upTime);
      checkVersions(3, row3ValFk, "new-value",
                    "new-value", upTime,
                    "row3-memo-again", new Date(1315876965382L),
                    "row3-memo-revised", new Date(1315876953077L),
                    "row3-memo", new Date(1315876879126L));

      try {
        v.update();
        fail("UnsupportedOperationException should have been thrown");
      } catch(UnsupportedOperationException expected) {
        // success
      }
    
      db.close();
    }
  }

  public void testAttachments() throws Exception
  {
    for(final TestDB testDB : TestDB.getSupportedForBasename(Basename.COMPLEX)) {
      
      Database db = openCopy(testDB);
      db.setTimeZone(TEST_TZ);

      Table t1 = db.getTable("Table1");
      Column col = t1.getColumn("attach-data");
      assertEquals(ComplexDataType.ATTACHMENT,
                   col.getComplexInfo().getType());

      for(Map<String,Object> row : t1) {
        String rowId = (String)row.get("id");
        ComplexValueForeignKey complexValueFk =
          (ComplexValueForeignKey)col.getRowValue(row);

        if(rowId.equals("row1")) {
          checkAttachments(1, complexValueFk);
        } else if(rowId.equals("row2")) {
          checkAttachments(2, complexValueFk, "test_data.txt", "test_data2.txt");
        } else if(rowId.equals("row3")) {
          checkAttachments(3, complexValueFk);
        } else if(rowId.equals("row4")) {
          checkAttachments(4, complexValueFk, "test_data2.txt");
        } else {
          assertTrue(false);
        }
      }

      Object[] row8 = {"row8", Column.AUTO_NUMBER, "some-data", "row8-memo",
                       Column.AUTO_NUMBER, Column.AUTO_NUMBER};
      t1.addRow(row8);

      ComplexValueForeignKey row8ValFk = (ComplexValueForeignKey)
        col.getRowValue(row8);
      row8ValFk.addAttachment(null, "test_data.txt", "txt",
                              getFileBytes("test_data.txt"), null, null);
      checkAttachments(8, row8ValFk, "test_data.txt");

      Cursor cursor = Cursor.createCursor(t1);
      assertTrue(cursor.findRow(t1.getColumn("id"), "row4"));
      ComplexValueForeignKey row4ValFk = (ComplexValueForeignKey)
        cursor.getCurrentRowValue(col);
      Attachment a = row4ValFk.addAttachment(null, "test_data.txt", "txt",
                                             getFileBytes("test_data.txt"), null,
                                             null);
      checkAttachments(4, row4ValFk, "test_data2.txt", "test_data.txt");

      a.setFileType("xml");
      a.setFileName("some_data.xml");
      byte[] newBytes = "this is not xml".getBytes("US-ASCII");
      a.setFileData(newBytes);
      a.update();

      Attachment updated = row4ValFk.getAttachments().get(1);
      assertNotSame(updated, a);
      assertEquals("xml", updated.getFileType());
      assertEquals("some_data.xml", updated.getFileName());
      assertTrue(Arrays.equals(newBytes, updated.getFileData()));
    
      db.close();
    }
  }

  public void testMultiValues() throws Exception
  {
    for(final TestDB testDB : TestDB.getSupportedForBasename(Basename.COMPLEX)) {
      
      Database db = openCopy(testDB);
      db.setTimeZone(TEST_TZ);

      Table t1 = db.getTable("Table1");
      Column col = t1.getColumn("multi-value-data");
      assertEquals(ComplexDataType.MULTI_VALUE,
                   col.getComplexInfo().getType());

      for(Map<String,Object> row : t1) {
        String rowId = (String)row.get("id");
        ComplexValueForeignKey complexValueFk =
          (ComplexValueForeignKey)col.getRowValue(row);

        if(rowId.equals("row1")) {
          checkMultiValues(1, complexValueFk);
        } else if(rowId.equals("row2")) {
          checkMultiValues(2, complexValueFk, "value1", "value4");
        } else if(rowId.equals("row3")) {
          checkMultiValues(3, complexValueFk,
                           "value1", "value2", "value3", "value4");
        } else if(rowId.equals("row4")) {
          checkMultiValues(4, complexValueFk);
        } else {
          assertTrue(false);
        }
      }     

      Object[] row8 = {"row8", Column.AUTO_NUMBER, "some-data", "row8-memo",
                       Column.AUTO_NUMBER, Column.AUTO_NUMBER};
      t1.addRow(row8);

      ComplexValueForeignKey row8ValFk = (ComplexValueForeignKey)
        col.getRowValue(row8);
      row8ValFk.addMultiValue("value1");
      row8ValFk.addMultiValue("value2");
      checkMultiValues(8, row8ValFk, "value1", "value2");

      Cursor cursor = Cursor.createCursor(t1);
      assertTrue(cursor.findRow(t1.getColumn("id"), "row2"));
      ComplexValueForeignKey row2ValFk = (ComplexValueForeignKey)
        cursor.getCurrentRowValue(col);
      SingleValue v = row2ValFk.addMultiValue("value2");
      row2ValFk.addMultiValue("value3");
      checkMultiValues(2, row2ValFk, "value1", "value4", "value2", "value3");

      v.set("value5");
      v.update();
      checkMultiValues(2, row2ValFk, "value1", "value4", "value5", "value3");
    
      db.close();
    }
  }
  
  private static void checkVersions(
      int cValId, ComplexValueForeignKey complexValueFk,
      String curValue, Object... versionInfos)
    throws Exception
  {
    assertEquals(cValId, complexValueFk.get());

    List<Version> versions = complexValueFk.getVersions();
    if(versionInfos.length == 0) {
      assertTrue(versions.isEmpty());
      assertNull(curValue);
    } else {
      assertEquals(versionInfos.length / 2, versions.size());
      assertEquals(curValue, versions.get(0).getValue());
      for(int i = 0; i < versionInfos.length; i+=2) {
        String value = (String)versionInfos[i];
        Date modDate = (Date)versionInfos[i+1];
        Version v = versions.get(i/2);
        assertEquals(value, v.getValue());
        assertSameDate(modDate, v.getModifiedDate());
      }
    }
  }

  private static void checkAttachments(
      int cValId, ComplexValueForeignKey complexValueFk,
      String... fileNames)
    throws Exception
  {
    assertEquals(cValId, complexValueFk.get());
    
    List<Attachment> attachments = complexValueFk.getAttachments();
    if(fileNames.length == 0) {
      assertTrue(attachments.isEmpty());
    } else {
      assertEquals(fileNames.length, attachments.size());
      for(int i = 0; i < fileNames.length; ++i) {
        String fname = fileNames[i];
        byte[] dataBytes = getFileBytes(fname);
        Attachment a = attachments.get(i);
        assertEquals(fname, a.getFileName());
        assertEquals("txt", a.getFileType());
        assertTrue(Arrays.equals(dataBytes, a.getFileData()));
      }
    }
  }
  
  private static void checkMultiValues(
      int cValId, ComplexValueForeignKey complexValueFk,
      Object... expectedValues)
    throws Exception
  {
    assertEquals(cValId, complexValueFk.get());

    List<SingleValue> values = complexValueFk.getMultiValues();
    if(expectedValues.length == 0) {
      assertTrue(values.isEmpty());
    } else {
      assertEquals(expectedValues.length, values.size());
      for(int i = 0; i < expectedValues.length; ++i) {
        Object value = expectedValues[i];
        SingleValue v = values.get(i);
        assertEquals(value, v.get());
      }
    }    
  }

  private static byte[] getFileBytes(String fname) throws Exception
  {
    if("test_data.txt".equals(fname)) {
      return TEST_BYTES;
    }
    if("test_data2.txt".equals(fname)) {
      return TEST2_BYTES;
    }
    throw new RuntimeException("unexpected bytes");
  }
  
  private static byte b(int i) { return (byte)i; }
  
  private static final byte[] TEST_BYTES = new byte[] {
    b(0x01),b(0x00),b(0x00),b(0x00),b(0x3A),b(0x00),b(0x00),b(0x00),b(0x78),b(0x5E),b(0x13),b(0x61),b(0x60),b(0x60),b(0x60),b(0x04),b(0x62),b(0x16),b(0x20),b(0x2E),b(0x61),b(0xA8),b(0x00),b(0x62),
    b(0x20),b(0x9D),b(0x91),b(0x59),b(0xAC),b(0x00),b(0x44),b(0xC5),b(0xF9),b(0xB9),b(0xA9),b(0x0A),b(0x25),b(0xA9),b(0xC5),b(0x25),b(0x0A),b(0x29),b(0x89),b(0x25),b(0x89),b(0x0A),b(0x69),b(0xF9),
    b(0x45),b(0x0A),b(0x89),b(0x25),b(0x25),b(0x89),b(0xC9),b(0x19),b(0xB9),b(0xA9),b(0x79),b(0x25),b(0x7A),b(0x00),b(0x52),b(0xA9),b(0x0F),b(0x7A)
  };
  
  private static final byte[] TEST2_BYTES = new byte[] {
    b(0x01),b(0x00),b(0x00),b(0x00),b(0x3F),b(0x00),b(0x00),b(0x00),b(0x78),b(0x5E),b(0x13),b(0x61),b(0x60),b(0x60),b(0x60),b(0x04),b(0x62),b(0x16),b(0x20),b(0x2E),b(0x61),b(0xA8),b(0x00),b(0x62),
    b(0x20),b(0x9D),b(0x91),b(0x59),b(0xAC),b(0x00),b(0x44),b(0xC5),b(0xF9),b(0xB9),b(0xA9),b(0x0A),b(0xB9),b(0xF9),b(0x45),b(0xA9),b(0x0A),b(0x25),b(0xA9),b(0xC5),b(0x25),b(0x0A),b(0x29),b(0x89),
    b(0x25),b(0x89),b(0x0A),b(0x69),b(0xF9),b(0x45),b(0x0A),b(0x89),b(0x25),b(0x25),b(0x89),b(0xC9),b(0x19),b(0xB9),b(0xA9),b(0x79),b(0x25),b(0x7A),b(0x00),b(0xA5),b(0x0B),b(0x11),b(0x4D)
  };
}
