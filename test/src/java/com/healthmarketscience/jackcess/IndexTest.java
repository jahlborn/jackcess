// Copyright (c) 2006 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.DatabaseTest.*;

/**
 * @author James Ahlborn
 */
public class IndexTest extends TestCase {

  /**
   * Creates a new <code>IndexTest</code> instance.
   *
   */
  public IndexTest(String name) {
    super(name);
  }

  public void testByteOrder() throws Exception {
    byte b1 = (byte)0x00;
    byte b2 = (byte)0x01;
    byte b3 = (byte)0x7F;
    byte b4 = (byte)0x80;
    byte b5 = (byte)0xFF;

    assertTrue(ByteUtil.toUnsignedInt(b1) < ByteUtil.toUnsignedInt(b2));
    assertTrue(ByteUtil.toUnsignedInt(b2) < ByteUtil.toUnsignedInt(b3));
    assertTrue(ByteUtil.toUnsignedInt(b3) < ByteUtil.toUnsignedInt(b4));
    assertTrue(ByteUtil.toUnsignedInt(b4) < ByteUtil.toUnsignedInt(b5));
  }
  
  public void testByteCodeComparator() {
    // FIXME, writeme
    byte[] b0 = null;
    byte[] b1 = new byte[]{(byte)0x00};
    byte[] b2 = new byte[]{(byte)0x00, (byte)0x00};
    byte[] b3 = new byte[]{(byte)0x00, (byte)0x01};
    byte[] b4 = new byte[]{(byte)0x01};
    byte[] b5 = new byte[]{(byte)0x80};
    byte[] b6 = new byte[]{(byte)0xFF};
    byte[] b7 = new byte[]{(byte)0xFF, (byte)0x00};
    byte[] b8 = new byte[]{(byte)0xFF, (byte)0x01};

    List<byte[]> expectedList = Arrays.<byte[]>asList(b0, b1, b2, b3, b4,
                                                      b5, b6, b7, b8);
    SortedSet<byte[]> sortedSet = new TreeSet<byte[]>(
        Index.BYTE_CODE_COMPARATOR);
    sortedSet.addAll(expectedList);
    assertEquals(expectedList, new ArrayList<byte[]>(sortedSet));
    
  }

  public void testPrimaryKey() throws Exception {
    Table table = open().getTable("Table1");
    Map<String, Boolean> foundPKs = new HashMap<String, Boolean>();
    for(Index index : table.getIndexes()) {
      foundPKs.put(index.getColumns().iterator().next().getName(),
                   index.isPrimaryKey());
    }
    Map<String, Boolean> expectedPKs = new HashMap<String, Boolean>();
    expectedPKs.put("A", Boolean.TRUE);
    expectedPKs.put("B", Boolean.FALSE);
    assertEquals(expectedPKs, foundPKs);
  }
  
  public void testIndexSlots() throws Exception
  {
    Database mdb = Database.open(new File("test/data/indexTest.mdb"));

    Table table = mdb.getTable("Table1");
    assertEquals(4, table.getIndexes().size());
    assertEquals(4, table.getIndexSlotCount());

    table = mdb.getTable("Table2");
    assertEquals(2, table.getIndexes().size());
    assertEquals(3, table.getIndexSlotCount());

    table = mdb.getTable("Table3");
    assertEquals(2, table.getIndexes().size());
    assertEquals(3, table.getIndexSlotCount());
  }

  public void testComplexIndex() throws Exception
  {
    // this file has an index with "compressed" entries and node pages
    File origFile = new File("test/data/compIndexTest.mdb");
    Database db = Database.open(origFile);
    Table t = db.getTable("Table1");
    assertEquals(512, countRows(t));
    db.close();

    // copy to temp file and attemp to edit
    File testFile = File.createTempFile("databaseTest", ".mdb");
    testFile.deleteOnExit();

    copyFile(origFile, testFile);

    db = Database.open(testFile);
    t = db.getTable("Table1");
    
    try {
      // we don't support writing these indexes
      t.addRow(99, "abc", "def");
      fail("Should have thrown IOException");
    } catch(UnsupportedOperationException e) {
      // success
    }
  }

  
}
