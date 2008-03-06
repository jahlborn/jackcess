/*
Copyright (c) 2007 Health Market Science, Inc.

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
    for(Index idx : table.getIndexes()) {
      idx.initialize();
    }
    assertEquals(4, table.getIndexes().size());
    assertEquals(4, table.getIndexSlotCount());
    checkIndexColumns(table,
                      "id", "id",
                      "PrimaryKey", "id",
                      "Table2Table1", "otherfk1",
                      "Table3Table1", "otherfk2");
    
    table = mdb.getTable("Table2");
    for(Index idx : table.getIndexes()) {
      idx.initialize();
    }
    assertEquals(2, table.getIndexes().size());
    assertEquals(3, table.getIndexSlotCount());
    checkIndexColumns(table,
                      "id", "id",
                      "PrimaryKey", "id");

    table = mdb.getTable("Table3");
    for(Index idx : table.getIndexes()) {
      idx.initialize();
    }
    assertEquals(2, table.getIndexes().size());
    assertEquals(3, table.getIndexSlotCount());
    checkIndexColumns(table,
                      "id", "id",
                      "PrimaryKey", "id");
  }

  public void testComplexIndex() throws Exception
  {
    // this file has an index with "compressed" entries and node pages
    File origFile = new File("test/data/compIndexTest.mdb");
    Database db = Database.open(origFile);
    Table t = db.getTable("Table1");
    Index index = t.getIndexes().get(0);
    assertFalse(index.isInitialized());
    assertEquals(512, countRows(t));
    assertEquals(512, index.getEntryCount());
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

  public void testEntryDeletion() throws Exception {
    File srcFile = new File("test/data/test.mdb");
    File dbFile = File.createTempFile("databaseTest", ".mdb");
    dbFile.deleteOnExit();
    copyFile(srcFile, dbFile);

    Table table = Database.open(dbFile).getTable("Table1");

    for(int i = 0; i < 10; ++i) {
      table.addRow("foo" + i, "bar" + i, (byte)42 + i, (short)53 + i, 13 * i,
                   (6.7d / i), null, null, true);
    }
    table.reset();
    assertRowCount(12, table);

    for(Index index : table.getIndexes()) {
      assertEquals(12, index.getEntryCount());
    }

    table.reset();
    table.getNextRow();
    table.getNextRow();
    table.deleteCurrentRow();
    table.getNextRow();
    table.deleteCurrentRow();
    table.getNextRow();
    table.getNextRow();
    table.deleteCurrentRow();
    table.getNextRow();
    table.getNextRow();
    table.getNextRow();
    table.deleteCurrentRow();

    table.reset();
    assertRowCount(8, table);

    for(Index index : table.getIndexes()) {
      assertEquals(8, index.getEntryCount());
    }
  }

  private void checkIndexColumns(Table table, String... idxInfo)
    throws Exception
  {
    Map<String, String> expectedIndexes = new HashMap<String, String>();
    for(int i = 0; i < idxInfo.length; i+=2) {
      expectedIndexes.put(idxInfo[i], idxInfo[i+1]);
    }

    for(Index idx : table.getIndexes()) {
      String colName = expectedIndexes.get(idx.getName());
      assertEquals(1, idx.getColumns().size());
      assertEquals(colName, idx.getColumns().get(0).getName());
      if("PrimaryKey".equals(idx.getName())) {
        assertTrue(idx.isPrimaryKey());
      } else {
        assertFalse(idx.isPrimaryKey());
      }
    }
  }
  
}
