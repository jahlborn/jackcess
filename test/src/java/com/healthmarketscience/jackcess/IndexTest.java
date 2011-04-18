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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.Database.*;
import static com.healthmarketscience.jackcess.DatabaseTest.*;
import static com.healthmarketscience.jackcess.JetFormatTest.*;

/**
 * @author James Ahlborn
 */
public class IndexTest extends TestCase {

  public IndexTest(String name) {
    super(name);
  }

  public void testByteOrder() throws Exception {
    byte b1 = (byte)0x00;
    byte b2 = (byte)0x01;
    byte b3 = (byte)0x7F;
    byte b4 = (byte)0x80;
    byte b5 = (byte)0xFF;

    assertTrue(ByteUtil.asUnsignedByte(b1) < ByteUtil.asUnsignedByte(b2));
    assertTrue(ByteUtil.asUnsignedByte(b2) < ByteUtil.asUnsignedByte(b3));
    assertTrue(ByteUtil.asUnsignedByte(b3) < ByteUtil.asUnsignedByte(b4));
    assertTrue(ByteUtil.asUnsignedByte(b4) < ByteUtil.asUnsignedByte(b5));
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
        IndexData.BYTE_CODE_COMPARATOR);
    sortedSet.addAll(expectedList);
    assertEquals(expectedList, new ArrayList<byte[]>(sortedSet));
    
  }

  public void testPrimaryKey() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {
      Table table = open(testDB).getTable("Table1");
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
  }
  
  public void testLogicalIndexes() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX, true)) {
      Database mdb = open(testDB);

      Table table = mdb.getTable("Table1");
      for(Index idx : table.getIndexes()) {
        idx.initialize();
      }
      assertEquals(4, table.getIndexes().size());
      assertEquals(4, table.getLogicalIndexCount());
      checkIndexColumns(table,
                        "id", "id",
                        "PrimaryKey", "id",
                        getRelationshipName(testDB.getExpectedFormat(), 
                                            "Table2Table1"), "otherfk1",
                        getRelationshipName(testDB.getExpectedFormat(), 
                                            "Table3Table1"), "otherfk2");

      table = mdb.getTable("Table2");
      for(Index idx : table.getIndexes()) {
        idx.initialize();
      }
      assertEquals(3, table.getIndexes().size());
      assertEquals(2, table.getIndexDatas().size());
      assertEquals(3, table.getLogicalIndexCount());
      checkIndexColumns(table,
                        "id", "id",
                        "PrimaryKey", "id",
                        ".rC", "id");

      Index pkIdx = table.getIndex("PrimaryKey");
      Index fkIdx = table.getIndex(".rC");
      assertNotSame(pkIdx, fkIdx);
      assertTrue(fkIdx.isForeignKey());
      assertSame(pkIdx.getIndexData(), fkIdx.getIndexData());
      IndexData indexData = pkIdx.getIndexData();
      assertEquals(Arrays.asList(pkIdx, fkIdx), indexData.getIndexes());
      assertSame(pkIdx, indexData.getPrimaryIndex());

      table = mdb.getTable("Table3");
      for(Index idx : table.getIndexes()) {
        idx.initialize();
      }
      assertEquals(3, table.getIndexes().size());
      assertEquals(2, table.getIndexDatas().size());
      assertEquals(3, table.getLogicalIndexCount());
      checkIndexColumns(table,
                        "id", "id",
                        "PrimaryKey", "id",
                        ".rC", "id");

      pkIdx = table.getIndex("PrimaryKey");
      fkIdx = table.getIndex(".rC");
      assertNotSame(pkIdx, fkIdx);
      assertTrue(fkIdx.isForeignKey());
      assertSame(pkIdx.getIndexData(), fkIdx.getIndexData());
      indexData = pkIdx.getIndexData();
      assertEquals(Arrays.asList(pkIdx, fkIdx), indexData.getIndexes());
      assertSame(pkIdx, indexData.getPrimaryIndex());
    }
  }

  public void testComplexIndex() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.COMP_INDEX)) {
      // this file has an index with "compressed" entries and node pages
      Database db = open(testDB);
      Table t = db.getTable("Table1");
      Index index = t.getIndexes().get(0);
      assertFalse(index.isInitialized());
      assertEquals(512, countRows(t));
      assertEquals(512, index.getIndexData().getEntryCount());
      db.close();

      // copy to temp file and attempt to edit
      db = openCopy(testDB);
      t = db.getTable("Table1");
      index = t.getIndexes().get(0);

      System.out.println("IndexTest: Index type: " + 
                         index.getIndexData().getClass());
      try {
        t.addRow(99, "abc", "def");
        if(index.getIndexData() instanceof SimpleIndexData) {
          // SimpleIndex doesn't support writing these indexes
          fail("Should have thrown UnsupportedOperationException");
        }
      } catch(UnsupportedOperationException e) {
        // success
        if(index.getIndexData() instanceof BigIndexData) {
          throw e;
        }
      }
    }
  }

  public void testEntryDeletion() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST) {
      Table table = openCopy(testDB).getTable("Table1");

      for(int i = 0; i < 10; ++i) {
        table.addRow("foo" + i, "bar" + i, (byte)42 + i, (short)53 + i, 13 * i,
                     (6.7d / i), null, null, true);
      }
      table.reset();
      assertRowCount(12, table);

      for(Index index : table.getIndexes()) {
        assertEquals(12, index.getIndexData().getEntryCount());
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
        assertEquals(8, index.getIndexData().getEntryCount());
      }
    }
  }

  public void testIgnoreNulls() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX_PROPERTIES)) {
      Database db = openCopy(testDB);

      doTestIgnoreNulls(db, "TableIgnoreNulls1");
      doTestIgnoreNulls(db, "TableIgnoreNulls2");

      db.close();
    }
  }

  private void doTestIgnoreNulls(Database db, String tableName)
    throws Exception
  {
    Table orig = db.getTable(tableName);
    Index origI = orig.getIndex("DataIndex");
    Table temp = db.getTable(tableName + "_temp");
    Index tempI = temp.getIndex("DataIndex");

    // copy from orig table to temp table
    for(Map<String,Object> row : orig) {
      temp.addRow(orig.asRow(row));
    }

    assertEquals(origI.getIndexData().getEntryCount(), 
                 tempI.getIndexData().getEntryCount());

    Cursor origC = Cursor.createIndexCursor(orig, origI);
    Cursor tempC = Cursor.createIndexCursor(temp, tempI);

    while(true) {
      boolean origHasNext = origC.moveToNextRow();
      boolean tempHasNext = tempC.moveToNextRow();
      assertTrue(origHasNext == tempHasNext);
      if(!origHasNext) {
        break;
      }

      Map<String,Object> origRow = origC.getCurrentRow();
      Cursor.Position origCurPos = origC.getSavepoint().getCurrentPosition();
      Map<String,Object> tempRow = tempC.getCurrentRow();
      Cursor.Position tempCurPos = tempC.getSavepoint().getCurrentPosition();
      
      assertEquals(origRow, tempRow);
      assertEquals(IndexCodesTest.entryToString(origCurPos),
                   IndexCodesTest.entryToString(tempCurPos));
    }
  }

  public void testUnique() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX_PROPERTIES)) {
      Database db = openCopy(testDB);

      Table t = db.getTable("TableUnique1_temp");
      Index index = t.getIndex("DataIndex");

      doTestUnique(index, 1,
                   null, true,
                   "unique data", true,
                   null, true,
                   "more", false,
                   "stuff", false,
                   "unique data", false);

      t = db.getTable("TableUnique2_temp");
      index = t.getIndex("DataIndex");

      doTestUnique(index, 2,
                   null, null, true,
                   "unique data", 42, true,
                   "unique data", null, true,
                   null, null, true,
                   "some", 42, true,
                   "more unique data", 13, true,
                   null, -4242, true,
                   "another row", -3462, false,
                   null, 49, false,
                   "more", null, false,
                   "unique data", 42, false,
                   "unique data", null, false,
                   null, -4242, false);

      db.close();
    }
  }

  private void doTestUnique(Index index, int numValues,
                            Object... testData)
    throws Exception
  {
    for(int i = 0; i < testData.length; i += (numValues + 1)) {
      Object[] row = new Object[numValues + 1];
      row[0] = "testRow" + i;
      for(int j = 1; j < (numValues + 1); ++j) {
        row[j] = testData[i + j - 1];
      }
      boolean expectedSuccess = (Boolean)testData[i + numValues];

      IOException failure = null;
      try {
        index.addRow(row, new RowId(400 + i, 0));
      } catch(IOException e) {
        failure = e;
      }
      if(expectedSuccess) {
        assertNull(failure);
      } else {
        assertTrue(failure != null);
        assertTrue(failure.getMessage().contains("uniqueness"));
      }
    }
  }

  public void testUniqueEntryCount() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST) {
      Database db = openCopy(testDB);
      Table table = db.getTable("Table1");
      Index indA = table.getIndex("PrimaryKey");
      Index indB = table.getIndex("B");

      assertEquals(2, indA.getUniqueEntryCount());
      assertEquals(2, indB.getUniqueEntryCount());

      List<String> bElems = Arrays.asList("bar", null, "baz", "argle", null,
                                          "bazzle", "37", "bar", "bar", "BAZ");

      for(int i = 0; i < 10; ++i) {
        table.addRow("foo" + i, bElems.get(i), (byte)42 + i, (short)53 + i,
                     13 * i, (6.7d / i), null, null, true);
      }

      assertEquals(12, indA.getIndexData().getEntryCount());
      assertEquals(12, indB.getIndexData().getEntryCount());

      assertEquals(12, indA.getUniqueEntryCount());
      assertEquals(8, indB.getUniqueEntryCount());

      table = null;
      indA = null;
      indB = null;

      table = db.getTable("Table1");
      indA = table.getIndex("PrimaryKey");
      indB = table.getIndex("B");

      assertEquals(12, indA.getIndexData().getEntryCount());
      assertEquals(12, indB.getIndexData().getEntryCount());

      assertEquals(12, indA.getUniqueEntryCount());
      assertEquals(8, indB.getUniqueEntryCount());

      Cursor c = Cursor.createCursor(table);
      assertTrue(c.moveToNextRow());

      final Map<String,Object> row = c.getCurrentRow();
      // Row order is arbitrary, so v2007 row order difference is valid
      if (testDB.getExpectedFileFormat().ordinal() >= 
          Database.FileFormat.V2007.ordinal()) {
        DatabaseTest.checkTestDBTable1RowA(testDB, table, row);
      } else {
        DatabaseTest.checkTestDBTable1RowABCDEFG(testDB, table, row);
      }
      c.deleteCurrentRow();

      assertEquals(11, indA.getIndexData().getEntryCount());
      assertEquals(11, indB.getIndexData().getEntryCount());

      assertEquals(12, indA.getUniqueEntryCount());
      assertEquals(8, indB.getUniqueEntryCount());

      db.close();
    }
  }
  
  public void testReplId() throws Exception
  {
    for (final TestDB testDB : SUPPORTED_DBS_TEST) {
      Database db = openCopy(testDB);
      Table table = db.getTable("Table4");

      for(int i = 0; i< 20; ++i) {
        table.addRow("row" + i, Column.AUTO_NUMBER);
      }

      assertEquals(20, table.getRowCount());

      db.close();
    }
  }

  public void testIndexCreation() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table t = new TableBuilder("TestTable")
        .addColumn(new ColumnBuilder("id", DataType.LONG))
        .addColumn(new ColumnBuilder("data", DataType.TEXT))
        .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                  .addColumns("id").setPrimaryKey())
        .toTable(db);

      assertEquals(1, t.getIndexes().size());
      Index idx = t.getIndexes().get(0);
      
      assertEquals(IndexBuilder.PRIMARY_KEY_NAME, idx.getName());       
      assertEquals(1, idx.getColumns().size());
      assertEquals("id", idx.getColumns().get(0).getName());
      assertTrue(idx.getColumns().get(0).isAscending());
      assertTrue(idx.isPrimaryKey());
      assertTrue(idx.isUnique());
      assertFalse(idx.shouldIgnoreNulls());
      assertNull(idx.getReference());

      t.addRow(2, "row2");
      t.addRow(1, "row1");
      t.addRow(3, "row3");

      Cursor c = new CursorBuilder(t)
        .setIndexByName(IndexBuilder.PRIMARY_KEY_NAME).toCursor();

      for(int i = 1; i <= 3; ++i) {
        Map<String,Object> row = c.getNextRow();
        assertEquals(i, row.get("id"));
        assertEquals("row" + i, row.get("data"));
      }
      assertFalse(c.moveToNextRow());
    }    
  }
  
  public void testGetForeignKeyIndex() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX, true)) {
      Database db = open(testDB);
      Table t1 = db.getTable("Table1");
      Table t2 = db.getTable("Table2");
      Table t3 = db.getTable("Table3");

      Index t2t1 = t1.getIndex(IndexTest.getRelationshipName(
                                   db.getFormat(), "Table2Table1"));
      Index t3t1 = t1.getIndex(IndexTest.getRelationshipName(
                                   db.getFormat(), "Table3Table1"));


      assertTrue(t2t1.isForeignKey());
      assertNotNull(t2t1.getReference());
      assertFalse(t2t1.getReference().isPrimaryTable());
      doCheckForeignKeyIndex(t1, t2t1, t2);

      assertTrue(t3t1.isForeignKey());
      assertNotNull(t3t1.getReference());
      assertFalse(t3t1.getReference().isPrimaryTable());
      doCheckForeignKeyIndex(t1, t3t1, t3);
      
      Index t1pk = t1.getIndex(IndexBuilder.PRIMARY_KEY_NAME);
      assertNotNull(t1pk);
      assertNull(t1pk.getReference());
      assertNull(t1pk.getReferencedIndex());
    }    
  }

  private void doCheckForeignKeyIndex(Table ta, Index ia, Table tb)
    throws Exception
  {
    Index ib = ia.getReferencedIndex();
    assertNotNull(ib);
    assertSame(tb, ib.getTable());

    assertNotNull(ib.getReference());
    assertSame(ia, ib.getReferencedIndex());
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

  static String getRelationshipName(JetFormat format, String name)
  {
    if(format == JetFormat.VERSION_3) {
      if(name.equals("Table2Table1")) {
        return "{150B6687-5C64-4DC0-B934-A8CF92FF73FF}";
      }
      if(name.equals("Table3Table1")) {
        return "{D039E343-97A1-471F-B2E3-8DFCF1EEC597}";
      }
    }
    return name;
  }
}
