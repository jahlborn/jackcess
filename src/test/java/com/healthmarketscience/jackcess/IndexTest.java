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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.impl.ByteUtil;
import com.healthmarketscience.jackcess.impl.IndexCodesTest;
import com.healthmarketscience.jackcess.impl.IndexData;
import com.healthmarketscience.jackcess.impl.IndexImpl;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.TestUtil.*;

/**
 * @author James Ahlborn
 */
public class IndexTest extends TestCase {

  public IndexTest(String name) {
    super(name);
  }

  @Override
  protected void setUp() {
    TestUtil.setTestAutoSync(false);
  }

  @Override
  protected void tearDown() {
    TestUtil.clearTestAutoSync();
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
      Index pkIndex = null;
      for(Index index : table.getIndexes()) {
        foundPKs.put(index.getColumns().iterator().next().getName(),
                     index.isPrimaryKey());
        if(index.isPrimaryKey()) {
          pkIndex= index;
        
        }
      }
      Map<String, Boolean> expectedPKs = new HashMap<String, Boolean>();
      expectedPKs.put("A", Boolean.TRUE);
      expectedPKs.put("B", Boolean.FALSE);
      assertEquals(expectedPKs, foundPKs);
      assertSame(pkIndex, table.getPrimaryKeyIndex());
    }
  }
  
  public void testLogicalIndexes() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX, true)) {
      Database mdb = open(testDB);

      TableImpl table = (TableImpl)mdb.getTable("Table1");
      for(IndexImpl idx : table.getIndexes()) {
        idx.initialize();
      }
      assertEquals(4, table.getIndexes().size());
      assertEquals(4, table.getLogicalIndexCount());
      checkIndexColumns(table,
                        "id", "id",
                        "PrimaryKey", "id",
                        "Table2Table1", "otherfk1",
                        "Table3Table1", "otherfk2");

      table = (TableImpl)mdb.getTable("Table2");
      for(IndexImpl idx : table.getIndexes()) {
        idx.initialize();
      }
      assertEquals(3, table.getIndexes().size());
      assertEquals(2, table.getIndexDatas().size());
      assertEquals(3, table.getLogicalIndexCount());
      checkIndexColumns(table,
                        "id", "id",
                        "PrimaryKey", "id",
                        ".rC", "id");

      IndexImpl pkIdx = table.getIndex("PrimaryKey");
      IndexImpl fkIdx = table.getIndex(".rC");
      assertNotSame(pkIdx, fkIdx);
      assertTrue(fkIdx.isForeignKey());
      assertSame(pkIdx.getIndexData(), fkIdx.getIndexData());
      IndexData indexData = pkIdx.getIndexData();
      assertEquals(Arrays.asList(pkIdx, fkIdx), indexData.getIndexes());
      assertSame(pkIdx, indexData.getPrimaryIndex());

      table = (TableImpl)mdb.getTable("Table3");
      for(IndexImpl idx : table.getIndexes()) {
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
      TableImpl t = (TableImpl)db.getTable("Table1");
      IndexImpl index = t.getIndexes().get(0);
      assertFalse(index.isInitialized());
      assertEquals(512, countRows(t));
      assertEquals(512, index.getIndexData().getEntryCount());
      db.close();

      // copy to temp file and attempt to edit
      db = openCopy(testDB);
      t = (TableImpl)db.getTable("Table1");
      index = t.getIndexes().get(0);

      t.addRow(99, "abc", "def");
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
        assertEquals(12, ((IndexImpl)index).getIndexData().getEntryCount());
      }

      table.reset();
      table.getNextRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      table.getNextRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      table.getNextRow();
      table.getNextRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();

      table.reset();
      assertRowCount(8, table);

      for(Index index : table.getIndexes()) {
        assertEquals(8, ((IndexImpl)index).getIndexData().getEntryCount());
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
    IndexImpl origI = (IndexImpl)orig.getIndex("DataIndex");
    Table temp = db.getTable(tableName + "_temp");
    IndexImpl tempI = (IndexImpl)temp.getIndex("DataIndex");

    // copy from orig table to temp table
    for(Map<String,Object> row : orig) {
      temp.addRow(orig.asRow(row));
    }

    assertEquals(origI.getIndexData().getEntryCount(), 
                 tempI.getIndexData().getEntryCount());

    Cursor origC = origI.newCursor().toCursor();
    Cursor tempC = tempI.newCursor().toCursor();

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
        ((IndexImpl)index).getIndexData().prepareAddRow(
            row, new RowIdImpl(400 + i, 0), null).commit();
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
      IndexImpl indA = (IndexImpl)table.getIndex("PrimaryKey");
      IndexImpl indB = (IndexImpl)table.getIndex("B");

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
      indA = (IndexImpl)table.getIndex("PrimaryKey");
      indB = (IndexImpl)table.getIndex("B");

      assertEquals(12, indA.getIndexData().getEntryCount());
      assertEquals(12, indB.getIndexData().getEntryCount());

      assertEquals(12, indA.getUniqueEntryCount());
      assertEquals(8, indB.getUniqueEntryCount());

      Cursor c = CursorBuilder.createCursor(table);
      assertTrue(c.moveToNextRow());

      final Row row = c.getCurrentRow();
      // Row order is arbitrary, so v2007 row order difference is valid
      if (testDB.getExpectedFileFormat().ordinal() >= 
          Database.FileFormat.V2007.ordinal()) {
        TestUtil.checkTestDBTable1RowA(testDB, table, row);
      } else {
        TestUtil.checkTestDBTable1RowABCDEFG(testDB, table, row);
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
      IndexImpl idx = (IndexImpl)t.getIndexes().get(0);
      
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

      Cursor c = t.newCursor()
        .setIndexByName(IndexBuilder.PRIMARY_KEY_NAME).toCursor();

      for(int i = 1; i <= 3; ++i) {
        Map<String,Object> row = c.getNextRow();
        assertEquals(i, row.get("id"));
        assertEquals("row" + i, row.get("data"));
      }
      assertFalse(c.moveToNextRow());
    }    
  }
  
  public void testIndexCreationSharedData() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table t = new TableBuilder("TestTable")
        .addColumn(new ColumnBuilder("id", DataType.LONG))
        .addColumn(new ColumnBuilder("data", DataType.TEXT))
        .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                  .addColumns("id").setPrimaryKey())
        .addIndex(new IndexBuilder("Index1").addColumns("id"))
        .addIndex(new IndexBuilder("Index2").addColumns("id"))
        .addIndex(new IndexBuilder("Index3").addColumns(false, "id"))
        .toTable(db);

      assertEquals(4, t.getIndexes().size());
      IndexImpl idx = (IndexImpl)t.getIndexes().get(0);
      
      assertEquals(IndexBuilder.PRIMARY_KEY_NAME, idx.getName());       
      assertEquals(1, idx.getColumns().size());
      assertEquals("id", idx.getColumns().get(0).getName());
      assertTrue(idx.getColumns().get(0).isAscending());
      assertTrue(idx.isPrimaryKey());
      assertTrue(idx.isUnique());
      assertFalse(idx.shouldIgnoreNulls());
      assertNull(idx.getReference());

      IndexImpl idx1 = (IndexImpl)t.getIndexes().get(1);
      IndexImpl idx2 = (IndexImpl)t.getIndexes().get(2);
      IndexImpl idx3 = (IndexImpl)t.getIndexes().get(3);

      assertNotSame(idx.getIndexData(), idx1.getIndexData());
      assertSame(idx1.getIndexData(), idx2.getIndexData());
      assertNotSame(idx2.getIndexData(), idx3.getIndexData());

      t.addRow(2, "row2");
      t.addRow(1, "row1");
      t.addRow(3, "row3");

      Cursor c = t.newCursor()
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

      IndexImpl t2t1 = (IndexImpl)t1.getIndex("Table2Table1");
      IndexImpl t3t1 = (IndexImpl)t1.getIndex("Table3Table1");


      assertTrue(t2t1.isForeignKey());
      assertNotNull(t2t1.getReference());
      assertFalse(t2t1.getReference().isPrimaryTable());
      assertFalse(t2t1.getReference().isCascadeUpdates());
      assertTrue(t2t1.getReference().isCascadeDeletes());
      doCheckForeignKeyIndex(t1, t2t1, t2);

      assertTrue(t3t1.isForeignKey());
      assertNotNull(t3t1.getReference());
      assertFalse(t3t1.getReference().isPrimaryTable());
      assertTrue(t3t1.getReference().isCascadeUpdates());
      assertFalse(t3t1.getReference().isCascadeDeletes());
      doCheckForeignKeyIndex(t1, t3t1, t3);
      
      Index t1pk = t1.getIndex(IndexBuilder.PRIMARY_KEY_NAME);
      assertNotNull(t1pk);
      assertNull(((IndexImpl)t1pk).getReference());
      assertNull(t1pk.getReferencedIndex());
    }    
  }

  public void testConstraintViolation() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table t = new TableBuilder("TestTable")
        .addColumn(new ColumnBuilder("id", DataType.LONG))
        .addColumn(new ColumnBuilder("data", DataType.TEXT))
        .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                  .addColumns("id").setPrimaryKey())
        .addIndex(new IndexBuilder("data_ind")
                  .addColumns("data").setUnique())
        .toTable(db);

      for(int i = 0; i < 5; ++i) {
        t.addRow(i, "row" + i);
      }

      try {
        t.addRow(3, "badrow");
        fail("ConstraintViolationException should have been thrown");
      } catch(ConstraintViolationException ce) {
        // success
      }

      assertEquals(5, t.getRowCount());

      List<Row> expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 0, "data", "row0"),
            createExpectedRow(
                "id", 1, "data", "row1"),
            createExpectedRow(
                "id", 2, "data", "row2"),
            createExpectedRow(
                "id", 3, "data", "row3"),
            createExpectedRow(
                "id", 4, "data", "row4"));

      assertTable(expectedRows, t);

      IndexCursor pkCursor = CursorBuilder.createPrimaryKeyCursor(t);
      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, 
                   CursorBuilder.createCursor(t.getIndex("data_ind")));

      List<Object[]> batch = new ArrayList<Object[]>();
      batch.add(new Object[]{5, "row5"});
      batch.add(new Object[]{6, "row6"});
      batch.add(new Object[]{7, "row2"});
      batch.add(new Object[]{8, "row8"});

      try {
        t.addRows(batch);
        fail("BatchUpdateException should have been thrown");
      } catch(BatchUpdateException be) {
        // success
        assertTrue(be.getCause() instanceof ConstraintViolationException);
        assertEquals(2, be.getUpdateCount());
      }

      expectedRows = new ArrayList<Row>(expectedRows);
      expectedRows.add(createExpectedRow("id", 5, "data", "row5"));
      expectedRows.add(createExpectedRow("id", 6, "data", "row6"));

      assertTable(expectedRows, t);
      
      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, 
                   CursorBuilder.createCursor(t.getIndex("data_ind")));

      pkCursor.findFirstRowByEntry(4);
      Row row4 = pkCursor.getCurrentRow();

      row4.put("id", 3);

      try {
        t.updateRow(row4);
        fail("ConstraintViolationException should have been thrown");
      } catch(ConstraintViolationException ce) {
        // success
      }
      
      assertTable(expectedRows, t);
      
      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, 
                   CursorBuilder.createCursor(t.getIndex("data_ind")));

      db.close();
    }    
  }

  public void testAutoNumberRecover() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table t = new TableBuilder("TestTable")
        .addColumn(new ColumnBuilder("id", DataType.LONG).setAutoNumber(true))
        .addColumn(new ColumnBuilder("data", DataType.TEXT))
        .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                  .addColumns("id").setPrimaryKey())
        .addIndex(new IndexBuilder("data_ind")
                  .addColumns("data").setUnique())
        .toTable(db);

      for(int i = 1; i < 3; ++i) {
        t.addRow(null, "row" + i);
      }

      try {
        t.addRow(null, "row1");
        fail("ConstraintViolationException should have been thrown");
      } catch(ConstraintViolationException ce) {
        // success
      }
      
      t.addRow(null, "row3");

      assertEquals(3, t.getRowCount());

      List<Row> expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 1, "data", "row1"),
            createExpectedRow(
                "id", 2, "data", "row2"),
            createExpectedRow(
                "id", 3, "data", "row3"));

      assertTable(expectedRows, t);

      IndexCursor pkCursor = CursorBuilder.createPrimaryKeyCursor(t);
      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, 
                   CursorBuilder.createCursor(t.getIndex("data_ind")));

      List<Object[]> batch = new ArrayList<Object[]>();
      batch.add(new Object[]{null, "row4"});
      batch.add(new Object[]{null, "row5"});
      batch.add(new Object[]{null, "row3"});

      try {
        t.addRows(batch);
        fail("BatchUpdateException should have been thrown");
      } catch(BatchUpdateException be) {
        // success
        assertTrue(be.getCause() instanceof ConstraintViolationException);
        assertEquals(2, be.getUpdateCount());
      }

      expectedRows = new ArrayList<Row>(expectedRows);
      expectedRows.add(createExpectedRow("id", 4, "data", "row4"));
      expectedRows.add(createExpectedRow("id", 5, "data", "row5"));

      assertTable(expectedRows, t);
      
      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, 
                   CursorBuilder.createCursor(t.getIndex("data_ind")));

      db.close();
    }
  }
  
  public void testBinaryIndex() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.BINARY_INDEX)) {
      Database db = open(testDB);

      Table table = db.getTable("Test");

      Index idx = table.getIndex("BinAscIdx");
      doTestBinaryIndex(idx, "BinAsc", false);

      idx = table.getIndex("BinDscIdx");
      doTestBinaryIndex(idx, "BinDsc", true);

      db.close();
    }
  }

  private static void doTestBinaryIndex(Index idx, String colName, boolean forward)
    throws Exception
  {
    IndexCursor ic = CursorBuilder.createCursor(idx);

    for(Row row : idx.getTable().getDefaultCursor().newIterable().setForward(forward)) {
      int id = row.getInt("ID");
      byte[] data = row.getBytes(colName);

      boolean found = false;
      for(Row idxRow : ic.newEntryIterable(data)) {
          
        assertTrue(Arrays.equals(data, idxRow.getBytes(colName)));
        if(id == idxRow.getInt("ID")) {
          found = true;
        }
      }

      assertTrue(found);
    }
  }

  private void doCheckForeignKeyIndex(Table ta, Index ia, Table tb)
    throws Exception
  {
    IndexImpl ib = (IndexImpl)ia.getReferencedIndex();
    assertNotNull(ib);
    assertSame(tb, ib.getTable());

    assertNotNull(ib.getReference());
    assertSame(ia, ib.getReferencedIndex());
    assertTrue(ib.getReference().isPrimaryTable());
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
