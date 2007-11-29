// Copyright (c) 2007 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.DatabaseTest.*;

/**
 * @author James Ahlborn
 */
public class CursorTest extends TestCase {

  public CursorTest(String name) throws Exception {
    super(name);
  }

  private static List<Map<String,Object>> createTestTableData()
    throws Exception
  {
    List<Map<String,Object>> expectedRows =
      new ArrayList<Map<String,Object>>();
    for(int i = 0; i < 10; ++i) {
      expectedRows.add(createExpectedRow("id", i, "value", "data" + i));
    }
    return expectedRows;
  }
  
  private static Database createTestTable() throws Exception {
    Database db = create();

    List<Column> columns = new ArrayList<Column>();
    Column col = new Column();
    col.setName("id");
    col.setType(DataType.LONG);
    columns.add(col);
    col = new Column();
    col.setName("value");
    col.setType(DataType.TEXT);
    columns.add(col);
    db.createTable("test", columns);

    Table table = db.getTable("test");
    for(Map<String,Object> row : createTestTableData()) {
      table.addRow(row.get("id"), row.get("value"));
    }

    return db;
  }

  private static List<Map<String,Object>> createUnorderedTestTableData()
    throws Exception
  {
    List<Map<String,Object>> expectedRows =
      new ArrayList<Map<String,Object>>();
    int[] ids = new int[]{3, 7, 6, 1, 2, 9, 0, 5, 4, 8};
    for(int i : ids) {
      expectedRows.add(createExpectedRow("id", i, "value", "data" + i));
    }
    return expectedRows;
  }  
  
  private static Database createTestIndexTable() throws Exception {
    File srcFile = new File("test/data/indexCursorTest.mdb");
    File dbFile = File.createTempFile("databaseTest", ".mdb");
    dbFile.deleteOnExit();
    copyFile(srcFile, dbFile);
    
    Database db = Database.open(dbFile);

    Table table = db.getTable("test");

    for(Map<String,Object> row : createUnorderedTestTableData()) {
      table.addRow(row.get("id"), row.get("value"));
    }
    
    return db;
  }

  public void testRowId() throws Exception {
    // test special cases
    RowId rowId1 = new RowId(1, 2);
    RowId rowId2 = new RowId(1, 3);
    RowId rowId3 = new RowId(2, 1);

    List<RowId> sortedRowIds = new ArrayList<RowId>(new TreeSet<RowId>(
        Arrays.asList(rowId1, rowId2, rowId3, RowId.FIRST_ROW_ID,
                      RowId.LAST_ROW_ID)));

    assertEquals(Arrays.asList(RowId.FIRST_ROW_ID, rowId1, rowId2, rowId3,
                               RowId.LAST_ROW_ID),
                 sortedRowIds);
  }
  
  public void testSimple() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");
    Cursor cursor = Cursor.createCursor(table);
    doTestSimple(table, cursor);
    db.close();
  }

  private void doTestSimple(Table table, Cursor cursor) throws Exception {  
    List<Map<String,Object>> expectedRows = createTestTableData();

    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    for(Map<String, Object> row : cursor) {
      foundRows.add(row);
    }
    assertEquals(expectedRows, foundRows);
  }

  public void testMove() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");
    Cursor cursor = Cursor.createCursor(table);
    doTestMove(table, cursor);
    
    db.close();
  }

  private void doTestMove(Table table, Cursor cursor) throws Exception {
    List<Map<String,Object>> expectedRows = createTestTableData();
    expectedRows.subList(1, 4).clear();

    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    assertTrue(cursor.isBeforeFirst());
    assertFalse(cursor.isAfterLast());
    foundRows.add(cursor.getNextRow());
    assertEquals(3, cursor.moveNextRows(3));
    assertFalse(cursor.isBeforeFirst());
    assertFalse(cursor.isAfterLast());

    Map<String,Object> expectedRow = cursor.getCurrentRow();
    Cursor.Savepoint savepoint = cursor.getSavepoint();
    assertEquals(2, cursor.movePreviousRows(2));
    assertEquals(2, cursor.moveNextRows(2));
    assertTrue(cursor.moveToNextRow());
    assertTrue(cursor.moveToPreviousRow());
    assertEquals(expectedRow, cursor.getCurrentRow());
    
    while(cursor.moveToNextRow()) {
      foundRows.add(cursor.getCurrentRow());
    }
    assertEquals(expectedRows, foundRows);
    assertFalse(cursor.isBeforeFirst());
    assertTrue(cursor.isAfterLast());

    assertEquals(0, cursor.moveNextRows(3));

    cursor.beforeFirst();
    assertTrue(cursor.isBeforeFirst());
    assertFalse(cursor.isAfterLast());

    cursor.afterLast();
    assertFalse(cursor.isBeforeFirst());
    assertTrue(cursor.isAfterLast());

    cursor.restoreSavepoint(savepoint);
    assertEquals(expectedRow, cursor.getCurrentRow());    
  }

  public void testSearch() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");
    Cursor cursor = Cursor.createCursor(table);
    doTestSearch(table, cursor, null);
    
    db.close();
  }

  private void doTestSearch(Table table, Cursor cursor, Index index)
    throws Exception
  {
    assertTrue(cursor.findRow(table.getColumn("id"), 3));
    assertEquals(createExpectedRow("id", 3,
                                   "value", "data" + 3),
                 cursor.getCurrentRow());

    assertTrue(cursor.findRow(createExpectedRow(
                                    "id", 6,
                                    "value", "data" + 6)));
    assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    assertFalse(cursor.findRow(createExpectedRow(
                                   "id", 8,
                                   "value", "data" + 13)));
    assertFalse(cursor.findRow(table.getColumn("id"), 13));
    assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    assertTrue(cursor.findRow(createExpectedRow(
                                    "value", "data" + 7)));
    assertEquals(createExpectedRow("id", 7,
                                   "value", "data" + 7),
                 cursor.getCurrentRow());
    
    assertTrue(cursor.findRow(table.getColumn("value"), "data" + 2));
    assertEquals(createExpectedRow("id", 2,
                                   "value", "data" + 2),
                 cursor.getCurrentRow());
    
    assertEquals("data" + 9,
                 Cursor.findValue(table,
                                  table.getColumn("value"),
                                  table.getColumn("id"), 9));
    assertEquals(createExpectedRow("id", 9,
                                   "value", "data" + 9),
                 Cursor.findRow(table,
                                createExpectedRow("id", 9)));
    if(index != null) {
      assertEquals("data" + 9,
                   Cursor.findValue(table, index,
                                    table.getColumn("value"),
                                    table.getColumn("id"), 9));
      assertEquals(createExpectedRow("id", 9,
                                     "value", "data" + 9),
                   Cursor.findRow(table, index,
                                  createExpectedRow("id", 9)));
    }
  }

  public void testReverse() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");
    Cursor cursor = Cursor.createCursor(table);
    doTestReverse(table, cursor);

    db.close();
  }

  private void doTestReverse(Table table, Cursor cursor) throws Exception {
    List<Map<String,Object>> expectedRows = createTestTableData();
    Collections.reverse(expectedRows);

    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    for(Map<String, Object> row : cursor.reverseIterable()) {
      foundRows.add(row);
    }
    assertEquals(expectedRows, foundRows);    
  }
  
  public void testLiveAddition() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");

    Cursor cursor1 = Cursor.createCursor(table);
    Cursor cursor2 = Cursor.createCursor(table);
    doTestLiveAddition(table, cursor1, cursor2);
    
    db.close();
  }

  private void doTestLiveAddition(Table table,
                                  Cursor cursor1,
                                  Cursor cursor2) throws Exception
  {
    cursor1.moveNextRows(11);
    cursor2.moveNextRows(11);

    assertTrue(cursor1.isAfterLast());
    assertTrue(cursor2.isAfterLast());

    int newRowNum = 11;
    table.addRow(newRowNum, "data" + newRowNum);
    Map<String,Object> expectedRow = 
      createExpectedRow("id", newRowNum, "value", "data" + newRowNum);

    assertFalse(cursor1.isAfterLast());
    assertFalse(cursor2.isAfterLast());

    assertEquals(expectedRow, cursor1.getCurrentRow());
    assertEquals(expectedRow, cursor2.getCurrentRow());
    assertFalse(cursor1.moveToNextRow());
    assertFalse(cursor2.moveToNextRow());
    assertTrue(cursor1.isAfterLast());
    assertTrue(cursor2.isAfterLast());
  }

  
  public void testLiveDeletion() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");

    Cursor cursor1 = Cursor.createCursor(table);
    Cursor cursor2 = Cursor.createCursor(table);
    Cursor cursor3 = Cursor.createCursor(table);
    Cursor cursor4 = Cursor.createCursor(table);
    doTestLiveDeletion(table, cursor1, cursor2, cursor3, cursor4);
    
    db.close();
  }

  private void doTestLiveDeletion(Table table,
                                  Cursor cursor1,
                                  Cursor cursor2,
                                  Cursor cursor3,
                                  Cursor cursor4) throws Exception
  {
    cursor1.moveNextRows(2);
    cursor2.moveNextRows(3);
    cursor3.moveNextRows(3);
    cursor4.moveNextRows(4);

    Map<String,Object> expectedPrevRow =
      createExpectedRow("id", 1, "value", "data" + 1);
    Map<String,Object> expectedDeletedRow =
      createExpectedRow("id", 2, "value", "data" + 2);
    Map<String,Object> expectedNextRow =
      createExpectedRow("id", 3, "value", "data" + 3);
    
    assertEquals(expectedDeletedRow, cursor2.getCurrentRow());
    assertEquals(expectedDeletedRow, cursor3.getCurrentRow());
    
    assertFalse(cursor2.isCurrentRowDeleted());
    assertFalse(cursor3.isCurrentRowDeleted());

    cursor2.deleteCurrentRow();

    assertTrue(cursor2.isCurrentRowDeleted());
    assertTrue(cursor3.isCurrentRowDeleted());

    assertEquals(expectedNextRow, cursor1.getNextRow());
    assertEquals(expectedNextRow, cursor2.getNextRow());
    assertEquals(expectedNextRow, cursor3.getNextRow());
    
    assertEquals(expectedPrevRow, cursor3.getPreviousRow());
  }

  public void testSimpleIndex() throws Exception {
    Database db = createTestIndexTable();

    Table table = db.getTable("test");
    Index idx = table.getIndexes().get(0);

    assertTable(createUnorderedTestTableData(), table);

    Cursor cursor = Cursor.createIndexCursor(table, idx);
    doTestSimple(table, cursor);

    db.close();
  }

  public void testMoveIndex() throws Exception {
    Database db = createTestIndexTable();

    Table table = db.getTable("test");
    Index idx = table.getIndexes().get(0);
    Cursor cursor = Cursor.createIndexCursor(table, idx);
    doTestMove(table, cursor);
    
    db.close();
  }
  
  public void testReverseIndex() throws Exception {
    Database db = createTestIndexTable();

    Table table = db.getTable("test");
    Index idx = table.getIndexes().get(0);
    Cursor cursor = Cursor.createIndexCursor(table, idx);
    doTestReverse(table, cursor);

    db.close();
  }

  public void testSearchIndex() throws Exception {
    Database db = createTestIndexTable();

    Table table = db.getTable("test");
    Index idx = table.getIndexes().get(0);
    Cursor cursor = Cursor.createIndexCursor(table, idx);
    doTestSearch(table, cursor, idx);
    
    db.close();
  }

  public void testLiveAdditionIndex() throws Exception {
    Database db = createTestIndexTable();

    Table table = db.getTable("test");
    Index idx = table.getIndexes().get(0);

    Cursor cursor1 = Cursor.createIndexCursor(table, idx);
    Cursor cursor2 = Cursor.createIndexCursor(table, idx);
    doTestLiveAddition(table, cursor1, cursor2);
    
    db.close();
  }

  public void testLiveDeletionIndex() throws Exception {
    Database db = createTestIndexTable();

    Table table = db.getTable("test");
    Index idx = table.getIndexes().get(0);

    Cursor cursor1 = Cursor.createIndexCursor(table, idx);
    Cursor cursor2 = Cursor.createIndexCursor(table, idx);
    Cursor cursor3 = Cursor.createIndexCursor(table, idx);
    Cursor cursor4 = Cursor.createIndexCursor(table, idx);
    doTestLiveDeletion(table, cursor1, cursor2, cursor3, cursor4);
    
    db.close();
  }
  
}
