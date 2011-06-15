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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import static com.healthmarketscience.jackcess.Database.*;
import static com.healthmarketscience.jackcess.DatabaseTest.*;
import static com.healthmarketscience.jackcess.JetFormatTest.*;
import junit.framework.TestCase;

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

  private static List<Map<String,Object>> createTestTableData(
      int startIdx,
      int endIdx)
    throws Exception
  {
    List<Map<String,Object>> expectedRows = createTestTableData();
    expectedRows.subList(endIdx, expectedRows.size()).clear();
    expectedRows.subList(0, startIdx).clear();
    return expectedRows;
  }
  
  private static Database createTestTable(final FileFormat fileFormat) 
    throws Exception 
  {
    Database db = create(fileFormat);

    Table table = new TableBuilder("test")
      .addColumn(new ColumnBuilder("id", DataType.LONG))
      .addColumn(new ColumnBuilder("value", DataType.TEXT))
      .toTable(db);

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

  static final List<TestDB> INDEX_CURSOR_DBS = 
    TestDB.getSupportedForBasename(Basename.INDEX_CURSOR);

  static Database createTestIndexTable(final TestDB indexCursorDB) 
    throws Exception 
  {
    Database db = openCopy(indexCursorDB);

    Table table = db.getTable("test");

    for(Map<String,Object> row : createUnorderedTestTableData()) {
      table.addRow(row.get("id"), row.get("value"));
    }

    return db;
  }

  private static Cursor createIndexSubRangeCursor(Table table,
                                                  Index idx,
                                                  int type)
    throws Exception
  {
    return new CursorBuilder(table)
      .setIndex(idx)
      .setStartEntry(3 - type)
      .setStartRowInclusive(type == 0)
      .setEndEntry(8 + type)
      .setEndRowInclusive(type == 0)
      .toCursor();
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
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = Cursor.createCursor(table);
      doTestSimple(cursor, null);
      db.close();
    }
  }

  private void doTestSimple(Cursor cursor,
                            List<Map<String, Object>> expectedRows)
    throws Exception
  {
    if(expectedRows == null) {
      expectedRows = createTestTableData();
    }

    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    for(Map<String, Object> row : cursor) {
      foundRows.add(row);
    }
    assertEquals(expectedRows, foundRows);
  }

  public void testMove() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = Cursor.createCursor(table);
      doTestMove(cursor, null);

      db.close();
    }
  }

  private void doTestMove(Cursor cursor,
                          List<Map<String, Object>> expectedRows)
    throws Exception
  {
    if(expectedRows == null) {
      expectedRows = createTestTableData();
    }
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
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = Cursor.createCursor(table);
      doTestSearch(table, cursor, null, 42, -13);

      db.close();
    }
  }

  private void doTestSearch(Table table, Cursor cursor, Index index,
                            Integer... outOfRangeValues)
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
    
    assertTrue(cursor.findRow(table.getColumn("value"), "data" + 4));
    assertEquals(createExpectedRow("id", 4,
                                   "value", "data" + 4),
                 cursor.getCurrentRow());

    for(Integer outOfRangeValue : outOfRangeValues) {
      assertFalse(cursor.findRow(table.getColumn("id"),
                                 outOfRangeValue));
      assertFalse(cursor.findRow(table.getColumn("value"),
                                 "data" + outOfRangeValue));
      assertFalse(cursor.findRow(createExpectedRow(
                                     "id", outOfRangeValue,
                                     "value", "data" + outOfRangeValue)));
    }
    
    assertEquals("data" + 5,
                 Cursor.findValue(table,
                                  table.getColumn("value"),
                                  table.getColumn("id"), 5));
    assertEquals(createExpectedRow("id", 5,
                                   "value", "data" + 5),
                 Cursor.findRow(table,
                                createExpectedRow("id", 5)));
    if(index != null) {
      assertEquals("data" + 5,
                   Cursor.findValue(table, index,
                                    table.getColumn("value"),
                                    table.getColumn("id"), 5));
      assertEquals(createExpectedRow("id", 5,
                                     "value", "data" + 5),
                   Cursor.findRow(table, index,
                                  createExpectedRow("id", 5)));

      assertNull(Cursor.findValue(table, index,
                                  table.getColumn("value"),
                                  table.getColumn("id"),
                                  -17));
      assertNull(Cursor.findRow(table, index,
                                createExpectedRow("id", 13)));
    }
  }

  public void testReverse() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = Cursor.createCursor(table);
      doTestReverse(cursor, null);

      db.close();
    }
  }

  private void doTestReverse(Cursor cursor,
                             List<Map<String, Object>> expectedRows)
    throws Exception
  {
    if(expectedRows == null) {
      expectedRows = createTestTableData();
    }
    Collections.reverse(expectedRows);

    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    for(Map<String, Object> row : cursor.reverseIterable()) {
      foundRows.add(row);
    }
    assertEquals(expectedRows, foundRows);    
  }
  
  public void testLiveAddition() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");

      Cursor cursor1 = Cursor.createCursor(table);
      Cursor cursor2 = Cursor.createCursor(table);
      doTestLiveAddition(table, cursor1, cursor2, 11);

      db.close();
    }
  }

  private void doTestLiveAddition(Table table,
                                  Cursor cursor1,
                                  Cursor cursor2,
                                  Integer newRowNum) throws Exception
  {
    cursor1.moveNextRows(11);
    cursor2.moveNextRows(11);

    assertTrue(cursor1.isAfterLast());
    assertTrue(cursor2.isAfterLast());

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
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");

      Cursor cursor1 = Cursor.createCursor(table);
      Cursor cursor2 = Cursor.createCursor(table);
      Cursor cursor3 = Cursor.createCursor(table);
      Cursor cursor4 = Cursor.createCursor(table);
      doTestLiveDeletion(cursor1, cursor2, cursor3, cursor4, 1);

      db.close();
    }
  }

  private void doTestLiveDeletion(
          Cursor cursor1,
          Cursor cursor2,
          Cursor cursor3,
          Cursor cursor4,
          int firstValue) throws Exception
  {
    assertEquals(2, cursor1.moveNextRows(2));
    assertEquals(3, cursor2.moveNextRows(3));
    assertEquals(3, cursor3.moveNextRows(3));
    assertEquals(4, cursor4.moveNextRows(4));

    Map<String,Object> expectedPrevRow =
      createExpectedRow("id", firstValue, "value", "data" + firstValue);
    ++firstValue;
    Map<String,Object> expectedDeletedRow =
      createExpectedRow("id", firstValue, "value", "data" + firstValue);
    ++firstValue;
    Map<String,Object> expectedNextRow =
      createExpectedRow("id", firstValue, "value", "data" + firstValue);

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

    assertTrue(cursor3.moveToNextRow());
    cursor3.deleteCurrentRow();
    assertTrue(cursor3.isCurrentRowDeleted());

    firstValue += 2;
    expectedNextRow =
      createExpectedRow("id", firstValue, "value", "data" + firstValue);
    assertTrue(cursor3.moveToNextRow());
    assertEquals(expectedNextRow, cursor3.getNextRow());

    cursor1.beforeFirst();
    assertTrue(cursor1.moveToNextRow());
    cursor1.deleteCurrentRow();
    assertFalse(cursor1.isBeforeFirst());
    assertFalse(cursor1.isAfterLast());
    assertFalse(cursor1.moveToPreviousRow());
    assertTrue(cursor1.isBeforeFirst());
    assertFalse(cursor1.isAfterLast());

    cursor1.afterLast();
    assertTrue(cursor1.moveToPreviousRow());
    cursor1.deleteCurrentRow();
    assertFalse(cursor1.isBeforeFirst());
    assertFalse(cursor1.isAfterLast());
    assertFalse(cursor1.moveToNextRow());
    assertFalse(cursor1.isBeforeFirst());
    assertTrue(cursor1.isAfterLast());

    cursor1.beforeFirst();
    while(cursor1.moveToNextRow()) {
      cursor1.deleteCurrentRow();
    }

    assertTrue(cursor1.isAfterLast());
    assertTrue(cursor2.isCurrentRowDeleted());
    assertTrue(cursor3.isCurrentRowDeleted());
    assertTrue(cursor4.isCurrentRowDeleted());
  }

  public void testSimpleIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      assertTable(createUnorderedTestTableData(), table);

      Cursor cursor = Cursor.createIndexCursor(table, idx);
      doTestSimple(cursor, null);

      db.close();
    }
  }

  public void testMoveIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = Cursor.createIndexCursor(table, idx);
      doTestMove(cursor, null);

      db.close();
    }
  }
  
  public void testReverseIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = Cursor.createIndexCursor(table, idx);
      doTestReverse(cursor, null);

      db.close();
    }
  }

  public void testSearchIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = Cursor.createIndexCursor(table, idx);
      doTestSearch(table, cursor, idx, 42, -13);

      db.close();
    }
  }

  public void testLiveAdditionIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor cursor1 = Cursor.createIndexCursor(table, idx);
      Cursor cursor2 = Cursor.createIndexCursor(table, idx);
      doTestLiveAddition(table, cursor1, cursor2, 11);

      db.close();
    }
  }

  public void testLiveDeletionIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor cursor1 = Cursor.createIndexCursor(table, idx);
      Cursor cursor2 = Cursor.createIndexCursor(table, idx);
      Cursor cursor3 = Cursor.createIndexCursor(table, idx);
      Cursor cursor4 = Cursor.createIndexCursor(table, idx);
      doTestLiveDeletion(cursor1, cursor2, cursor3, cursor4, 1);

      db.close();
    }
  }

  public void testSimpleIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor = createIndexSubRangeCursor(table, idx, i);

        List<Map<String,Object>> expectedRows =
          createTestTableData(3, 9);

        doTestSimple(cursor, expectedRows);

        db.close();
      }
    }
  }
  
  public void testMoveIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor = createIndexSubRangeCursor(table, idx, i);

        List<Map<String,Object>> expectedRows =
          createTestTableData(3, 9);

        doTestMove(cursor, expectedRows);

        db.close();
      }
    }
  }
  
  public void testSearchIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor = createIndexSubRangeCursor(table, idx, i);

        doTestSearch(table, cursor, idx, 2, 9);

        db.close();
      }
    }
  }

  public void testReverseIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor = createIndexSubRangeCursor(table, idx, i);

        List<Map<String,Object>> expectedRows =
          createTestTableData(3, 9);

        doTestReverse(cursor, expectedRows);

        db.close();
      }
    }
  }

  public void testLiveAdditionIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor1 = createIndexSubRangeCursor(table, idx, i);
        Cursor cursor2 = createIndexSubRangeCursor(table, idx, i);

        doTestLiveAddition(table, cursor1, cursor2, 8);

        db.close();
      }
    }
  }
  
  public void testLiveDeletionIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor1 = createIndexSubRangeCursor(table, idx, i);
        Cursor cursor2 = createIndexSubRangeCursor(table, idx, i);
        Cursor cursor3 = createIndexSubRangeCursor(table, idx, i);
        Cursor cursor4 = createIndexSubRangeCursor(table, idx, i);

        doTestLiveDeletion(cursor1, cursor2, cursor3, cursor4, 4);

        db.close();
      }
    }
  }

  public void testId() throws Exception
  {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor tCursor = Cursor.createCursor(table);
      Cursor iCursor = Cursor.createIndexCursor(table, idx);

      Cursor.Savepoint tSave = tCursor.getSavepoint();
      Cursor.Savepoint iSave = iCursor.getSavepoint();

      tCursor.restoreSavepoint(tSave);
      iCursor.restoreSavepoint(iSave);

      try {
        tCursor.restoreSavepoint(iSave);
        fail("IllegalArgumentException should have been thrown");
      } catch(IllegalArgumentException e) {
        // success
      }

      try {
        iCursor.restoreSavepoint(tSave);
        fail("IllegalArgumentException should have been thrown");
      } catch(IllegalArgumentException e) {
        // success
      }

      Cursor tCursor2 = Cursor.createCursor(table);
      Cursor iCursor2 = Cursor.createIndexCursor(table, idx);

      tCursor2.restoreSavepoint(tSave);
      iCursor2.restoreSavepoint(iSave);

      db.close();
    }
  }
  
  public void testColmnMatcher() throws Exception {
    

    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");

      doTestMatchers(table, SimpleColumnMatcher.INSTANCE, false);
      doTestMatchers(table, CaseInsensitiveColumnMatcher.INSTANCE, true);

      Cursor cursor = Cursor.createCursor(table);
      doTestMatcher(table, cursor, SimpleColumnMatcher.INSTANCE, false);
      doTestMatcher(table, cursor, CaseInsensitiveColumnMatcher.INSTANCE, 
                    true);
      db.close();
    }
  }

  private void doTestMatchers(Table table, ColumnMatcher columnMatcher,
                              boolean caseInsensitive)
    throws Exception
  {
      assertTrue(columnMatcher.matches(table, "value", null, null));
      assertFalse(columnMatcher.matches(table, "value", "foo", null));
      assertFalse(columnMatcher.matches(table, "value", null, "foo"));
      assertTrue(columnMatcher.matches(table, "value", "foo", "foo"));
      assertTrue(columnMatcher.matches(table, "value", "foo", "Foo")
                 == caseInsensitive);

      assertFalse(columnMatcher.matches(table, "value", 13, null));
      assertFalse(columnMatcher.matches(table, "value", null, 13));
      assertTrue(columnMatcher.matches(table, "value", 13, 13));
  }
  
  private void doTestMatcher(Table table, Cursor cursor, 
                             ColumnMatcher columnMatcher,
                             boolean caseInsensitive)
    throws Exception
  {
    cursor.setColumnMatcher(columnMatcher);

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

    assertTrue(cursor.findRow(createExpectedRow(
                                    "id", 6,
                                    "value", "Data" + 6)) == caseInsensitive);
    if(caseInsensitive) {
      assertEquals(createExpectedRow("id", 6,
                                     "value", "data" + 6),
                   cursor.getCurrentRow());
    }

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
    
    assertTrue(cursor.findRow(createExpectedRow(
                                    "value", "Data" + 7)) == caseInsensitive);
    if(caseInsensitive) {
      assertEquals(createExpectedRow("id", 7,
                                     "value", "data" + 7),
                   cursor.getCurrentRow());
    }
    
    assertTrue(cursor.findRow(table.getColumn("value"), "data" + 4));
    assertEquals(createExpectedRow("id", 4,
                                   "value", "data" + 4),
                 cursor.getCurrentRow());

    assertTrue(cursor.findRow(table.getColumn("value"), "Data" + 4) 
               == caseInsensitive);
    if(caseInsensitive) {
      assertEquals(createExpectedRow("id", 4,
                                     "value", "data" + 4),
                   cursor.getCurrentRow());
    }
  }

  public void testIndexCursor() throws Exception
  { 
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX)) {

      Database db = open(testDB);
      Table t1 = db.getTable("Table1");
      Index idx = t1.getIndex(IndexBuilder.PRIMARY_KEY_NAME);
      IndexCursor cursor = IndexCursor.createCursor(t1, idx);

      assertFalse(cursor.findRowByEntry(-1));
      cursor.findClosestRowByEntry(-1);
      assertEquals(0, cursor.getCurrentRow().get("id"));

      assertTrue(cursor.findRowByEntry(1));
      assertEquals(1, cursor.getCurrentRow().get("id"));
      
      cursor.findClosestRowByEntry(2);
      assertEquals(2, cursor.getCurrentRow().get("id"));

      assertFalse(cursor.findRowByEntry(4));
      cursor.findClosestRowByEntry(4);
      assertTrue(cursor.isAfterLast());

      db.close();
    }    
  }
  
  public void testIndexCursorDelete() throws Exception
  { 
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX)) {

      Database db = openCopy(testDB);
      Table t1 = db.getTable("Table1");
      Index idx = t1.getIndex("Table2Table1");
      IndexCursor cursor = IndexCursor.createCursor(t1, idx);

      List<String> expectedData = new ArrayList<String>();
      for(Map<String,Object> row : cursor.entryIterable(
              Arrays.asList("data"), 1)) {
        expectedData.add((String)row.get("data"));
      }

      assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);

      expectedData = new ArrayList<String>();
      for(Iterator<Map<String,Object>> iter = cursor.entryIterator(1);
          iter.hasNext(); ) {
        expectedData.add((String)iter.next().get("data"));
        iter.remove();
        try {
          iter.remove();
          fail("IllegalArgumentException should have been thrown");
        } catch(IllegalStateException e) {
          // success
        }

        if(!iter.hasNext()) {
          try {
            iter.next();
            fail("NoSuchElementException should have been thrown");
          } catch(NoSuchElementException e) {
            // success
          }
        }
      }

      assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);
      
      expectedData = new ArrayList<String>();
      for(Map<String,Object> row : cursor.entryIterable(
              Arrays.asList("data"), 1)) {
        expectedData.add((String)row.get("data"));
      }

      assertTrue(expectedData.isEmpty());
      
      db.close();
    }    
  }
  
  public void testCursorDelete() throws Exception
  { 
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX)) {

      Database db = openCopy(testDB);
      Table t1 = db.getTable("Table1");
      Cursor cursor = Cursor.createCursor(t1);

      List<String> expectedData = new ArrayList<String>();
      for(Map<String,Object> row : cursor.iterable(
              Arrays.asList("otherfk1", "data"))) {
        if(row.get("otherfk1").equals(1)) {
          expectedData.add((String)row.get("data"));
        }
      }

      assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);

      expectedData = new ArrayList<String>();
      for(Iterator<Map<String,Object>> iter = cursor.iterator();
          iter.hasNext(); ) {
        Map<String,Object> row = iter.next();
        if(row.get("otherfk1").equals(1)) {
          expectedData.add((String)row.get("data"));
          iter.remove();
          try {
            iter.remove();
            fail("IllegalArgumentException should have been thrown");
          } catch(IllegalStateException e) {
            // success
          }
        }

        if(!iter.hasNext()) {
          try {
            iter.next();
            fail("NoSuchElementException should have been thrown");
          } catch(NoSuchElementException e) {
            // success
          }
        }
      }

      assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);
      
      expectedData = new ArrayList<String>();
      for(Map<String,Object> row : cursor.iterable(
              Arrays.asList("otherfk1", "data"))) {
        if(row.get("otherfk1").equals(1)) {
          expectedData.add((String)row.get("data"));
        }
      }

      assertTrue(expectedData.isEmpty());
      
      db.close();
    }    
  }
  
}
  
