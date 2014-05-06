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
import com.healthmarketscience.jackcess.impl.JetFormatTest;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.util.CaseInsensitiveColumnMatcher;
import com.healthmarketscience.jackcess.util.ColumnMatcher;
import com.healthmarketscience.jackcess.util.RowFilterTest;
import com.healthmarketscience.jackcess.util.SimpleColumnMatcher;
import junit.framework.TestCase;

/**
 * @author James Ahlborn
 */
public class CursorTest extends TestCase {

  static final List<TestDB> INDEX_CURSOR_DBS = 
    TestDB.getSupportedForBasename(Basename.INDEX_CURSOR);


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

  private static List<Map<String,Object>> createDupeTestTableData()
    throws Exception
  {
    List<Map<String,Object>> expectedRows =
      new ArrayList<Map<String,Object>>();
    int[] ids = new int[]{3, 7, 6, 1, 2, 9, 0, 5, 4, 8};
    for(int i : ids) {
      expectedRows.add(createExpectedRow("id", i, "value", "data" + (i % 3)));
    }
    for(int i : ids) {
      expectedRows.add(createExpectedRow("id", i, "value", "data" + (i % 5)));
    }
    return expectedRows;
  }

  private static Database createDupeTestTable(final FileFormat fileFormat) 
    throws Exception 
  {
    Database db = create(fileFormat);

    Table table = new TableBuilder("test")
      .addColumn(new ColumnBuilder("id", DataType.LONG))
      .addColumn(new ColumnBuilder("value", DataType.TEXT))
      .toTable(db);

    for(Map<String,Object> row : createDupeTestTableData()) {
      table.addRow(row.get("id"), row.get("value"));
    }

    return db;
  }

  static Database createDupeTestTable(final TestDB indexCursorDB) 
    throws Exception 
  {
    Database db = openCopy(indexCursorDB);

    Table table = db.getTable("test");

    for(Map<String,Object> row : createDupeTestTableData()) {
      table.addRow(row.get("id"), row.get("value"));
    }

    return db;
  }

  private static Cursor createIndexSubRangeCursor(Table table,
                                                  Index idx,
                                                  int type)
    throws Exception
  {
    return table.newCursor()
      .setIndex(idx)
      .setStartEntry(3 - type)
      .setStartRowInclusive(type == 0)
      .setEndEntry(8 + type)
      .setEndRowInclusive(type == 0)
      .toCursor();
  }
  
  public void testRowId() throws Exception {
    // test special cases
    RowIdImpl rowId1 = new RowIdImpl(1, 2);
    RowIdImpl rowId2 = new RowIdImpl(1, 3);
    RowIdImpl rowId3 = new RowIdImpl(2, 1);

    List<RowIdImpl> sortedRowIds =
      new ArrayList<RowIdImpl>(new TreeSet<RowIdImpl>(
        Arrays.asList(rowId1, rowId2, rowId3, RowIdImpl.FIRST_ROW_ID,
                      RowIdImpl.LAST_ROW_ID)));

    assertEquals(Arrays.asList(RowIdImpl.FIRST_ROW_ID, rowId1, rowId2, rowId3,
                               RowIdImpl.LAST_ROW_ID),
                 sortedRowIds);
  }
  
  public void testSimple() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestSimple(cursor, null);
      db.close();
    }
  }

  private static void doTestSimple(Cursor cursor,
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
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestMove(cursor, null);

      db.close();
    }
  }

  private static void doTestMove(Cursor cursor,
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

  public void testMoveNoReset() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestMoveNoReset(cursor);

      db.close();
    }
  }

  private static void doTestMoveNoReset(Cursor cursor)
    throws Exception
  {
    List<Map<String, Object>> expectedRows = createTestTableData();
    List<Map<String, Object>> foundRows = new ArrayList<Map<String, Object>>();
    
    Iterator<Row> iter = cursor.newIterable().iterator();

    for(int i = 0; i < 6; ++i) {
      foundRows.add(iter.next());
    }    

    iter = cursor.newIterable().reset(false).reverse().iterator();
    iter.next();
    Map<String, Object> row = iter.next();
    assertEquals(expectedRows.get(4), row);
    
    iter = cursor.newIterable().reset(false).iterator();
    iter.next();
    row = iter.next();
    assertEquals(expectedRows.get(5), row);
    iter.next();

    iter = cursor.newIterable().reset(false).iterator();
    for(int i = 6; i < 10; ++i) {
      foundRows.add(iter.next());
    }    

    assertEquals(expectedRows, foundRows);
  }
  
  public void testSearch() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestSearch(table, cursor, null, 42, -13);

      db.close();
    }
  }

  private static void doTestSearch(Table table, Cursor cursor, Index index,
                                   Integer... outOfRangeValues)
    throws Exception
  {
    assertTrue(cursor.findFirstRow(table.getColumn("id"), 3));
    assertEquals(createExpectedRow("id", 3,
                                   "value", "data" + 3),
                 cursor.getCurrentRow());

    assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "id", 6,
                                    "value", "data" + 6)));
    assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    assertFalse(cursor.findFirstRow(createExpectedRow(
                                   "id", 8,
                                   "value", "data" + 13)));
    assertFalse(cursor.findFirstRow(table.getColumn("id"), 13));
    assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "value", "data" + 7)));
    assertEquals(createExpectedRow("id", 7,
                                   "value", "data" + 7),
                 cursor.getCurrentRow());
    
    assertTrue(cursor.findFirstRow(table.getColumn("value"), "data" + 4));
    assertEquals(createExpectedRow("id", 4,
                                   "value", "data" + 4),
                 cursor.getCurrentRow());

    for(Integer outOfRangeValue : outOfRangeValues) {
      assertFalse(cursor.findFirstRow(table.getColumn("id"),
                                 outOfRangeValue));
      assertFalse(cursor.findFirstRow(table.getColumn("value"),
                                 "data" + outOfRangeValue));
      assertFalse(cursor.findFirstRow(createExpectedRow(
                                     "id", outOfRangeValue,
                                     "value", "data" + outOfRangeValue)));
    }
    
    assertEquals("data" + 5,
                 CursorBuilder.findValue(table,
                                  table.getColumn("value"),
                                  table.getColumn("id"), 5));
    assertEquals(createExpectedRow("id", 5,
                                   "value", "data" + 5),
                 CursorBuilder.findRow(table,
                                createExpectedRow("id", 5)));
    if(index != null) {
      assertEquals("data" + 5,
                   CursorBuilder.findValue(index,
                                    table.getColumn("value"),
                                    table.getColumn("id"), 5));
      assertEquals(createExpectedRow("id", 5,
                                     "value", "data" + 5),
                   CursorBuilder.findRow(index,
                                  createExpectedRow("id", 5)));

      assertNull(CursorBuilder.findValue(index,
                                  table.getColumn("value"),
                                  table.getColumn("id"),
                                  -17));
      assertNull(CursorBuilder.findRow(index,
                                createExpectedRow("id", 13)));
    }
  }

  public void testReverse() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestReverse(cursor, null);

      db.close();
    }
  }

  private static void doTestReverse(Cursor cursor,
                                    List<Map<String, Object>> expectedRows)
    throws Exception
  {
    if(expectedRows == null) {
      expectedRows = createTestTableData();
    }
    Collections.reverse(expectedRows);

    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    for(Map<String, Object> row : cursor.newIterable().reverse()) {
      foundRows.add(row);
    }
    assertEquals(expectedRows, foundRows);    
  }
  
  public void testLiveAddition() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");

      Cursor cursor1 = CursorBuilder.createCursor(table);
      Cursor cursor2 = CursorBuilder.createCursor(table);
      doTestLiveAddition(table, cursor1, cursor2, 11);

      db.close();
    }
  }

  private static void doTestLiveAddition(Table table,
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

      Cursor cursor1 = CursorBuilder.createCursor(table);
      Cursor cursor2 = CursorBuilder.createCursor(table);
      Cursor cursor3 = CursorBuilder.createCursor(table);
      Cursor cursor4 = CursorBuilder.createCursor(table);
      doTestLiveDeletion(cursor1, cursor2, cursor3, cursor4, 1);

      db.close();
    }
  }

  private static void doTestLiveDeletion(
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

      Cursor cursor = CursorBuilder.createCursor(idx);
      doTestSimple(cursor, null);

      db.close();
    }
  }

  public void testMoveIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = CursorBuilder.createCursor(idx);
      doTestMove(cursor, null);

      db.close();
    }
  }
  
  public void testReverseIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = CursorBuilder.createCursor(idx);
      doTestReverse(cursor, null);

      db.close();
    }
  }

  public void testSearchIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = CursorBuilder.createCursor(idx);
      doTestSearch(table, cursor, idx, 42, -13);

      db.close();
    }
  }

  public void testLiveAdditionIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor cursor1 = CursorBuilder.createCursor(idx);
      Cursor cursor2 = CursorBuilder.createCursor(idx);
      doTestLiveAddition(table, cursor1, cursor2, 11);

      db.close();
    }
  }

  public void testLiveDeletionIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor cursor1 = CursorBuilder.createCursor(idx);
      Cursor cursor2 = CursorBuilder.createCursor(idx);
      Cursor cursor3 = CursorBuilder.createCursor(idx);
      Cursor cursor4 = CursorBuilder.createCursor(idx);
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

  public void testFindAllIndex() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createDupeTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);

      doTestFindAll(table, cursor, null);

      db.close();
    }
  }

  public void testFindAll() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createDupeTestTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = CursorBuilder.createCursor(idx);

      doTestFindAll(table, cursor, idx);

      db.close();
    }
  }

  private static void doTestFindAll(Table table, Cursor cursor, Index index)
    throws Exception
  {
    List<? extends Map<String,Object>> rows = RowFilterTest.toList(
        cursor.newIterable().setMatchPattern("value", "data2"));

    List<? extends Map<String, Object>> expectedRows = null;

    if(index == null) {
      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 2, "value", "data2"),
            createExpectedRow(
                "id", 5, "value", "data2"),
            createExpectedRow(
                "id", 8, "value", "data2"),
            createExpectedRow(
                "id", 7, "value", "data2"),
            createExpectedRow(
                "id", 2, "value", "data2"));
    } else {
      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 2, "value", "data2"),
            createExpectedRow(
                "id", 2, "value", "data2"),
            createExpectedRow(
                "id", 5, "value", "data2"),
            createExpectedRow(
                "id", 7, "value", "data2"),
            createExpectedRow(
                "id", 8, "value", "data2"));
    }
    assertEquals(expectedRows, rows);

    Column valCol = table.getColumn("value");
    rows = RowFilterTest.toList(
        cursor.newIterable().setMatchPattern(valCol, "data4"));

    if(index == null) {
      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 9, "value", "data4"),
            createExpectedRow(
                "id", 4, "value", "data4"));
    } else {
      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 4, "value", "data4"),
            createExpectedRow(
                "id", 9, "value", "data4"));
    }
    assertEquals(expectedRows, rows);

    rows = RowFilterTest.toList(
        cursor.newIterable().setMatchPattern(valCol, "data9"));

    assertTrue(rows.isEmpty());

    rows = RowFilterTest.toList(
        cursor.newIterable().setMatchPattern(
            Collections.singletonMap("id", 8)));
    
    expectedRows =
      createExpectedTable(
          createExpectedRow(
              "id", 8, "value", "data2"),
          createExpectedRow(
              "id", 8, "value", "data3"));
    assertEquals(expectedRows, rows);

    for(Map<String,Object> row : table) {
      
      List<Map<String,Object>> tmpRows = new ArrayList<Map<String,Object>>();
      for(Map<String,Object> tmpRow : cursor) {
        if(row.equals(tmpRow)) {
          tmpRows.add(tmpRow);
        }
      }
      expectedRows = tmpRows;
      assertFalse(expectedRows.isEmpty());
      
      rows = RowFilterTest.toList(cursor.newIterable().setMatchPattern(row));

      assertEquals(expectedRows, rows);
    }

    rows = RowFilterTest.toList(
        cursor.newIterable().addMatchPattern("id", 8)
        .addMatchPattern("value", "data13"));
    assertTrue(rows.isEmpty());
  }

  public void testId() throws Exception
  {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor tCursor = CursorBuilder.createCursor(table);
      Cursor iCursor = CursorBuilder.createCursor(idx);

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

      Cursor tCursor2 = CursorBuilder.createCursor(table);
      Cursor iCursor2 = CursorBuilder.createCursor(idx);

      tCursor2.restoreSavepoint(tSave);
      iCursor2.restoreSavepoint(iSave);

      db.close();
    }
  }
  
  public void testColumnMatcher() throws Exception {
    

    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");

      doTestMatchers(table, SimpleColumnMatcher.INSTANCE, false);
      doTestMatchers(table, CaseInsensitiveColumnMatcher.INSTANCE, true);

      Cursor cursor = CursorBuilder.createCursor(table);
      doTestMatcher(table, cursor, SimpleColumnMatcher.INSTANCE, false);
      doTestMatcher(table, cursor, CaseInsensitiveColumnMatcher.INSTANCE, 
                    true);
      db.close();
    }
  }

  private static void doTestMatchers(Table table, ColumnMatcher columnMatcher,
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
  
  private static void doTestMatcher(Table table, Cursor cursor, 
                                    ColumnMatcher columnMatcher,
                                    boolean caseInsensitive)
    throws Exception
  {
    cursor.setColumnMatcher(columnMatcher);

    assertTrue(cursor.findFirstRow(table.getColumn("id"), 3));
    assertEquals(createExpectedRow("id", 3,
                                   "value", "data" + 3),
                 cursor.getCurrentRow());

    assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "id", 6,
                                    "value", "data" + 6)));
    assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "id", 6,
                                    "value", "Data" + 6)) == caseInsensitive);
    if(caseInsensitive) {
      assertEquals(createExpectedRow("id", 6,
                                     "value", "data" + 6),
                   cursor.getCurrentRow());
    }

    assertFalse(cursor.findFirstRow(createExpectedRow(
                                   "id", 8,
                                   "value", "data" + 13)));
    assertFalse(cursor.findFirstRow(table.getColumn("id"), 13));
    assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "value", "data" + 7)));
    assertEquals(createExpectedRow("id", 7,
                                   "value", "data" + 7),
                 cursor.getCurrentRow());
    
    assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "value", "Data" + 7)) == caseInsensitive);
    if(caseInsensitive) {
      assertEquals(createExpectedRow("id", 7,
                                     "value", "data" + 7),
                   cursor.getCurrentRow());
    }
    
    assertTrue(cursor.findFirstRow(table.getColumn("value"), "data" + 4));
    assertEquals(createExpectedRow("id", 4,
                                   "value", "data" + 4),
                 cursor.getCurrentRow());

    assertTrue(cursor.findFirstRow(table.getColumn("value"), "Data" + 4) 
               == caseInsensitive);
    if(caseInsensitive) {
      assertEquals(createExpectedRow("id", 4,
                                     "value", "data" + 4),
                   cursor.getCurrentRow());
    }

    assertEquals(Arrays.asList(createExpectedRow("id", 4,
                                                 "value", "data" + 4)),
                 RowFilterTest.toList(
                     cursor.newIterable()
                     .setMatchPattern("value", "data4")
                     .setColumnMatcher(SimpleColumnMatcher.INSTANCE)));

    assertEquals(Arrays.asList(createExpectedRow("id", 3,
                                                 "value", "data" + 3)),
                 RowFilterTest.toList(
                     cursor.newIterable()
                     .setMatchPattern("value", "DaTa3")
                     .setColumnMatcher(CaseInsensitiveColumnMatcher.INSTANCE)));

    assertEquals(Arrays.asList(createExpectedRow("id", 2,
                                                 "value", "data" + 2)),
                 RowFilterTest.toList(
                     cursor.newIterable()
                     .addMatchPattern("value", "DaTa2")
                     .addMatchPattern("id", 2)
                     .setColumnMatcher(CaseInsensitiveColumnMatcher.INSTANCE)));
  }

  public void testIndexCursor() throws Exception
  { 
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX)) {

      Database db = open(testDB);
      Table t1 = db.getTable("Table1");
      Index idx = t1.getIndex(IndexBuilder.PRIMARY_KEY_NAME);
      IndexCursor cursor = CursorBuilder.createCursor(idx);

      assertFalse(cursor.findFirstRowByEntry(-1));
      cursor.findClosestRowByEntry(-1);
      assertEquals(0, cursor.getCurrentRow().get("id"));

      assertTrue(cursor.findFirstRowByEntry(1));
      assertEquals(1, cursor.getCurrentRow().get("id"));
      
      cursor.findClosestRowByEntry(2);
      assertEquals(2, cursor.getCurrentRow().get("id"));

      assertFalse(cursor.findFirstRowByEntry(4));
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
      IndexCursor cursor = CursorBuilder.createCursor(idx);

      List<String> expectedData = new ArrayList<String>();
      for(Row row : cursor.newEntryIterable(1)
            .addColumnNames("data")) {
        expectedData.add(row.getString("data"));
      }

      assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);

      expectedData = new ArrayList<String>();
      for(Iterator<? extends Row> iter = 
            cursor.newEntryIterable(1).iterator();
          iter.hasNext(); ) {
        expectedData.add(iter.next().getString("data"));
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
      for(Row row : cursor.newEntryIterable(1)
            .addColumnNames("data")) {
        expectedData.add(row.getString("data"));
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
      Cursor cursor = CursorBuilder.createCursor(t1);

      List<String> expectedData = new ArrayList<String>();
      for(Row row : cursor.newIterable().setColumnNames(
              Arrays.asList("otherfk1", "data"))) {
        if(row.get("otherfk1").equals(1)) {
          expectedData.add(row.getString("data"));
        }
      }

      assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);

      expectedData = new ArrayList<String>();
      for(Iterator<? extends Row> iter = cursor.iterator();
          iter.hasNext(); ) {
        Row row = iter.next();
        if(row.get("otherfk1").equals(1)) {
          expectedData.add(row.getString("data"));
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
      for(Row row : cursor.newIterable().setColumnNames(
              Arrays.asList("otherfk1", "data"))) {
        if(row.get("otherfk1").equals(1)) {
          expectedData.add(row.getString("data"));
        }
      }

      assertTrue(expectedData.isEmpty());
      
      db.close();
    }    
  }
  
}
  
