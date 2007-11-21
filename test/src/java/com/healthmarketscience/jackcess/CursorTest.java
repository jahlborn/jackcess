// Copyright (c) 2007 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    for(int i = 0; i < 10; ++i) {
      table.addRow(i, "data" + i);
    }

    return db;
  }
  
  public void testSimple() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");
    List<Map<String,Object>> expectedRows = createTestTableData();

    Cursor cursor = Cursor.createCursor(table);
    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    for(Map<String, Object> row : cursor) {
      foundRows.add(row);
    }
    assertEquals(expectedRows, foundRows);

    db.close();
  }

  public void testSkip() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");
    List<Map<String,Object>> expectedRows = createTestTableData();
    expectedRows.subList(1, 4).clear();

    Cursor cursor = Cursor.createCursor(table);
    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    foundRows.add(cursor.getNextRow());
    assertEquals(3, cursor.skipNextRows(3));
    while(cursor.moveToNextRow()) {
      foundRows.add(cursor.getCurrentRow());
    }
    assertEquals(expectedRows, foundRows);

    assertEquals(0, cursor.skipNextRows(3));

    db.close();
  }

  public void testSearch() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");
    List<Map<String,Object>> expectedRows = createTestTableData();

    Cursor cursor = Cursor.createCursor(table);

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
    
    db.close();
  }

  public void testReverse() throws Exception {
    Database db = createTestTable();

    Table table = db.getTable("test");
    List<Map<String,Object>> expectedRows = createTestTableData();
    Collections.reverse(expectedRows);

    Cursor cursor = Cursor.createCursor(table);
    cursor.afterLast();
    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    while(cursor.moveToPreviousRow()) {
      foundRows.add(cursor.getCurrentRow());
    }
    assertEquals(expectedRows, foundRows);

    db.close();
  }

  
}
