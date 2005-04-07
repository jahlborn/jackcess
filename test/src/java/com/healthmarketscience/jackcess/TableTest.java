// Copyright (c) 2004 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataTypes;
import com.healthmarketscience.jackcess.Table;

import junit.framework.TestCase;

/**
 * @author Tim McCune
 */
public class TableTest extends TestCase {
  
  public TableTest(String name) {
    super(name);
  }
  
  public void testCreateRow() throws Exception {
    Table table = new Table();
    List columns = new ArrayList();
    Column col = new Column();
    col.setType(DataTypes.INT);
    columns.add(col);
    col = new Column();
    col.setType(DataTypes.TEXT);
    columns.add(col);
    columns.add(col);
    table.setColumns(columns);
    int colCount = 3;
    Object[] row = new Object[colCount];
    row[0] = new Short((short) 9);
    row[1] = "Tim";
    row[2] = "McCune";
    ByteBuffer buffer = table.createRow(row);
    assertEquals((short) colCount, buffer.getShort());
    assertEquals((short) 9, buffer.getShort());
    assertEquals((byte) 'T', buffer.get());
    assertEquals((short) 22, buffer.getShort(22));
    assertEquals((short) 10, buffer.getShort(24));
    assertEquals((short) 4, buffer.getShort(26));
    assertEquals((short) 2, buffer.getShort(28));
    assertEquals((byte) 7, buffer.get(30));
  }
  
}
