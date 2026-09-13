/*
Copyright (c) 2016 James Ahlborn

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.Database.FileFormat;
import static com.healthmarketscience.jackcess.DatabaseBuilder.*;
import static com.healthmarketscience.jackcess.TestUtil.*;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.TableImpl;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author James Ahlborn
 */
public class TableUpdaterTest
{

  @Test
  public void testTableUpdating() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      try (Database db = create(fileFormat)) {

        doTestUpdating(db, false, true, null);

      }
    }
  }

  @Test
  public void testTableUpdatingOneToOne() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      try (Database db = create(fileFormat)) {

        doTestUpdating(db, true, true, null);

      }
    }
  }

  @Test
  public void testTableUpdatingNoEnforce() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      try (Database db = create(fileFormat)) {

        doTestUpdating(db, false, false, null);

      }
    }
  }

  @Test
  public void testTableUpdatingNamedRelationship() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      try (Database db = create(fileFormat)) {

        doTestUpdating(db, false, true, "FKnun3jvv47l9kyl74h85y8a0if");

      }
    }
  }

  private void doTestUpdating(Database db, boolean oneToOne, boolean enforce, String relationshipName)
    throws Exception
  {
    Table t1 = newTable("TestTable")
      .addColumn(newColumn("id", DataType.LONG))
      .toTable(db);

    Table t2 = newTable("TestTable2")
      .addColumn(newColumn("id2", DataType.LONG))
      .toTable(db);

    int t1idxs = 1;
    newPrimaryKey("id")
      .addToTable(t1);
    newColumn("data", DataType.TEXT)
      .addToTable(t1);
    newColumn("bigdata", DataType.MEMO)
      .addToTable(t1);

    newColumn("data2", DataType.TEXT)
      .addToTable(t2);
    newColumn("bigdata2", DataType.MEMO)
      .addToTable(t2);

    int t2idxs = 0;
    if(oneToOne) {
      ++t2idxs;
      newPrimaryKey("id2")
        .addToTable(t2);
    }

    RelationshipBuilder rb = newRelationship("TestTable", "TestTable2")
      .addColumns("id", "id2");
    if(enforce) {
      ++t1idxs;
      ++t2idxs;
      rb.setReferentialIntegrity()
        .setCascadeDeletes();
    }

    if (relationshipName != null) {
      rb.setName(relationshipName);
    }

    Relationship rel = rb.toRelationship(db);

    if (relationshipName == null) {
      assertEquals("TestTableTestTable2", rel.getName());
    } else {
      assertEquals(relationshipName, rel.getName());
    }
    assertSame(t1, rel.getFromTable());
    assertEquals(Arrays.asList(t1.getColumn("id")), rel.getFromColumns());
    assertSame(t2, rel.getToTable());
    assertEquals(Arrays.asList(t2.getColumn("id2")), rel.getToColumns());
    assertEquals(oneToOne, rel.isOneToOne());
    assertEquals(enforce, rel.hasReferentialIntegrity());
    assertEquals(enforce, rel.cascadeDeletes());
    assertFalse(rel.cascadeUpdates());
    assertEquals(Relationship.JoinType.INNER, rel.getJoinType());

    assertEquals(t1idxs, t1.getIndexes().size());
    assertEquals(1, ((TableImpl)t1).getIndexDatas().size());

    assertEquals(t2idxs, t2.getIndexes().size());
    assertEquals((t2idxs > 0 ? 1 : 0), ((TableImpl)t2).getIndexDatas().size());

    ((DatabaseImpl)db).getPageChannel().startWrite();
    try {

      for(int i = 0; i < 10; ++i) {
        t1.addRow(i, "row" + i, "row-data" + i);
      }

      for(int i = 0; i < 10; ++i) {
        t2.addRow(i, "row2_" + i, "row-data2_" + i);
      }

    } finally {
      ((DatabaseImpl)db).getPageChannel().finishWrite();
    }

    try {
      t2.addRow(10, "row10", "row-data10");
      if(enforce) {
        fail("ConstraintViolationException should have been thrown");
      }
    } catch(ConstraintViolationException cv) {
      // success
      if(!enforce) { throw cv; }
    }

    Row r1 = CursorBuilder.findRowByPrimaryKey(t1, 5);
    t1.deleteRow(r1);

    int id = 0;
    for(Row r : t1) {
      assertEquals(id, r.get("id"));
      ++id;
      if(id == 5) {
        ++id;
      }
    }

    id = 0;
    for(Row r : t2) {
      assertEquals(id, r.get("id2"));
      ++id;
      if(enforce && (id == 5)) {
        ++id;
      }
    }
  }

  @Test
  public void testInvalidUpdate() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      try (Database db = create(fileFormat)) {

        Table t1 = newTable("TestTable")
          .addColumn(newColumn("id", DataType.LONG))
          .toTable(db);

        try {
          newColumn("ID", DataType.TEXT)
            .addToTable(t1);
          fail("created table with no columns?");
        } catch(IllegalArgumentException e) {
          // success
        }

        Table t2 = newTable("TestTable2")
          .addColumn(newColumn("id2", DataType.LONG))
          .toTable(db);

        try {
          newRelationship(t1, t2)
            .toRelationship(db);
          fail("created rel with no columns?");
        } catch(IllegalArgumentException e) {
          // success
        }

        try {
          newRelationship("TestTable", "TestTable2")
            .addColumns("id", "id")
            .toRelationship(db);
          fail("created rel with wrong columns?");
        } catch(IllegalArgumentException e) {
          // success
        }

      }
    }
  }

  @Test
  public void testAddColumnGetsUnusedId() throws Exception
  {
    // a new column takes an id above every id in use, rather than the
    // physical column number, which in these tables is below the highest id
    for(final TestDB testDB :
          TestDB.getSupportedForBasename(Basename.DEL_COL)) {

      Database db = openCopy(testDB);

      TableImpl t = (TableImpl)db.getTable("Table1");

      short maxId = -1;
      for(ColumnImpl col : t.getColumns()) {
        if(col.getColumnId() > maxId) {
          maxId = col.getColumnId();
        }
      }
      // the deleted columns left the highest id past the last column
      assertTrue(maxId > (t.getColumnCount() - 1));

      newColumn("newCol", DataType.TEXT).addToTable(t);

      db.close();

      t = (TableImpl)open(db.getFile()).getTable("Table1");

      ColumnImpl newCol = t.getColumn("newCol");
      assertEquals((short)(maxId + 1), newCol.getColumnId());

      Set<Short> ids = new HashSet<Short>();
      for(ColumnImpl col : t.getColumns()) {
        assertTrue(ids.add(col.getColumnId()),
                   "duplicate column id " + col.getColumnId());
      }
    }
  }

  @Test
  public void testUpdateLargeTableDef() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      try (Database db = create(fileFormat)) {

        final int numColumns = 89;

        Table t = newTable("test")
          .addColumn(newColumn("first", DataType.TEXT))
          .toTable(db);

        List<String> colNames = new ArrayList<String>();
        colNames.add("first");
        for(int i = 0; i < numColumns; ++i) {
          String colName = "MyColumnName" + i;
          colNames.add(colName);
          DataType type = (((i % 3) == 0) ? DataType.MEMO : DataType.TEXT);
          newColumn(colName, type)
            .addToTable(t);
        }

        List<String> row = new ArrayList<String>();
        Map<String,Object> expectedRowData = new LinkedHashMap<String, Object>();
        for(int i = 0; i < colNames.size(); ++i) {
          String value = "" + i + " some row data";
          row.add(value);
          expectedRowData.put(colNames.get(i), value);
        }

        t.addRow(row.toArray());

        t.reset();
        assertEquals(expectedRowData, t.getNextRow());

      }
    }
  }

  @Test
  public void testCorruptTableDef() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {

      // variable length column count which does not cover all the variable
      // length columns.  writing a row would run off the end of the offset
      // table.  an older version of jackcess could write this count when
      // adding a column
      checkCorruptTableDef(
          fileFormat, t -> setTableField(t, "_maxVarColumnCount", (short)1),
          "data2");

      // column count which does not cover all the columns.  writing a row
      // would run off the end of the null mask
      checkCorruptTableDef(
          fileFormat, t -> setTableField(t, "_maxColumnCount", (short)1),
          "data1");

      // fixed length column which ends beyond the maximum row size.  writing
      // a row would run off the end of the row buffer
      checkCorruptTableDef(
          fileFormat, t -> setColumnField(t, "id", "_fixedDataOffset", 5000),
          "id");
    }
  }

  /** damages part of a table definition which has already been loaded */
  private interface TableDefDamager
  {
    void damage(Table t) throws Exception;
  }

  private static void checkCorruptTableDef(
      FileFormat fileFormat, TableDefDamager damager, String expectedColName)
    throws Exception
  {
    try(Database db = create(fileFormat)) {

      Table t = newTable("test")
        .addColumn(newColumn("id", DataType.LONG))
        .addColumn(newColumn("data1", DataType.TEXT))
        .addColumn(newColumn("data2", DataType.TEXT))
        .toTable(db);

      damager.damage(t);

      // the table def is validated when the table is loaded, so re-run the
      // validation now that the def has been damaged
      java.lang.reflect.Method m =
        TableImpl.class.getDeclaredMethod("validateColumnDefs");
      m.setAccessible(true);
      m.invoke(t);

      // rows can still be read
      assertNull(t.getNextRow());

      // but not written
      try {
        t.addRow(1, "foo", "bar");
        fail("JackcessException should have been thrown");
      } catch(JackcessException e) {
        assertTrue(e.getMessage().contains("Table definition is corrupt"));
        assertTrue(e.getMessage().contains(expectedColName));
      }
    }
  }

  private static void setTableField(Table t, String fieldName, short value)
    throws Exception
  {
    java.lang.reflect.Field f = TableImpl.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    f.setShort(t, value);
  }

  private static void setColumnField(
      Table t, String colName, String fieldName, int value)
    throws Exception
  {
    java.lang.reflect.Field f = ColumnImpl.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    f.setInt(t.getColumn(colName), value);
  }
}
