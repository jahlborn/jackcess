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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Database.FileFormat;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.TestUtil.*;

/**
 *
 * @author James Ahlborn
 */
public class TableUpdaterTest extends TestCase
{

  public TableUpdaterTest(String name) throws Exception {
    super(name);
  }

  public void testTableUpdating() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      doTestUpdating(db, false, true);
 
      db.close();
    }    
  }

  public void testTableUpdatingOneToOne() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      doTestUpdating(db, true, true);
 
      db.close();
    }    
  }

  public void testTableUpdatingNoEnforce() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      doTestUpdating(db, false, false);
 
      db.close();
    }    
  }

  private void doTestUpdating(Database db, boolean oneToOne, boolean enforce) 
    throws Exception
  {
    Table t1 = new TableBuilder("TestTable")
      .addColumn(new ColumnBuilder("id", DataType.LONG))
      .toTable(db);
      
    Table t2 = new TableBuilder("TestTable2")
      .addColumn(new ColumnBuilder("id2", DataType.LONG))
      .toTable(db);

    int t1idxs = 1;
    new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
      .addColumns("id").setPrimaryKey()
      .addToTable(t1);
    new ColumnBuilder("data", DataType.TEXT)
      .addToTable(t1);
    new ColumnBuilder("bigdata", DataType.MEMO)
      .addToTable(t1);

    new ColumnBuilder("data2", DataType.TEXT)
      .addToTable(t2);
    new ColumnBuilder("bigdata2", DataType.MEMO)
      .addToTable(t2);

    int t2idxs = 0;
    if(oneToOne) {
      ++t2idxs;
      new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
        .addColumns("id2").setPrimaryKey()
        .addToTable(t2);
    }

    RelationshipBuilder rb = new RelationshipBuilder("TestTable", "TestTable2")
      .addColumns("id", "id2");
    if(enforce) {
      ++t1idxs;
      ++t2idxs;      
      rb.setReferentialIntegrity()
        .setCascadeDeletes();
    }

    Relationship rel = rb.toRelationship(db);

    assertEquals("TestTableTestTable2", rel.getName());
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

  public void testInvalidUpdate() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table t1 = new TableBuilder("TestTable")
        .addColumn(new ColumnBuilder("id", DataType.LONG))
        .toTable(db);

      try {
        new ColumnBuilder("ID", DataType.TEXT)
          .addToTable(t1);
        fail("created table with no columns?");
      } catch(IllegalArgumentException e) {
        // success
      }

      new TableBuilder("TestTable2")
        .addColumn(new ColumnBuilder("id2", DataType.LONG))
        .toTable(db);

      try {
        new RelationshipBuilder("TestTable", "TestTable2")
          .addColumns("id", "id")
          .toRelationship(db);
        fail("created rel with wrong columns?");
      } catch(IllegalArgumentException e) {
        // success
      }
 
      db.close();
    }    
  }

  public void testUpdateLargeTableDef() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      final int numColumns = 89;

      Table t = new TableBuilder("test")
        .addColumn(new ColumnBuilder("first", DataType.TEXT))
        .toTable(db);

      List<String> colNames = new ArrayList<String>();
      colNames.add("first");
      for(int i = 0; i < numColumns; ++i) {
        String colName = "MyColumnName" + i;
        colNames.add(colName);
        DataType type = (((i % 3) == 0) ? DataType.MEMO : DataType.TEXT);
        new ColumnBuilder(colName, type)
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

      db.close();
    }
  } 
}
