/*
Copyright (c) 2008 Health Market Science, Inc.

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
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.DatabaseTest.*;


/**
 * @author james
 */
public class BigIndexTest extends TestCase {

  /**
   * Creates a new <code>IndexTest</code> instance.
   *
   */
  public BigIndexTest(String name) {
    super(name);
  }

  @Override
  protected void setUp() {
    System.setProperty(Database.USE_BIG_INDEX_PROPERTY,
                       Boolean.TRUE.toString());
  }
  
  @Override
  protected void tearDown() {
    System.clearProperty(Database.USE_BIG_INDEX_PROPERTY);
  }
  
  public void testComplexIndex() throws Exception
  {
    // this file has an index with "compressed" entries and node pages
    File origFile = new File("test/data/compIndexTest.mdb");
    Database db = open(origFile);
    Table t = db.getTable("Table1");
    Index index = t.getIndex("CD_AGENTE");
    assertFalse(index.isInitialized());
    assertEquals(512, countRows(t));
    assertEquals(512, index.getEntryCount());
    db.close();

    DatabaseTest._autoSync = false;
    try {
      
      // copy to temp file and attempt to edit
      db = openCopy(origFile);
      t = db.getTable("Table1");
      index = t.getIndex("CD_AGENTE");

      System.out.println("BigIndexTest: Index type: " + index.getClass());

      // add 10,000 (pseudo) random entries to the table
      Random rand = new Random(13L);
      for(int i = 0; i < 10000; ++i) {
        Integer nextInt = rand.nextInt();
        if(((i + 1) % 3333) == 0) {
          nextInt = null;
        }
        t.addRow(nextInt,
                 "this is some row data " + nextInt,
                 "this is some more row data " + nextInt);
      }

      ((BigIndex)index).validate();
      
      db.flush();
      t = db.getTable("Table1");
      index = t.getIndex("CD_AGENTE");

      // make sure all entries are there and correctly ordered
      int lastValue = Integer.MAX_VALUE;
      int rowCount = 0;
      List<Integer> firstTwo = new ArrayList<Integer>();
      for(Map<String,Object> row : Cursor.createIndexCursor(t, index)) {
        Integer tmpVal = (Integer)row.get("CD_AGENTE");
        int val = ((tmpVal != null) ? (int)tmpVal : Integer.MIN_VALUE);
        assertTrue("" + val + " <= " + lastValue + " " + rowCount,
                   val <= lastValue);
        if(firstTwo.size() < 2) {
          firstTwo.add(tmpVal);
        }
        lastValue = val;
        ++rowCount;
      }

      assertEquals(10512, rowCount);

      ((BigIndex)index).validate();
      
      // remove all but the first two entries
      Cursor cursor = new CursorBuilder(t).setIndex(index)
        .afterLast().toCursor();
      for(int i = 0; i < (rowCount - 2); ++i) {
        assertTrue(cursor.moveToPreviousRow());
        cursor.deleteCurrentRow();
      }

      ((BigIndex)index).validate();
      
      List<Integer> found = new ArrayList<Integer>();
      for(Map<String,Object> row : Cursor.createIndexCursor(t, index)) {
        found.add((Integer)row.get("CD_AGENTE"));
      }      

      assertEquals(firstTwo, found);

      // remove remaining entries
      cursor = Cursor.createCursor(t);
      for(int i = 0; i < 2; ++i) {
        assertTrue(cursor.moveToNextRow());
        cursor.deleteCurrentRow();
      }

      assertFalse(cursor.moveToNextRow());
      assertFalse(cursor.moveToPreviousRow());
      
      ((BigIndex)index).validate();
      
      db.close();
      
    } finally {
      DatabaseTest._autoSync = Database.DEFAULT_AUTO_SYNC;
    }
  }

  public void x_testBigIndex() throws Exception
  {
    File f = new File("test/data/databaseTest19731_ind.mdb");
    Database db = open(f);
    Table t = db.getTable("test");
    System.out.println("FOO reading index");
    Index index = t.getIndexes().get(0);
    index.initialize();
    System.out.println(index);
    db.close();
  }


}
