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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.DatabaseTest.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.impl.IndexImpl;

/**
 * @author james
 */
public class BigIndexTest extends TestCase {

  public BigIndexTest(String name) {
    super(name);
  }
  
  public void testComplexIndex() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.COMP_INDEX, true)) {
      // this file has an index with "compressed" entries and node pages
      Database db = open(testDB);
      TableImpl t = (TableImpl)db.getTable("Table1");
      IndexImpl index = t.getIndex("CD_AGENTE");
      assertFalse(index.isInitialized());
      assertEquals(512, countRows(t));
      assertEquals(512, index.getIndexData().getEntryCount());
      db.close();
    }
  }

  public void testBigIndex() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.BIG_INDEX)) {
    // this file has an index with "compressed" entries and node pages
      Database db = open(testDB);
      TableImpl t = (TableImpl)db.getTable("Table1");
      IndexImpl index = t.getIndex("col1");
      assertFalse(index.isInitialized());
      assertEquals(0, countRows(t));
      assertEquals(0, index.getIndexData().getEntryCount());
      db.close();

      DatabaseTest._autoSync = false;
      try {

        String extraText = " some random text to fill out the index and make it fill up pages with lots of extra bytes so i will keep typing until i think that i probably have enough text in the index entry so that i do not need to add as many entries in order";

        // copy to temp file and attempt to edit
        db = openCopy(testDB);
        t = (TableImpl)db.getTable("Table1");
        index = t.getIndex("col1");

        // add 2,000 (pseudo) random entries to the table
        Random rand = new Random(13L);
        for(int i = 0; i < 2000; ++i) {
          if((i == 850) || (i == 1850)) {
            int end = i + 50;
            List<Object[]> rows = new ArrayList<Object[]>(50);
            for(; i < end; ++i) {
              int nextInt = rand.nextInt(Integer.MAX_VALUE);
              String nextVal = "" + nextInt + extraText;
              if(((i + 1) % 333) == 0) {
                nextVal = null;
              }
              rows.add(new Object[]{nextVal,
                                    "this is some row data " + nextInt});
            }
            t.addRows(rows);
            --i;
          } else {
            int nextInt = rand.nextInt(Integer.MAX_VALUE);
            String nextVal = "" + nextInt + extraText;
            if(((i + 1) % 333) == 0) {
              nextVal = null;
            }
            t.addRow(nextVal, "this is some row data " + nextInt);
          }
        }

        index.getIndexData().validate();

        db.flush();
        t = null;
        System.gc();
        
        t = (TableImpl)db.getTable("Table1");
        index = t.getIndex("col1");

        // make sure all entries are there and correctly ordered
        String firstValue = "      ";
        String prevValue = firstValue;
        int rowCount = 0;
        List<String> firstTwo = new ArrayList<String>();
        for(Map<String,Object> row : CursorBuilder.createCursor(t, index)) {
          String origVal = (String)row.get("col1");
          String val = origVal;
          if(val == null) {
            val = firstValue;
          }
          assertTrue("" + prevValue + " <= " + val + " " + rowCount,
                     prevValue.compareTo(val) <= 0);
          if(firstTwo.size() < 2) {
            firstTwo.add(origVal);
          }
          prevValue = val;
          ++rowCount;
        }

        assertEquals(2000, rowCount);

        index.getIndexData().validate();

        // delete an entry in the middle
        Cursor cursor = CursorBuilder.createCursor(t, index);
        for(int i = 0; i < (rowCount / 2); ++i) {
          assertTrue(cursor.moveToNextRow());
        }
        cursor.deleteCurrentRow();
        --rowCount;

        // remove all but the first two entries (from the end)
        cursor.afterLast();
        for(int i = 0; i < (rowCount - 2); ++i) {
          assertTrue(cursor.moveToPreviousRow());
          cursor.deleteCurrentRow();
        }

        index.getIndexData().validate();

        List<String> found = new ArrayList<String>();
        for(Map<String,Object> row : CursorBuilder.createCursor(t, index)) {
          found.add((String)row.get("col1"));
        }

        assertEquals(firstTwo, found);

        // remove remaining entries
        cursor = CursorBuilder.createCursor(t);
        for(int i = 0; i < 2; ++i) {
          assertTrue(cursor.moveToNextRow());
          cursor.deleteCurrentRow();
        }

        assertFalse(cursor.moveToNextRow());
        assertFalse(cursor.moveToPreviousRow());

        index.getIndexData().validate();

        // add 50 (pseudo) random entries to the table
        rand = new Random(42L);
        for(int i = 0; i < 50; ++i) {
          int nextInt = rand.nextInt(Integer.MAX_VALUE);
          String nextVal = "some prefix " + nextInt + extraText;
          if(((i + 1) % 3333) == 0) {
            nextVal = null;
          }
          t.addRow(nextVal, "this is some row data " + nextInt);
        }

        index.getIndexData().validate();

        cursor = CursorBuilder.createCursor(t, index);
        while(cursor.moveToNextRow()) {
          cursor.deleteCurrentRow();
        }

        index.getIndexData().validate();

        db.close();

      } finally {
        DatabaseTest._autoSync = Database.DEFAULT_AUTO_SYNC;
      }
    }
  }

}
