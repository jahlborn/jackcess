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
    Index index = t.getIndexes().get(0);
    assertFalse(index.isInitialized());
    assertEquals(512, countRows(t));
    assertEquals(512, index.getEntryCount());
    db.close();

    // copy to temp file and attempt to edit
    db = openCopy(origFile);
    t = db.getTable("Table1");
    index = t.getIndexes().get(0);

    System.out.println("BigIndexTest: Index type: " + index.getClass());

    // FIXME
//     Random rand = new Random(13L);
//     for(int i = 0; i < 10000; ++i) {
//       int nextInt = rand.nextInt();
//       t.addRow(nextInt,
//                "this is some row data " + nextInt,
//                "this is some more row data " + nextInt);
//     }
  }


}
