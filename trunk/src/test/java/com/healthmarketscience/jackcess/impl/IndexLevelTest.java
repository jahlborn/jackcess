/*
Copyright (c) 2026 James Ahlborn

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

package com.healthmarketscience.jackcess.impl;

import java.nio.ByteBuffer;
import java.util.Random;

import com.healthmarketscience.jackcess.Database;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import junit.framework.TestCase;

/**
 * Tests that an index page records its level in the index tree, which is 0 at
 * the leaves and rises toward the root.
 *
 * @author James Ahlborn
 */
public class IndexLevelTest extends TestCase
{
  private static final String EXTRA_TEXT =
    " some random text to fill out the index and make it fill up pages with" +
    " lots of extra bytes so that a few hundred rows are enough to give the" +
    " index a root page with children under it";

  public IndexLevelTest(String name) {
    super(name);
  }

  public void testLevelIsWritten() throws Exception
  {
    int numDbs = 0;
    for(TestDB testDb : TestDB.getSupportedForBasename(Basename.BIG_INDEX)) {
      ++numDbs;

      Database db = openMem(testDb);
      TableImpl t = (TableImpl)db.getTable("Table1");
      IndexImpl index = t.getIndex("col1");

      Random rand = new Random(13L);
      for(int i = 0; i < 1000; ++i) {
        int nextInt = rand.nextInt(Integer.MAX_VALUE);
        t.addRow("" + nextInt + EXTRA_TEXT, "this is some row data");
      }

      IndexData idxData = index.getIndexData();
      idxData.validate(false);

      db.flush();

      int rootPageNumber = idxData.getRootPageNumber();
      DatabaseImpl dbImpl = (DatabaseImpl)db;
      JetFormat format = dbImpl.getFormat();
      ByteBuffer buffer = dbImpl.getPageChannel().createPageBuffer();

      dbImpl.getPageChannel().readPage(buffer, rootPageNumber);
      assertEquals(PageTypes.INDEX_NODE, buffer.get(0));
      assertTrue("root level " + getLevel(buffer, format),
                 getLevel(buffer, format) >= 1);

      // every other index page agrees: a leaf is 0 and a node is not
      int numNodes = 0;
      for(int pageNumber = 1; ; ++pageNumber) {
        try {
          dbImpl.getPageChannel().readPage(buffer, pageNumber);
        } catch(Exception ignored) {
          break;
        }
        byte pageType = buffer.get(0);
        if(pageType == PageTypes.INDEX_LEAF) {
          assertEquals("page " + pageNumber, 0, getLevel(buffer, format));
        } else if(pageType == PageTypes.INDEX_NODE) {
          assertTrue("page " + pageNumber, getLevel(buffer, format) >= 1);
          ++numNodes;
        }
      }

      assertTrue(numNodes > 0);

      db.close();
    }

    assertTrue(numDbs > 0);
  }

  private static int getLevel(ByteBuffer buffer, JetFormat format) {
    return ByteUtil.getUnsignedByte(buffer, format.OFFSET_INDEX_LEVEL);
  }
}
