/*
Copyright (c) 2017 James Ahlborn

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

package com.healthmarketscience.jackcess.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;

/**
 *
 * @author James Ahlborn
 */
public class CustomLinkResolverTest extends TestCase
{

  public CustomLinkResolverTest(String name) {
    super(name);
  }

  public void testCustomLinkResolver() throws Exception {
    for(final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      db.setLinkResolver(new TestLinkResolver());

      db.createLinkedTable("Table1", "testFile1.txt", "Table1");
      db.createLinkedTable("Table2", "testFile2.txt", "OtherTable2");
      db.createLinkedTable("Table3", "missingFile3.txt", "MissingTable3");
      db.createLinkedTable("Table4", "testFile2.txt", "MissingTable4");

      Table t1 = db.getTable("Table1");
      assertNotNull(t1);
      assertNotSame(db, t1.getDatabase());

      assertTable(createExpectedTable(createExpectedRow("id", 0,
                                                        "data1", "row0"),
                                      createExpectedRow("id", 1,
                                                        "data1", "row1"),
                                      createExpectedRow("id", 2,
                                                        "data1", "row2")),
                  t1);

      Table t2 = db.getTable("Table2");
      assertNotNull(t2);
      assertNotSame(db, t2.getDatabase());

      assertTable(createExpectedTable(createExpectedRow("id", 3,
                                                        "data2", "row3"),
                                      createExpectedRow("id", 4,
                                                        "data2", "row4"),
                                      createExpectedRow("id", 5,
                                                        "data2", "row5")),
                  t2);

      assertNull(db.getTable("Table4"));

      try {
        db.getTable("Table3");
        fail("FileNotFoundException should have been thrown");
      } catch(FileNotFoundException e) {
        // success
      }

      db.close();
    }
  }

  private static class TestLinkResolver extends CustomLinkResolver
  {
    private TestLinkResolver()
    {
      super(DEFAULT_FORMAT, true, DEFAULT_TEMP_DIR);
    }

    @Override
    protected Object loadCustomFile(
        Database linkerDb, String linkeeFileName) throws IOException
    {
      return (("testFile1.txt".equals(linkeeFileName) ||
               "testFile2.txt".equals(linkeeFileName)) ?
              linkeeFileName : null);
    }

    @Override
    protected boolean loadCustomTable(
        Database tempDb, Object customFile, String tableName)
      throws IOException
    {
      if("Table1".equals(tableName)) {

        assertEquals("testFile1.txt", customFile);
        Table t = new TableBuilder(tableName)
          .addColumn(new ColumnBuilder("id", DataType.LONG))
          .addColumn(new ColumnBuilder("data1", DataType.TEXT))
          .toTable(tempDb);

        for(int i = 0; i < 3; ++i) {
          t.addRow(i, "row" + i);
        }

        return true;

      } else if("OtherTable2".equals(tableName)) {

        assertEquals("testFile2.txt", customFile);
        Table t = new TableBuilder(tableName)
          .addColumn(new ColumnBuilder("id", DataType.LONG))
          .addColumn(new ColumnBuilder("data2", DataType.TEXT))
          .toTable(tempDb);

        for(int i = 3; i < 6; ++i) {
          t.addRow(i, "row" + i);
        }

        return true;

      } else if("Table4".equals(tableName)) {

        assertEquals("testFile2.txt", customFile);
        return false;
      }

      return false;
    }

    @Override
    protected Database createTempDb(Object customFile, FileFormat format,
                                    boolean inMemory, Path tempDir)
      throws IOException
    {
      inMemory = "testFile1.txt".equals(customFile);
      return super.createTempDb(customFile, format, inMemory, tempDir);
    }
  }
}
