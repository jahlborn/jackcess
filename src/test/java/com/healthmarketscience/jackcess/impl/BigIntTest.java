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

package com.healthmarketscience.jackcess.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;

/**
 *
 * @author James Ahlborn
 */
public class BigIntTest extends TestCase
{

  public BigIntTest(String name) throws Exception {
    super(name);
  }

  public void testBigInt() throws Exception {

    for (final Database.FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      JetFormat format = DatabaseImpl.getFileFormatDetails(fileFormat)
        .getFormat();

      if(!format.isSupportedDataType(DataType.BIG_INT)) {
        continue;
      }

      Database db = create(fileFormat);

      Table t = new TableBuilder("Test")
        .addColumn(new ColumnBuilder("id", DataType.LONG)
                   .setAutoNumber(true))
        .addColumn(new ColumnBuilder("data1", DataType.TEXT))
        .addColumn(new ColumnBuilder("num1", DataType.BIG_INT))
        .addIndex(new IndexBuilder("idx").addColumns("num1"))
        .toTable(db);

      long[] vals = new long[] {
        0L, -10L, 3844L, -45309590834L, 50392084913L, 65000L, -6489273L};

      List<Map<String, Object>> expectedTable = 
        new ArrayList<Map<String, Object>>();

      int idx = 1;
      for(long lng : vals) {
        t.addRow(Column.AUTO_NUMBER, "" + lng, lng);

        expectedTable.add(createExpectedRow(
                              "id", idx++,
                              "data1", "" + lng,
                              "num1", lng));
      }

      Collections.sort(expectedTable, new Comparator<Map<String, Object>>() {
          public int compare(
              Map<String, Object> r1,
              Map<String, Object> r2) {
            Long l1 = (Long)r1.get("num1");
            Long l2 = (Long)r2.get("num1");
            return l1.compareTo(l2);
          }
        });
      
      Cursor c = new CursorBuilder(t).setIndexByName("idx").toIndexCursor();

      assertCursor(expectedTable, c);

      db.close();
    }
  }
}
