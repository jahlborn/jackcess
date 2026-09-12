/*
Copyright (c) 2021 James Ahlborn

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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import static com.healthmarketscience.jackcess.DatabaseBuilder.*;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 *
 * @author James Ahlborn
 */
public class ExtendedDateTest extends TestCase
{

  public ExtendedDateTest(String name) throws Exception {
    super(name);
  }

  public void testReadExtendedDate() throws Exception {

    DateTimeFormatter dtfNoTime = DateTimeFormatter.ofPattern("M/d/yyy");
    DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("M/d/yyy h:mm:ss.SSSSSSS a");

    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.EXT_DATE)) {

      Database db = openMem(testDB);

      Table t = db.getTable("Table1");
      for(Row r : t) {
        LocalDateTime ldt = r.getLocalDateTime("DateExt");
        String str = r.getString("DateExtStr");

        if(ldt != null) {
          String str1 = dtfNoTime.format(ldt);
          String str2 = dtfFull.format(ldt);

          Assert.assertTrue(str1.equals(str) || str2.equals(str));
        } else {
          Assert.assertNull(str);
        }

      }

      Index idx = t.getIndex("DateExtAsc");
      IndexCodesTest.checkIndexEntries(testDB, t, idx);
      idx = t.getIndex("DateExtDesc");
      IndexCodesTest.checkIndexEntries(testDB, t, idx);

      db.close();
    }
  }

  public void testWriteExtendedDate() throws Exception {

    for (final Database.FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      JetFormat format = DatabaseImpl.getFileFormatDetails(fileFormat)
        .getFormat();

      if(!format.isSupportedDataType(DataType.EXT_DATE_TIME)) {
        continue;
      }

      Database db = create(fileFormat);

      Table t = newTable("Test")
        .addColumn(newColumn("id", DataType.LONG)
                   .setAutoNumber(true))
        .addColumn(newColumn("data1", DataType.TEXT))
        .addColumn(newColumn("extDate", DataType.EXT_DATE_TIME))
        .addIndex(newIndex("idxAsc").addColumns("extDate"))
        .addIndex(newIndex("idxDesc").addColumns(false, "extDate"))
        .toTable(db);

      Object[] ldts = {
        LocalDate.of(2020,6,17),
        LocalDate.of(2021,6,14),
        LocalDateTime.of(2021,6,14,12,45),
        LocalDateTime.of(2021,6,14,1,45),
        LocalDateTime.of(2021,6,14,22,45,12,345678900),
        LocalDateTime.of(1765,6,14,12,45),
        LocalDateTime.of(100,6,14,12,45,00,123456700),
        LocalDateTime.of(1265,6,14,12,45)
      };

      List<Map<String, Object>> expectedTable =
        new ArrayList<Map<String, Object>>();

      int idx = 1;
      for(Object ldt : ldts) {
        t.addRow(Column.AUTO_NUMBER, "" + ldt, ldt);

        LocalDateTime realLdt = (LocalDateTime)ColumnImpl.toInternalValue(
            DataType.EXT_DATE_TIME, ldt, (DatabaseImpl)db);

        expectedTable.add(createExpectedRow(
                              "id", idx++,
                              "data1", "" + ldt,
                              "extDate", realLdt));
      }

      Comparator<Map<String, Object>> comp = (r1, r2) -> {
          LocalDateTime l1 = (LocalDateTime)r1.get("extDate");
          LocalDateTime l2 = (LocalDateTime)r2.get("extDate");
          return l1.compareTo(l2);
      };
      Collections.sort(expectedTable, comp);

      Cursor c = t.newCursor().setIndexByName("idxAsc").toIndexCursor();

      assertCursor(expectedTable, c);

      Collections.sort(expectedTable, comp.reversed());

      c = t.newCursor().setIndexByName("idxDesc").toIndexCursor();

      assertCursor(expectedTable, c);

      db.close();
    }
  }
}
