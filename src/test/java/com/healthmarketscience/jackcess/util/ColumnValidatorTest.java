/*
Copyright (c) 2014 James Ahlborn

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

import java.util.List;
import java.util.Map;

import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author James Ahlborn
 */
public class ColumnValidatorTest
{
  @Test
  public void testValidate() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      ColumnValidatorFactory initFact = db.getColumnValidatorFactory();
      assertNotNull(initFact);

      Table table = new TableBuilder("Test")
        .addColumn(new ColumnBuilder("id", DataType.LONG).setAutoNumber(true))
        .addColumn(new ColumnBuilder("data", DataType.TEXT))
        .addColumn(new ColumnBuilder("num", DataType.LONG))
        .setPrimaryKey("id")
        .toTable(db);

      for(Column col : table.getColumns()) {
        assertSame(SimpleColumnValidator.INSTANCE, col.getColumnValidator());
      }

      int val = -1;
      for(int i = 1; i <= 3; ++i) {
        table.addRow(Column.AUTO_NUMBER, "row" + i, val++);
      }

      table = null;

      // force table to be reloaded
      clearTableCache(db);

      final ColumnValidator cv = (col, v1) -> {
          Number num = (Number)v1;
          if((num == null) || (num.intValue() < 0)) {
            throw new IllegalArgumentException("not gonna happen");
          }
          return v1;
        };

      ColumnValidatorFactory fact = col -> {
          Table t = col.getTable();
          assertFalse(t.isSystem());
          if(!"Test".equals(t.getName())) {
            return null;
          }

          if(col.getType() == DataType.LONG) {
            return cv;
          }

          return null;
        };

      db.setColumnValidatorFactory(fact);

      final Table test = db.getTable("Test");

      for(Column col : test.getColumns()) {
        ColumnValidator cur = col.getColumnValidator();
        assertNotNull(cur);
        if("num".equals(col.getName())) {
          assertSame(cv, cur);
        } else {
          assertSame(SimpleColumnValidator.INSTANCE, cur);
        }
      }

      Column idCol = test.getColumn("id");
      Column dataCol = test.getColumn("data");
      Column numCol = test.getColumn("num");

      assertThrows(IllegalArgumentException.class, () -> idCol.setColumnValidator(cv));

      assertSame(SimpleColumnValidator.INSTANCE, idCol.getColumnValidator());

      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                                                () -> test.addRow(Column.AUTO_NUMBER, "row4", -3));
      assertEquals("not gonna happen", e.getMessage());

      test.addRow(Column.AUTO_NUMBER, "row4", 4);

      List<? extends Map<String, Object>> expectedRows =
        createExpectedTable(
            createExpectedRow("id", 1, "data", "row1", "num", -1),
            createExpectedRow("id", 2, "data", "row2", "num", 0),
            createExpectedRow("id", 3, "data", "row3", "num", 1),
            createExpectedRow("id", 4, "data", "row4", "num", 4));

      assertTable(expectedRows, test);

      IndexCursor pkCursor = CursorBuilder.createPrimaryKeyCursor(test);
      assertNotNull(pkCursor.findRowByEntry(1));

      pkCursor.setCurrentRowValue(dataCol, "row1_mod");

      assertEquals(createExpectedRow("id", 1, "data", "row1_mod", "num", -1),
                   pkCursor.getCurrentRow());

      e = assertThrows(IllegalArgumentException.class,
                       () -> pkCursor.setCurrentRowValue(numCol, -2));
      assertEquals("not gonna happen", e.getMessage());

      assertEquals(createExpectedRow("id", 1, "data", "row1_mod", "num", -1),
                   pkCursor.getCurrentRow());

      Row row3 = CursorBuilder.findRowByPrimaryKey(test, 3);

      row3.put("num", -2);

      e = assertThrows(IllegalArgumentException.class, () -> test.updateRow(row3));
      assertEquals("not gonna happen", e.getMessage());

      assertEquals(createExpectedRow("id", 3, "data", "row3", "num", 1),
                   CursorBuilder.findRowByPrimaryKey(test, 3));

      final ColumnValidator cv2 = (col, v1) -> {
          Number num = (Number)v1;
          if((num == null) || (num.intValue() < 0)) {
            return 0;
          }
          return v1;
        };

      numCol.setColumnValidator(cv2);

      test.addRow(Column.AUTO_NUMBER, "row5", -5);

      expectedRows =
        createExpectedTable(
            createExpectedRow("id", 1, "data", "row1_mod", "num", -1),
            createExpectedRow("id", 2, "data", "row2", "num", 0),
            createExpectedRow("id", 3, "data", "row3", "num", 1),
            createExpectedRow("id", 4, "data", "row4", "num", 4),
            createExpectedRow("id", 5, "data", "row5", "num", 0));

      assertTable(expectedRows, test);

      assertNotNull(pkCursor.findRowByEntry(3));
      pkCursor.setCurrentRowValue(numCol, -10);

      assertEquals(createExpectedRow("id", 3, "data", "row3", "num", 0),
                   pkCursor.getCurrentRow());

      db.close();
    }
  }
}
