/*
Copyright (c) 2007 Health Market Science, Inc.

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

package com.healthmarketscience.jackcess.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteOrder;
import java.util.List;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import static com.healthmarketscience.jackcess.Database.*;
import static com.healthmarketscience.jackcess.DatabaseTest.*;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.JetFormatTest;
import com.healthmarketscience.jackcess.impl.TableImpl;
import junit.framework.TestCase;

/**
 * @author James Ahlborn
 */
public class ErrorHandlerTest extends TestCase 
{

  public ErrorHandlerTest(String name) {
    super(name);
  }

  public void testErrorHandler() throws Exception
  {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table table =
        new TableBuilder("test")
        .addColumn(new ColumnBuilder("col", DataType.TEXT))
        .addColumn(new ColumnBuilder("val", DataType.LONG))
        .toTable(db);

      table.addRow("row1", 1);
      table.addRow("row2", 2);
      table.addRow("row3", 3);

      assertTable(createExpectedTable(
                      createExpectedRow("col", "row1",
                                        "val", 1),
                      createExpectedRow("col", "row2",
                                        "val", 2),
                      createExpectedRow("col", "row3",
                                        "val", 3)),
                  table);


      replaceColumn(table, "val");

      table.reset();
      try {
        table.getNextRow();
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // success
      }

      table.reset();
      table.setErrorHandler(new ReplacementErrorHandler());

      assertTable(createExpectedTable(
                      createExpectedRow("col", "row1",
                                        "val", null),
                      createExpectedRow("col", "row2",
                                        "val", null),
                      createExpectedRow("col", "row3",
                                        "val", null)),
                  table);

      Cursor c1 = CursorBuilder.createCursor(table);
      Cursor c2 = CursorBuilder.createCursor(table);
      Cursor c3 = CursorBuilder.createCursor(table);

      c2.setErrorHandler(new DebugErrorHandler("#error"));
      c3.setErrorHandler(ErrorHandler.DEFAULT);

      assertCursor(createExpectedTable(
                      createExpectedRow("col", "row1",
                                        "val", null),
                      createExpectedRow("col", "row2",
                                        "val", null),
                      createExpectedRow("col", "row3",
                                        "val", null)),
                  c1);

      assertCursor(createExpectedTable(
                      createExpectedRow("col", "row1",
                                        "val", "#error"),
                      createExpectedRow("col", "row2",
                                        "val", "#error"),
                      createExpectedRow("col", "row3",
                                        "val", "#error")),
                  c2);

      try {
        c3.getNextRow();
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // success
      }

      table.setErrorHandler(null);
      c1.setErrorHandler(null);
      c1.reset();
      try {
        c1.getNextRow();
        fail("IOException should have been thrown");
      } catch(IOException e) {
        // success
      }


      db.close();
    }
  }

  @SuppressWarnings("unchecked")
  private static void replaceColumn(Table t, String colName) throws Exception
  {
    Field colsField = TableImpl.class.getDeclaredField("_columns");
    colsField.setAccessible(true);
    List<Column> cols = (List<Column>)colsField.get(t);

    Column srcCol = null;
    ColumnImpl destCol = new BogusColumn(t, colName);
    for(int i = 0; i < cols.size(); ++i) {
      srcCol = cols.get(i);
      if(srcCol.getName().equals(colName)) {
        cols.set(i, destCol);
        break;
      }
    }

    // copy fields from source to dest
    for(Field f : Column.class.getDeclaredFields()) {
      if(!Modifier.isFinal(f.getModifiers())) {
        f.setAccessible(true);
        f.set(destCol, f.get(srcCol));
      }
    }
    
  }

  private static class BogusColumn extends ColumnImpl
  {
    private BogusColumn(Table table, String name) {
      super((TableImpl)table, name, DataType.LONG, 1, 0, 0);
    }
    
    @Override
    public Object read(byte[] data, ByteOrder order) throws IOException {
      throw new IOException("bogus column");
    }
  }

}
