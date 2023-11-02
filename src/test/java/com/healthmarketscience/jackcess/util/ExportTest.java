/*
Copyright (c) 2007 Health Market Science, Inc.

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

import java.io.BufferedWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.healthmarketscience.jackcess.impl.JetFormatTest;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author James Ahlborn
 */
public class ExportTest
{
  private static final String NL = System.lineSeparator();

  @Test
  public void testExportToFile() throws Exception
  {
    DateFormat df = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
    df.setTimeZone(TEST_TZ);

    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);
      db.setDateTimeType(DateTimeType.DATE);
      db.setTimeZone(TEST_TZ);

      Table t = new TableBuilder("test")
        .addColumn(new ColumnBuilder("col1", DataType.TEXT))
        .addColumn(new ColumnBuilder("col2", DataType.LONG))
        .addColumn(new ColumnBuilder("col3", DataType.DOUBLE))
        .addColumn(new ColumnBuilder("col4", DataType.OLE))
        .addColumn(new ColumnBuilder("col5", DataType.BOOLEAN))
        .addColumn(new ColumnBuilder("col6", DataType.SHORT_DATE_TIME))
        .toTable(db);

      Date testDate = df.parse("19801231 00:00:00");
      t.addRow("some text||some more", 13, 13.25, createString(30).getBytes(),
               true, testDate);

      t.addRow("crazy'data\"here", -345, -0.000345, createString(7).getBytes(),
               true, null);

      t.addRow("C:\\temp\\some_file.txt", 25, 0.0, null, false, null);

      StringWriter out = new StringWriter();

      new ExportUtil.Builder(db, "test")
        .exportWriter(new BufferedWriter(out));

      String expected =
        "some text||some more,13,13.25,\"61 62 63 64  65 66 67 68  69 6A 6B 6C  6D 6E 6F 70  71 72 73 74  75 76 77 78\n79 7A 61 62  63 64\",true," + testDate + NL +
        "\"crazy'data\"\"here\",-345,-3.45E-4,61 62 63 64  65 66 67,true," + NL +
        "C:\\temp\\some_file.txt,25,0.0,,false," + NL;

      assertEquals(expected, out.toString());

      out = new StringWriter();

      new ExportUtil.Builder(db, "test")
        .setHeader(true)
        .setDelimiter("||")
        .setQuote('\'')
        .exportWriter(new BufferedWriter(out));

      expected =
        "col1||col2||col3||col4||col5||col6" + NL +
        "'some text||some more'||13||13.25||'61 62 63 64  65 66 67 68  69 6A 6B 6C  6D 6E 6F 70  71 72 73 74  75 76 77 78\n79 7A 61 62  63 64'||true||" + testDate + NL +
        "'crazy''data\"here'||-345||-3.45E-4||61 62 63 64  65 66 67||true||" + NL +
        "C:\\temp\\some_file.txt||25||0.0||||false||" + NL;
      assertEquals(expected, out.toString());

      ExportFilter oddFilter = new SimpleExportFilter() {
        private int _num;
        @Override
        public Object[] filterRow(Object[] row) {
          if((_num++ % 2) == 1) {
            return null;
          }
          return row;
        }
      };

      out = new StringWriter();

      new ExportUtil.Builder(db, "test")
        .setFilter(oddFilter)
        .exportWriter(new BufferedWriter(out));

      expected =
        "some text||some more,13,13.25,\"61 62 63 64  65 66 67 68  69 6A 6B 6C  6D 6E 6F 70  71 72 73 74  75 76 77 78\n79 7A 61 62  63 64\",true," + testDate + NL +
        "C:\\temp\\some_file.txt,25,0.0,,false," + NL;

      assertEquals(expected, out.toString());
    }
  }

}
