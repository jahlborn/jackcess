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

package com.healthmarketscience.jackcess;

import java.io.BufferedWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import junit.framework.TestCase;
import org.apache.commons.lang.SystemUtils;

import static com.healthmarketscience.jackcess.Database.*;
import static com.healthmarketscience.jackcess.DatabaseTest.*;

/**
 *
 * @author James Ahlborn
 */
public class ExportTest extends TestCase
{
  private static final String NL = SystemUtils.LINE_SEPARATOR;


  public ExportTest(String name) {
    super(name);
  }

  public void testExportToFile() throws Exception
  {
    DateFormat df = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table t = new TableBuilder("test")
        .addColumn(new ColumnBuilder("col1", DataType.TEXT).toColumn())
        .addColumn(new ColumnBuilder("col2", DataType.LONG).toColumn())
        .addColumn(new ColumnBuilder("col3", DataType.DOUBLE).toColumn())
        .addColumn(new ColumnBuilder("col4", DataType.OLE).toColumn())
        .addColumn(new ColumnBuilder("col5", DataType.BOOLEAN).toColumn())
        .addColumn(new ColumnBuilder("col6", DataType.SHORT_DATE_TIME).toColumn())
        .toTable(db);

      t.addRow("some text||some more", 13, 13.25, createString(30).getBytes(), true, df.parse("19801231 00:00:00"));

      t.addRow("crazy'data\"here", -345, -0.000345, createString(7).getBytes(),
               true, null);

      StringWriter out = new StringWriter();

      ExportUtil.exportWriter(db, "test", new BufferedWriter(out));

      String expected = 
        "some text||some more,13,13.25,\"61 62 63 64  65 66 67 68  69 6A 6B 6C  6D 6E 6F 70  71 72 73 74  75 76 77 78" + NL +
        "79 7A 61 62  63 64\",true,Wed Dec 31 00:00:00 EST 1980" + NL +
        "\"crazy'data\"\"here\",-345,-3.45E-4,61 62 63 64  65 66 67,true," + NL;

      assertEquals(expected, out.toString());

      out = new StringWriter();
      
      ExportUtil.exportWriter(db, "test", new BufferedWriter(out),
                              true, "||", '\'', SimpleExportFilter.INSTANCE);

      expected = 
        "col1||col2||col3||col4||col5||col6" + NL +
        "'some text||some more'||13||13.25||'61 62 63 64  65 66 67 68  69 6A 6B 6C  6D 6E 6F 70  71 72 73 74  75 76 77 78" + NL +
        "79 7A 61 62  63 64'||true||Wed Dec 31 00:00:00 EST 1980" + NL +
        "'crazy''data\"here'||-345||-3.45E-4||61 62 63 64  65 66 67||true||" + NL;
      assertEquals(expected, out.toString());
    }
  }

}
