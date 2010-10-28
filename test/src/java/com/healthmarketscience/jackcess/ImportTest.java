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

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.Database.*;
import static com.healthmarketscience.jackcess.DatabaseTest.*;

/** 
 *  @author Rob Di Marco
 */ 
public class ImportTest extends TestCase
{

  public ImportTest(String name) {
    super(name);
  }

  public void testImportFromFile() throws Exception
  {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);
      String tableName = db.importFile(
          "test", new File("test/data/sample-input.tab"), "\\t");
      Table t = db.getTable(tableName);

      List<String> colNames = new ArrayList<String>();
      for(Column c : t.getColumns()) {
        colNames.add(c.getName());
      }
      assertEquals(Arrays.asList("Test1", "Test2", "Test3"), colNames);

      List<Map<String, Object>> expectedRows =
        createExpectedTable(
            createExpectedRow(
                "Test1", "Foo",
                "Test2", "Bar",
                "Test3", "Ralph"),
            createExpectedRow(
                "Test1", "S",
                "Test2", "Mouse",
                "Test3", "Rocks"),
            createExpectedRow(
                "Test1", "",
                "Test2", "Partial line",
                "Test3", null),
            createExpectedRow(
                "Test1", " Quoted Value",
                "Test2", " bazz ",
                "Test3", " Really \"Crazy" + ImportUtil.LINE_SEPARATOR
                + "value\""),
            createExpectedRow(
                "Test1", "buzz",
                "Test2", "embedded\tseparator",
                "Test3", "long")
            );
      assertTable(expectedRows, t);

      db.close();
    }
  }

  public void testImportFromFileWithOnlyHeaders() throws Exception
  {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);
      String tableName = db.importFile(
          "test", new File("test/data/sample-input-only-headers.tab"), "\\t");

      Table t = db.getTable(tableName);

      List<String> colNames = new ArrayList<String>();
      for(Column c : t.getColumns()) {
        colNames.add(c.getName());
      }
      assertEquals(Arrays.asList(
                       "RESULT_PHYS_ID", "FIRST", "MIDDLE", "LAST", "OUTLIER",
                       "RANK", "CLAIM_COUNT", "PROCEDURE_COUNT",
                       "WEIGHTED_CLAIM_COUNT", "WEIGHTED_PROCEDURE_COUNT"), 
                   colNames);

      db.close();
    }
  }

  public void testCopySqlHeaders() throws Exception
  {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {

      TestResultSet rs = new TestResultSet();

      rs.addColumn(Types.INTEGER, "col1");
      rs.addColumn(Types.VARCHAR, "col2", 60, 0, 0);
      rs.addColumn(Types.VARCHAR, "col3", 500, 0, 0);
      rs.addColumn(Types.BINARY, "col4", 128, 0, 0);
      rs.addColumn(Types.BINARY, "col5", 512, 0, 0);
      rs.addColumn(Types.NUMERIC, "col6", 0, 7, 15);
      rs.addColumn(Types.VARCHAR, "col7", Integer.MAX_VALUE, 0, 0);

      Database db = create(fileFormat);
      db.copyTable("Test1", (ResultSet)Proxy.newProxyInstance(
                       Thread.currentThread().getContextClassLoader(),
                       new Class[]{ResultSet.class},
                       rs));

      Table t = db.getTable("Test1");
      List<Column> columns = t.getColumns();
      assertEquals(7, columns.size());

      Column c = columns.get(0);
      assertEquals("col1", c.getName());
      assertEquals(DataType.LONG, c.getType());

      c = columns.get(1);
      assertEquals("col2", c.getName());
      assertEquals(DataType.TEXT, c.getType());
      assertEquals(120, c.getLength());

      c = columns.get(2);
      assertEquals("col3", c.getName());
      assertEquals(DataType.MEMO, c.getType());
      assertEquals(0, c.getLength());

      c = columns.get(3);
      assertEquals("col4", c.getName());
      assertEquals(DataType.BINARY, c.getType());
      assertEquals(128, c.getLength());

      c = columns.get(4);
      assertEquals("col5", c.getName());
      assertEquals(DataType.OLE, c.getType());
      assertEquals(0, c.getLength());

      c = columns.get(5);
      assertEquals("col6", c.getName());
      assertEquals(DataType.NUMERIC, c.getType());
      assertEquals(17, c.getLength());
      assertEquals(7, c.getScale());
      assertEquals(15, c.getPrecision());

      c = columns.get(6);
      assertEquals("col7", c.getName());
      assertEquals(DataType.MEMO, c.getType());
      assertEquals(0, c.getLength());
    }
  }


  private static class TestResultSet implements InvocationHandler
  {
    private List<Integer> _types = new ArrayList<Integer>();
    private List<String> _names = new ArrayList<String>();
    private List<Integer> _displaySizes = new ArrayList<Integer>();
    private List<Integer> _scales = new ArrayList<Integer>();
    private List<Integer> _precisions = new ArrayList<Integer>();
    
    public Object invoke(Object proxy, Method method, Object[] args)
    {
      String methodName = method.getName();
      if(methodName.equals("getMetaData")) {
        return Proxy.newProxyInstance(
            Thread.currentThread().getContextClassLoader(),
            new Class[]{ResultSetMetaData.class},
            this);
      } else if(methodName.equals("next")) {
        return Boolean.FALSE;
      } else if(methodName.equals("getColumnCount")) {
        return _types.size();
      } else if(methodName.equals("getColumnName")) {
        return getValue(_names, args[0]);
      } else if(methodName.equals("getColumnDisplaySize")) {
        return getValue(_displaySizes, args[0]);
      } else if(methodName.equals("getColumnType")) {
        return getValue(_types, args[0]);
      } else if(methodName.equals("getScale")) {
        return getValue(_scales, args[0]);
      } else if(methodName.equals("getPrecision")) {
        return getValue(_precisions, args[0]);
      } else {
        throw new UnsupportedOperationException(methodName);
      }
    }

    public void addColumn(int type, String name)
    {
      addColumn(type, name, 0, 0, 0);
    }

    public void addColumn(int type, String name, int displaySize,
                          int scale, int precision)
    {
      _types.add(type);
      _names.add(name);
      _displaySizes.add(displaySize);
      _scales.add(scale);
      _precisions.add(precision);
    }

    public <T> T getValue(List<T> values, Object index) {
      return values.get((Integer)index - 1);
    }
  }
  
} 
