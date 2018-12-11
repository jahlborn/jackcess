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

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.JetFormat;
import com.healthmarketscience.jackcess.impl.PageChannel;
import com.healthmarketscience.jackcess.impl.TableImpl;
import junit.framework.TestCase;

/**
 * @author Tim McCune
 */
public class TableTest extends TestCase {

  private final PageChannel _pageChannel = new PageChannel(true) {};
  private List<ColumnImpl> _columns = new ArrayList<ColumnImpl>();
  private TestTable _testTable;
  private int _varLenIdx;
  private int _fixedOffset;


  public TableTest(String name) {
    super(name);
  }

  private void reset() {
    _testTable = null;
    _columns = new ArrayList<ColumnImpl>();
    _varLenIdx = 0;
    _fixedOffset = 0;
  }

  public void testCreateRow() throws Exception {
    reset();
    newTestColumn(DataType.INT, false);
    newTestColumn(DataType.TEXT, false);
    newTestColumn(DataType.TEXT, false);
    newTestTable();

    int colCount = _columns.size();
    ByteBuffer buffer = createRow(9, "Tim", "McCune");

    assertEquals((short) colCount, buffer.getShort());
    assertEquals((short) 9, buffer.getShort());
    assertEquals((byte) 'T', buffer.get());
    assertEquals((short) 22, buffer.getShort(22));
    assertEquals((short) 10, buffer.getShort(24));
    assertEquals((short) 4, buffer.getShort(26));
    assertEquals((short) 2, buffer.getShort(28));
    assertEquals((byte) 7, buffer.get(30));
  }

  public void testUnicodeCompression() throws Exception {
    reset();
    newTestColumn(DataType.TEXT, false);
    newTestColumn(DataType.TEXT, false);
    newTestTable();

    String small = "this is a string";
    String smallNotAscii = "this is a string\0";
    String large = TestUtil.createString(30);
    String largeNotAscii = large + "\0";

    ByteBuffer[] buf1 = encodeColumns(small, large);
    ByteBuffer[] buf2 = encodeColumns(smallNotAscii, largeNotAscii);

    reset();
    newTestColumn(DataType.TEXT, true);
    newTestColumn(DataType.TEXT, true);
    newTestTable();

    ByteBuffer[] bufCmp1 = encodeColumns(small, large);
    ByteBuffer[] bufCmp2 = encodeColumns(smallNotAscii, largeNotAscii);

    assertEquals(buf1[0].remaining(),
                 (bufCmp1[0].remaining() + small.length() - 2));
    assertEquals(buf1[1].remaining(),
                 (bufCmp1[1].remaining() + large.length() - 2));

    for(int i = 0; i < buf2.length; ++i) {
      assertTrue(Arrays.equals(toBytes(buf2[i]), toBytes(bufCmp2[i])));
    }

    assertEquals(Arrays.asList(small, large),
                 Arrays.asList(decodeColumns(bufCmp1)));
    assertEquals(Arrays.asList(smallNotAscii, largeNotAscii),
                 Arrays.asList(decodeColumns(bufCmp2)));

  }

  private ByteBuffer createRow(Object... row)
    throws IOException
  {
    return _testTable.createRow(row);
  }

  private ByteBuffer[] encodeColumns(Object... row)
    throws IOException
  {
    ByteBuffer[] result = new ByteBuffer[_columns.size()];
    for(int i = 0; i < _columns.size(); ++i) {
      ColumnImpl col = _columns.get(i);
      result[i] = col.write(row[i], _testTable.getFormat().MAX_ROW_SIZE);
    }
    return result;
  }

  private Object[] decodeColumns(ByteBuffer[] buffers)
    throws IOException
  {
    Object[] result = new Object[_columns.size()];
    for(int i = 0; i < _columns.size(); ++i) {
      ColumnImpl col = _columns.get(i);
      result[i] = col.read(toBytes(buffers[i]));
    }
    return result;
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    buffer.rewind();
    byte[] b = new byte[buffer.remaining()];
    buffer.get(b);
    return b;
  }

  private TableImpl newTestTable()
    throws Exception
  {
    _testTable = new TestTable();
    return _testTable;
  }

  private void newTestColumn(DataType type, final boolean compressedUnicode) {

    int nextColIdx = _columns.size();
    int nextVarLenIdx = 0;
    int nextFixedOff = 0;

    if(type.isVariableLength()) {
      nextVarLenIdx = _varLenIdx++;
    } else {
      nextFixedOff = _fixedOffset;
      _fixedOffset += type.getFixedSize();
    }

    ColumnImpl col = new ColumnImpl(null, null, type, nextColIdx, nextFixedOff,
                                    nextVarLenIdx) {
        @Override
        public TableImpl getTable() {
          return _testTable;
        }
        @Override
        public JetFormat getFormat() {
          return getTable().getFormat();
        }
        @Override
        public PageChannel getPageChannel() {
          return getTable().getPageChannel();
        }
        @Override
        protected Charset getCharset() {
          return getFormat().CHARSET;
        }
        @Override
        protected TimeZone getTimeZone() {
          return TimeZone.getDefault();
        }
        @Override
        public boolean isCompressedUnicode() {
          return compressedUnicode;
        }
      };

    _columns.add(col);
  }

  private class TestTable extends TableImpl {
    private TestTable() throws IOException {
      super(true, _columns);
    }
    public ByteBuffer createRow(Object... row) throws IOException {
      return super.createRow(row, getPageChannel().createPageBuffer());
    }
    @Override
    public PageChannel getPageChannel() {
      return _pageChannel;
    }
    @Override
    public JetFormat getFormat() {
      return JetFormat.VERSION_4;
    }
  }
}
