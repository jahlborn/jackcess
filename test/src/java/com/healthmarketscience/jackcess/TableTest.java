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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import junit.framework.TestCase;

/**
 * @author Tim McCune
 */
public class TableTest extends TestCase {

  private final PageChannel _pageChannel = new PageChannel(true);
  private List<ColumnImpl> _columns = new ArrayList<ColumnImpl>();
  private TableImpl _testTable;
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
    newTestColumn(DataType.TEXT, false);
    newTestColumn(DataType.MEMO, false);
    newTestTable();

    String small = "this is a string";
    String smallNotAscii = "this is a string\0";
    String large = DatabaseTest.createString(30);
    String largeNotAscii = large + "\0";

    ByteBuffer[] buf1 = encodeColumns(small, large);
    ByteBuffer[] buf2 = encodeColumns(smallNotAscii, largeNotAscii);

    reset();
    newTestColumn(DataType.TEXT, true);
    newTestColumn(DataType.MEMO, true);
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
    return _testTable.createRow(
        row, _testTable.getPageChannel().createPageBuffer());
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
    _testTable = new TableImpl(true, _columns) {
        @Override
        public PageChannel getPageChannel() {
          return _pageChannel;
        }
        @Override
        public JetFormat getFormat() {
          return JetFormat.VERSION_4;
        }
      };
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

    ColumnImpl col = new ColumnImpl(null, type, nextColIdx, nextFixedOff, nextVarLenIdx) {
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
        Charset getCharset() {
          return getFormat().CHARSET;
        }
        @Override
        Calendar getCalendar() { 
          return Calendar.getInstance();
        }
        @Override
        public boolean isCompressedUnicode() {
          return compressedUnicode;
        }
      };

    _columns.add(col);
  }
  
}
