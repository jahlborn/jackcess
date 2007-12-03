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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

/**
 * @author Tim McCune
 */
public class TableTest extends TestCase {
  
  public TableTest(String name) {
    super(name);
  }
  
  public void testCreateRow() throws Exception {
    final JetFormat format = JetFormat.VERSION_4;
    final PageChannel pageChannel = new PageChannel(true);
    List<Column> columns = new ArrayList<Column>();
    Column col = newTestColumn(pageChannel);
    col.setType(DataType.INT);
    columns.add(col);
    col = newTestColumn(pageChannel);
    col.setType(DataType.TEXT);
    columns.add(col);
    col = newTestColumn(pageChannel);
    col.setType(DataType.TEXT);
    columns.add(col);
    Table table = new Table(true, columns) {
        @Override
        public PageChannel getPageChannel() {
          return pageChannel;
        }
      };
    int colCount = 3;
    Object[] row = new Object[colCount];
    row[0] = new Short((short) 9);
    row[1] = "Tim";
    row[2] = "McCune";
    ByteBuffer buffer = table.createRow(row, format.MAX_ROW_SIZE);
    assertEquals((short) colCount, buffer.getShort());
    assertEquals((short) 9, buffer.getShort());
    assertEquals((byte) 'T', buffer.get());
    assertEquals((short) 22, buffer.getShort(22));
    assertEquals((short) 10, buffer.getShort(24));
    assertEquals((short) 4, buffer.getShort(26));
    assertEquals((short) 2, buffer.getShort(28));
    assertEquals((byte) 7, buffer.get(30));
  }

  private static Column newTestColumn(final PageChannel pageChannel) {
    return new Column(true) {
        @Override
        public PageChannel getPageChannel() {
          return pageChannel;
        }
        @Override
        public JetFormat getFormat() {
          return JetFormat.VERSION_4;
        }
      };
  }
  
}
