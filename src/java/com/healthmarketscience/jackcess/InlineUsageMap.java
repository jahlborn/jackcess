/*
Copyright (c) 2005 Health Market Science, Inc.

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

/**
 * Usage map whose map is written inline in the same page.  This type of map
 * can contain a maximum of 512 pages, and is always used for free space maps.
 * It has a start page, which all page numbers in its map are calculated as
 * starting from.
 * @author Tim McCune
 */
public class InlineUsageMap extends UsageMap {
  
  /** Size in bytes of the map */
  private static final int MAP_SIZE = 64;
  
  /**
   * @param pageChannel Used to read in pages
   * @param dataBuffer Buffer that contains this map's declaration
   * @param pageNum Page number that this usage map is contained in
   * @param format Format of the database that contains this usage map
   * @param rowStart Offset at which the declaration starts in the buffer
   */
  public InlineUsageMap(PageChannel pageChannel, ByteBuffer dataBuffer,
      int pageNum, JetFormat format, short rowStart)
  throws IOException
  {
    super(pageChannel, dataBuffer, pageNum, format, rowStart);
    int startPage = dataBuffer.getInt(rowStart + 1);
    processMap(dataBuffer, 0, startPage);
  }
  
  //Javadoc copied from UsageMap
  protected void addOrRemovePageNumber(final int pageNumber, boolean add)
  throws IOException
  {
    int startPage = getStartPage();
    if (add && pageNumber < startPage) {
      throw new IOException("Can't add page number " + pageNumber +
          " because it is less than start page " + startPage);
    }
    int relativePageNumber = pageNumber - startPage;
    ByteBuffer buffer = getDataBuffer();
    if ((!add && !getPageNumbers().get(relativePageNumber)) ||
        (add && (relativePageNumber > MAP_SIZE * 8 - 1)))
    {
      //Increase the start page to the current page and clear out the map.
      startPage = pageNumber;
      setStartPage(startPage);
      buffer.position(getRowStart() + 1);
      buffer.putInt(startPage);
      getPageNumbers().clear();
      if (!add) {
        for (int j = 0; j < MAP_SIZE; j++) {
          buffer.put((byte) 0xff); //Fill bitmap with 1s
        }
        getPageNumbers().set(0, (MAP_SIZE * 8)); //Fill our list with page numbers
      }
      getPageChannel().writePage(buffer, getDataPageNumber());
      relativePageNumber = pageNumber - startPage;
    }
    updateMap(pageNumber, relativePageNumber, 1 << (relativePageNumber % 8), buffer, add);
    //Write the updated map back to disk
    getPageChannel().writePage(buffer, getDataPageNumber());
  }
  
}
