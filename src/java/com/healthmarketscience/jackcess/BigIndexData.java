/*
Copyright (c) 2008 Health Market Science, Inc.

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


/**
 * Implementation of an Access table index which supports large indexes.
 * @author James Ahlborn
 */
public class BigIndexData extends IndexData {

  /** Cache which manages the index pages */
  private final IndexPageCache _pageCache;
  
  public BigIndexData(Table table, int number, int uniqueEntryCount,
                      int uniqueEntryCountOffset) {
    super(table, number, uniqueEntryCount, uniqueEntryCountOffset);
    _pageCache = new IndexPageCache(this);
  }

  @Override
  protected void updateImpl() throws IOException {
    _pageCache.write();
  }

  @Override
  protected void readIndexEntries()
    throws IOException
  {
    _pageCache.setRootPageNumber(getRootPageNumber());
  }

  @Override
  protected DataPage findDataPage(Entry entry)
    throws IOException
  {
    return _pageCache.findCacheDataPage(entry);
  }

  @Override
  protected DataPage getDataPage(int pageNumber)
    throws IOException
  {
    return _pageCache.getCacheDataPage(pageNumber);
  }

  @Override
  public String toString() {
    return super.toString() + "\n" + _pageCache.toString();
  }

  /**
   * Used by unit tests to validate the internal status of the index.
   */
  void validate() throws IOException {
    _pageCache.validate();
  }
  
}
