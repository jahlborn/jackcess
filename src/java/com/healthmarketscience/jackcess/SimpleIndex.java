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
import java.util.List;


/**
 * Simple implementation of an Access table index
 * @author Tim McCune
 */
public class SimpleIndex extends Index {

  /** data for the single index page.  if this data came from multiple pages,
      the index is read-only. */
  private SimpleDataPage _dataPage;
  
  public SimpleIndex(Table table, int uniqueEntryCount,
                     int uniqueEntryCountOffset)
  {
    super(table, uniqueEntryCount, uniqueEntryCountOffset);
  }

  @Override
  protected void updateImpl() throws IOException {
    writeDataPage(_dataPage);
  }

  @Override
  protected void readIndexEntries()
    throws IOException
  {
    // find first leaf page
    int nextPageNumber = getRootPageNumber();
    SimpleDataPage indexPage = null;
    while(true) {
      indexPage = new SimpleDataPage(nextPageNumber);
      readDataPage(indexPage);

      if(!indexPage.isLeaf()) {
        // FIXME we can't modify this index at this point in time
        setReadOnly();

        // found another node page
        if(!indexPage.getEntries().isEmpty()) {
          nextPageNumber = indexPage.getEntries().get(0).getSubPageNumber();
        } else {
          // try tail page
          nextPageNumber = indexPage.getChildTailPageNumber();
        }
        indexPage = null;
      } else {
        // found first leaf
        break;
      }
    }

    // save the first leaf page
    _dataPage = indexPage;
    nextPageNumber = indexPage.getNextPageNumber();
    _dataPage.setNextPageNumber(INVALID_INDEX_PAGE_NUMBER);
    indexPage = null;
    
    // read all leaf pages.
    while(nextPageNumber != INVALID_INDEX_PAGE_NUMBER) {
        
      // FIXME we can't modify this index at this point in time
      setReadOnly();
        
      // found another one
      indexPage = new SimpleDataPage(nextPageNumber);
      readDataPage(indexPage);

      // since we read all the entries in sort order, we can insert them
      // directly into the entries list
      _dataPage.getEntries().addAll(indexPage.getEntries());
      int totalSize = (_dataPage.getTotalEntrySize() +
                       indexPage.getTotalEntrySize());
      _dataPage.setTotalEntrySize(totalSize);
      nextPageNumber = indexPage.getNextPageNumber();
    }

    // check the entry order, just to be safe
    List<Entry> entries = _dataPage.getEntries();
    for(int i = 0; i < (entries.size() - 1); ++i) {
      Entry e1 = entries.get(i);
      Entry e2 = entries.get(i + 1);
      if(e1.compareTo(e2) > 0) {
        throw new IOException("Unexpected order in index entries, " +
                              e1 + " is greater than " + e2);
      }
    }
  }

  @Override
  protected DataPage findDataPage(Entry entry)
    throws IOException
  {
    return _dataPage;
  }

  @Override
  protected DataPage getDataPage(int pageNumber)
    throws IOException
  {
    throw new UnsupportedOperationException();
  }
  

  /**
   * Simple implementation of a DataPage
   */
  private static final class SimpleDataPage extends DataPage {
    private final int _pageNumber;
    private boolean _leaf;
    private int _nextPageNumber;
    private int _totalEntrySize;
    private int _childTailPageNumber;
    private List<Entry> _entries;

    private SimpleDataPage(int pageNumber) {
      _pageNumber = pageNumber;
    }
    
    @Override
    public int getPageNumber() {
      return _pageNumber;
    }
    
    @Override
    public boolean isLeaf() {
      return _leaf;
    }
    @Override
    public void setLeaf(boolean isLeaf) {
      _leaf = isLeaf;
    }

    @Override
    public int getPrevPageNumber() { return 0; }
    @Override
    public void setPrevPageNumber(int pageNumber) {
      // ignored
    }
    @Override
    public int getNextPageNumber() {
      return _nextPageNumber;
    }
    @Override
    public void setNextPageNumber(int pageNumber) {
      _nextPageNumber = pageNumber;
    }
    @Override
    public int getChildTailPageNumber() { 
      return _childTailPageNumber;
    }
    @Override
    public void setChildTailPageNumber(int pageNumber) {
      _childTailPageNumber = pageNumber;
    }
    
    @Override
    public int getTotalEntrySize() {
      return _totalEntrySize;
    }
    @Override
    public void setTotalEntrySize(int totalSize) {
      _totalEntrySize = totalSize;
    }
    @Override
    public byte[] getEntryPrefix() {
      return EMPTY_PREFIX;
    }
    @Override
    public void setEntryPrefix(byte[] entryPrefix) {
      // ignored
    }

    @Override
    public List<Entry> getEntries() {
      return _entries;
    }
    
    @Override
    public void setEntries(List<Entry> entries) {
      
      _entries = entries;
    }

    @Override
    public void addEntry(int idx, Entry entry) {
      _entries.add(idx, entry);
      _totalEntrySize += entry.size();
    }

    @Override
    public void removeEntry(int idx) {
      Entry oldEntry = _entries.remove(idx);
      _totalEntrySize -= oldEntry.size();
    }
    
  }
  
}
