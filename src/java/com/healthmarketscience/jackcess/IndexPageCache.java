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
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.ObjectUtils;

import static com.healthmarketscience.jackcess.Index.*;

/**
 * Manager of the index pages for a BigIndex.
 * @author James Ahlborn
 */
public class IndexPageCache
{
  /** the index whose pages this cache is managing */
  private final BigIndex _index;
  /** the root page for the index */
  private DataPageMain _rootPage;
  /** the currently loaded pages for this index, pageNumber -> page */
  private final Map<Integer, DataPageMain> _dataPages =
    new HashMap<Integer, DataPageMain>();
  /** the currently modified index pages */
  private final List<CacheDataPage> _modifiedPages =
    new ArrayList<CacheDataPage>();
  
  public IndexPageCache(BigIndex index) {
    _index = index;
  }

  public BigIndex getIndex() {
    return _index;
  }
  
  public JetFormat getFormat() {
    return getIndex().getFormat();
  }

  public PageChannel getPageChannel() {
    return getIndex().getPageChannel();
  }

  public void setRootPageNumber(int pageNumber) throws IOException {
    _rootPage = getDataPage(pageNumber);
  }
  
  public void write()
    throws IOException
  {
    preparePagesForWriting();
    writeDataPages();
  }

  private void preparePagesForWriting()
    throws IOException
  {
    // FIXME, writeme
  }

  private void writeDataPages()
    throws IOException
  {
    for(CacheDataPage cacheDataPage : _modifiedPages) {
      writeDataPage(cacheDataPage);
    }
    _modifiedPages.clear();
  }

  public CacheDataPage getCacheDataPage(Integer pageNumber)
    throws IOException
  {
    DataPageMain main = getDataPage(pageNumber);
    return((main != null) ? new CacheDataPage(main) : null);
  }
  
  private DataPageMain getDataPage(Integer pageNumber)
    throws IOException
  {
    DataPageMain dataPage = _dataPages.get(pageNumber);
    if((dataPage == null) && (pageNumber > INVALID_INDEX_PAGE_NUMBER)) {
      dataPage = readDataPage(pageNumber)._main;
      _dataPages.put(pageNumber, dataPage);
    }
    return dataPage;
  }

  /**
   * Writes the given index page to the file.
   */
  private void writeDataPage(CacheDataPage cacheDataPage)
    throws IOException
  {
    getIndex().writeDataPage(cacheDataPage);

    // lastly, mark the page as no longer modified
    cacheDataPage._extra._modified = false;    
  }
  
  /**
   * Reads the given index page from the file.
   */
  private CacheDataPage readDataPage(Integer pageNumber)
    throws IOException
  {
    DataPageMain dataPage = new DataPageMain(pageNumber);
    DataPageExtra extra = new DataPageExtra();
    CacheDataPage cacheDataPage = new CacheDataPage(dataPage, extra);
    getIndex().readDataPage(cacheDataPage);

    // associate the extra info with the main data page
    dataPage.setExtra(extra, true);
    
    return cacheDataPage;
  }  

  private void removeEntry(CacheDataPage cacheDataPage,
                           int entryIdx)
  {
    DataPageMain dpMain = cacheDataPage._main;
    DataPageExtra dpExtra = cacheDataPage._extra;

    setModified(cacheDataPage);
    
    boolean updateFirst = (entryIdx == 0);
    boolean updateLast = (entryIdx == (dpExtra._entries.size() - 1));

    Entry oldEntry = dpExtra._entries.remove(entryIdx);
    dpExtra._totalEntrySize -= oldEntry.size();

    // note, we don't need to futz with the _entryPrefix because a prefix is
    // always still valid on removal
    
    if(dpExtra._entries.isEmpty()) {
      // this page is dead
      if(dpMain.isRoot()) {
        // clear out this page
        dpMain._firstEntry = null;
        dpMain._lastEntry = null;
        dpExtra._entryPrefix = EMPTY_PREFIX;
        if(dpExtra._totalEntrySize != 0) {
          throw new IllegalStateException("Empty page but size is not 0?");
        }
      } else {
        // FIXME, remove from parent
        Entry oldParentEntry = oldEntry.asNodeEntry(dpMain._pageNumber);
        // FIXME, update next/prev/childTail links
        throw new UnsupportedOperationException();
      }
    } else {
      if(updateFirst) {
        dpMain._firstEntry = dpExtra.getFirstEntry();
        Entry oldParentEntry = oldEntry.asNodeEntry(dpMain._pageNumber);
        Entry newParentEntry =
          dpMain._firstEntry.asNodeEntry(dpMain._pageNumber);
        // FIXME, need to update parent
        throw new UnsupportedOperationException();
      }
      if(updateLast) {
        dpMain._lastEntry = dpExtra.getLastEntry();
      }
    }
    
  }

  private void addEntry(CacheDataPage cacheDataPage,
                        int entryIdx,
                        Entry newEntry)
    throws IOException
  {
    addOrReplaceEntry(cacheDataPage, entryIdx, newEntry, true);
  }
  
  private void replaceEntry(CacheDataPage cacheDataPage,
                            int entryIdx,
                            Entry newEntry)
    throws IOException
  {
    addOrReplaceEntry(cacheDataPage, entryIdx, newEntry, false);
  }
  
  private void addOrReplaceEntry(CacheDataPage cacheDataPage,
                                 int entryIdx,
                                 Entry newEntry,
                                 boolean isAdd)
    throws IOException
  {
    DataPageMain dpMain = cacheDataPage._main;
    DataPageExtra dpExtra = cacheDataPage._extra;

    validateEntryForPage(dpMain, newEntry);
    
    setModified(cacheDataPage);

    boolean updateFirst = false;
    boolean updateLast = false;

    if(isAdd) {
      updateFirst = (entryIdx == 0);
      updateLast = (entryIdx == dpExtra._entries.size());

      dpExtra._entries.add(entryIdx, newEntry);
      dpExtra._totalEntrySize += newEntry.size();
    } else {
      updateFirst = (entryIdx == 0);
      updateLast = (entryIdx == (dpExtra._entries.size() - 1));

      Entry oldEntry = dpExtra._entries.set(entryIdx, newEntry);
      dpExtra._totalEntrySize += newEntry.size() - oldEntry.size();
    }
    
    if(updateFirst) {
      Entry oldFirstEntry = dpMain._firstEntry;
      dpMain._firstEntry = newEntry;
      if(!dpMain.isRoot()) {
        // FIXME, handle null oldFirstEntry
        Entry oldParentEntry = oldFirstEntry.asNodeEntry(
            dpMain._pageNumber);
        Entry newParentEntry =
          dpMain._firstEntry.asNodeEntry(dpMain._pageNumber);
        // FIXME, need to update parent
        throw new UnsupportedOperationException();
      }
    }
    if(updateLast) {
      dpMain._lastEntry = newEntry;
    }
    if(updateFirst || updateLast) {
      // update the prefix
      dpExtra._entryPrefix = findNewPrefix(dpExtra._entryPrefix, newEntry);
    }
  }

  private void validateEntryForPage(DataPageMain dataPage, Entry entry) {
    if(dataPage._leaf != entry.isLeafEntry()) {
      throw new IllegalStateException(
          "Trying to update page with wrong entry type; pageLeaf " +
          dataPage._leaf + ", entryLeaf " + entry.isLeafEntry());
    }
  }
  
  public CacheDataPage findCacheDataPage(Entry e)
    throws IOException
  {
    DataPageMain curPage = _rootPage;
    while(true) {
      int pageCmp = curPage.compareToPage(e);
      if(pageCmp < 0) {

        // find first leaf
        while(!curPage._leaf) {
          curPage = getDataPage(curPage._firstEntry.getSubPageNumber());
        }
        return new CacheDataPage(curPage);
        
      } else if(pageCmp > 0) {
        
        if(!curPage._leaf) {
          // we need to handle any "childTail" pages, so we aren't done yet
          DataPageMain childTailPage = curPage.getChildTailPage();
          if((childTailPage != null) &&
             (childTailPage.compareToPage(e) >= 0)) {
            curPage = childTailPage;
          } else {
            curPage = getDataPage(curPage._lastEntry.getSubPageNumber());
          }
        } else {
          return new CacheDataPage(curPage);
        }
        
      } else if(pageCmp == 0) {

        DataPageExtra extra = curPage.getExtra();
        if(curPage._leaf) {
          return new CacheDataPage(curPage, extra);
        }

        // need to descend
        int idx = extra.findEntry(e);
        if(idx < 0) {
          idx = missingIndexToInsertionPoint(idx);
          if((idx == 0) || (idx == extra._entries.size())) {
            // this should never happen, cause we already checked first/last
            // entries in compareToPage
            throw new IllegalStateException("first/last entries incorrect");
          }
          // the insertion point index is actually the entry after the one we
          // want, so move back one element
          --idx;
        }

        Entry nodeEntry = extra._entries.get(idx);
        curPage = getDataPage(nodeEntry.getSubPageNumber());
        
      }
    }
  }

  private void setModified(CacheDataPage cacheDataPage)
  {
    if(!cacheDataPage._extra._modified) {
      _modifiedPages.add(cacheDataPage);
      cacheDataPage._extra._modified = true;
    }
  }

  private static byte[] findNewPrefix(byte[] curPrefix, Entry newEntry)
    throws IOException
  {
    byte[] newEntryBytes = newEntry.getEntryBytes();
    if(curPrefix.length > newEntryBytes.length) {
      // the entry bytes may include the page number.  need to encode the
      // entire entry
      newEntryBytes = new byte[newEntry.size()];
      newEntry.write(ByteBuffer.wrap(newEntryBytes), EMPTY_PREFIX);
    }

    return findCommonPrefix(curPrefix, newEntryBytes);
  }

  private static byte[] findCommonPrefix(Entry e1, Entry e2)
  {
    return findCommonPrefix(e1.getEntryBytes(), e2.getEntryBytes());
  }
  
  private static byte[] findCommonPrefix(byte[] b1, byte[] b2)
  {  
    int maxLen = b1.length;
    byte[] prefix = b1;
    if(b1.length > b2.length) {
      maxLen = b2.length;
      prefix = b2;
    }
    
    int len = 0;
    while((len < maxLen) && (b1[len] == b2[len])) {
      ++len;
    }
    
    if(len < prefix.length) {
      if(len == 0) {
        return EMPTY_PREFIX;
      }
      
      // need new prefix
      byte[] tmpPrefix = new byte[len];
      System.arraycopy(prefix, 0, tmpPrefix, 0, len);
      prefix = tmpPrefix;
    }

    return prefix;
  }
  

  private class DataPageMain implements Comparable<DataPageMain>
  {
    public final int _pageNumber;
    public Integer _prevPageNumber;
    public Integer _nextPageNumber;
    public Integer _childTailPageNumber;
    public Integer _parentPageNumber;
    public Entry _firstEntry;
    public Entry _lastEntry;
    public boolean _leaf;
    public boolean _tail;
    private Reference<DataPageExtra> _extra;

    private DataPageMain(int pageNumber) {
      _pageNumber = pageNumber;
    }

    public IndexPageCache getCache() {
      return IndexPageCache.this;
    }
    
    public boolean isRoot() {
      return(this == _rootPage);
    }
    
    public boolean isTail()
      throws IOException
    {
      resolveParent();
      return _tail;
    }
    
    public DataPageMain getParentPage()
      throws IOException
    {
      resolveParent();
      return IndexPageCache.this.getDataPage(_parentPageNumber);
    }
    
    public DataPageMain getPrevPage()
      throws IOException
    {
      return IndexPageCache.this.getDataPage(_prevPageNumber);
    }
    
    public DataPageMain getNextPage()
      throws IOException
    {
      return IndexPageCache.this.getDataPage(_nextPageNumber);
    }
    
    public DataPageMain getChildTailPage()
      throws IOException
    {
      return IndexPageCache.this.getDataPage(_childTailPageNumber);
    }
    
    public DataPageExtra getExtra()
      throws IOException
    {
      DataPageExtra extra = _extra.get();
      if(extra == null) {
        extra = readDataPage(_pageNumber)._extra;
        setExtra(extra, false);
      }
      
      return extra;
    }

    public void setExtra(DataPageExtra extra, boolean isNew)
      throws IOException
    {

      if(isNew) {
        // save first/last entries
        _firstEntry = extra.getFirstEntry();
        _lastEntry = extra.getLastEntry();
      } else {
        // check first/last entries, to be safe
        if(!ObjectUtils.equals(_firstEntry, extra.getFirstEntry()) ||
           !ObjectUtils.equals(_lastEntry, extra.getLastEntry())) {
          throw new IOException("Unexpected first or last entry found" +
                                "; had " + _firstEntry + ", " + _lastEntry +
                                "; found " + extra.getFirstEntry() + ", " +
                                extra.getLastEntry());
        }
      }
      
      _extra = new SoftReference<DataPageExtra>(extra);
    }

    public int compareToPage(Entry e)
    {
      return((_firstEntry == null) ? 0 :
             ((e.compareTo(_firstEntry) < 0) ? -1 :
              ((e.compareTo(_lastEntry) > 0) ? 1 : 0)));
    }

    public int compareTo(DataPageMain other)
    {
      // note, only leaf pages can be meaningfully compared
      if(!_leaf || !other._leaf) {
        throw new IllegalArgumentException("Only leaf pages can be compared");
      }
      if(this == other) {
        return 0;
      }
      // note, if there is more than one leaf page, neither should have null
      // entries
      return _firstEntry.compareTo(other._firstEntry);
    }

    private void resolveParent() {
      if((_parentPageNumber == null) && !isRoot()) {
        // FIXME, writeme
        // need to determine _parentPageNumber and _tail
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public String toString() {
      return "DPMain[" + _pageNumber + "] " + _leaf + ", " + _firstEntry +
        ", " + _lastEntry + ", " + _extra.get();
    }
  }

  private static class DataPageExtra
  {
    /** sorted collection of index entries.  this is kept in a list instead of
        a SortedSet because the SortedSet has lame traversal utilities */
    public List<Entry> _entries;
    public byte[] _entryPrefix;
    public int _totalEntrySize;
    public boolean _modified;

    private DataPageExtra()
    {
    }

    public int findEntry(Entry e) {
      return Collections.binarySearch(_entries, e);
    }

    public Entry getFirstEntry() {
      return (!_entries.isEmpty() ? _entries.get(0) : null);
    }

    public Entry getLastEntry() {
      return (!_entries.isEmpty() ? _entries.get(_entries.size() - 1) : null);
    }

    @Override
    public String toString() {
      return "DPExtra: " + _entries;
    }    
  }

  public static final class CacheDataPage
    extends Index.DataPage
  {
    public final DataPageMain _main;
    public final DataPageExtra _extra;

    private CacheDataPage(DataPageMain dataPage) throws IOException {
      this(dataPage, dataPage.getExtra());
    }
    
    private CacheDataPage(DataPageMain dataPage, DataPageExtra extra) {
      _main = dataPage;
      _extra = extra;
    }

    @Override
    public int getPageNumber() {
      return _main._pageNumber;
    }
    
    @Override
    public boolean isLeaf() {
      return _main._leaf;
    }

    @Override
    public void setLeaf(boolean isLeaf) {
      _main._leaf = isLeaf;
    }


    @Override
    public int getPrevPageNumber() {
      return _main._prevPageNumber;
    }

    @Override
    public void setPrevPageNumber(int pageNumber) {
      _main._prevPageNumber = pageNumber;
    }

    @Override
    public int getNextPageNumber() {
      return _main._nextPageNumber;
    }

    @Override
    public void setNextPageNumber(int pageNumber) {
      _main._nextPageNumber = pageNumber;
    }

    @Override
    public int getChildTailPageNumber() {
      return _main._childTailPageNumber;
    }

    @Override
    public void setChildTailPageNumber(int pageNumber) {
      _main._childTailPageNumber = pageNumber;
    }

    
    @Override
    public int getTotalEntrySize() {
      return _extra._totalEntrySize;
    }

    @Override
    public void setTotalEntrySize(int totalSize) {
      _extra._totalEntrySize = totalSize;
    }

    @Override
    public byte[] getEntryPrefix() {
      return _extra._entryPrefix;
    }

    @Override
    public void setEntryPrefix(byte[] entryPrefix) {
      _extra._entryPrefix = entryPrefix;
    }


    @Override
    public List<Entry> getEntries() {
      return _extra._entries;
    }

    @Override
    public void setEntries(List<Entry> entries) {
      _extra._entries = entries;
    }

    @Override
    public void addEntry(int idx, Entry entry) throws IOException {
      _main.getCache().addEntry(this, idx, entry);
    }
    
    @Override
    public void removeEntry(int idx) throws IOException {
      _main.getCache().removeEntry(this, idx);
    }
    
  }

}
