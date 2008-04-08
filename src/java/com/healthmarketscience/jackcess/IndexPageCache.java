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
import java.util.Iterator;
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
  private enum UpdateType {
    ADD, REMOVE, REPLACE;
  }

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
    // root page has no parent
    _rootPage.setParentPage(INVALID_INDEX_PAGE_NUMBER, false);
  }
  
  public void write()
    throws IOException
  {
    // first discard any empty pages
    handleEmptyPages();
    // next, handle any necessary page splitting
    preparePagesForWriting();
    // finally, write all the modified pages (which are not being deleted)
    writeDataPages();
  }

  private void handleEmptyPages() throws IOException
  {
    for(Iterator<CacheDataPage> iter = _modifiedPages.iterator();
        iter.hasNext(); ) {
      CacheDataPage cacheDataPage = iter.next();
      if(cacheDataPage.isEmpty()) {
        if(!cacheDataPage._main.isRoot()) {
          deleteDataPage(cacheDataPage);
        } else {
          writeDataPage(cacheDataPage);
        }
        iter.remove();
      }
    }
  }
  
  private void preparePagesForWriting() throws IOException
  {
    boolean splitPages = false;
    int maxPageEntrySize = getIndex().getMaxPageEntrySize();
    do {
      splitPages = false;

      for(CacheDataPage cacheDataPage : _modifiedPages) {
        if(cacheDataPage.getCompressedEntrySize() > maxPageEntrySize) {
          // need to split this page
          splitPages = true;
          splitDataPage(cacheDataPage);
        }
      }
      
    } while(splitPages);
  }

  private void writeDataPages() throws IOException
  {
    for(CacheDataPage cacheDataPage : _modifiedPages) {
      if(cacheDataPage.isEmpty()) {
        throw new IllegalStateException("Unexpected empty page " +
                                        cacheDataPage);
      }
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

  private DataPageMain getChildDataPage(Integer childPageNumber,
                                        DataPageMain parent,
                                        boolean isTail)
    throws IOException
  {
    DataPageMain child = getDataPage(childPageNumber);
    if(child != null) {
      // set the parent info for this child (if necessary)
      child.setParentPage(parent._pageNumber, isTail);
    }
    return child;
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
   * Deletes the given index page from the file (clears the page).
   */
  private void deleteDataPage(CacheDataPage cacheDataPage)
    throws IOException
  {
    // FIXME, clear/deallocate index page?

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

  private void removeEntry(CacheDataPage cacheDataPage, int entryIdx)
    throws IOException
  {
    updateEntry(cacheDataPage, entryIdx, null, UpdateType.REMOVE);
  }

  private void addEntry(CacheDataPage cacheDataPage,
                        int entryIdx,
                        Entry newEntry)
    throws IOException
  {
    updateEntry(cacheDataPage, entryIdx, newEntry, UpdateType.ADD);
  }
  
  private void replaceEntry(CacheDataPage cacheDataPage,
                            int entryIdx,
                            Entry newEntry)
    throws IOException
  {
    updateEntry(cacheDataPage, entryIdx, newEntry, UpdateType.REPLACE);
  }

  private void updateEntry(CacheDataPage cacheDataPage,
                           int entryIdx,
                           Entry newEntry,
                           UpdateType upType)
    throws IOException
  {
    DataPageMain dpMain = cacheDataPage._main;
    DataPageExtra dpExtra = cacheDataPage._extra;

    if(newEntry != null) {
      validateEntryForPage(dpMain, newEntry);
    }
    
    setModified(cacheDataPage);

    boolean updateFirst = (entryIdx == 0);
    boolean updateLast = false;
    boolean mayUpdatePrefix = true;

    switch(upType) {
    case ADD: {
      updateLast = (entryIdx == dpExtra._entries.size());

      dpExtra._entries.add(entryIdx, newEntry);
      dpExtra._totalEntrySize += newEntry.size();
      break;
    }
    case REPLACE: {
      updateLast = (entryIdx == (dpExtra._entries.size() - 1));

      Entry oldEntry = dpExtra._entries.set(entryIdx, newEntry);
      dpExtra._totalEntrySize += newEntry.size() - oldEntry.size();
      break;
    }
    case REMOVE: {
      updateLast = (entryIdx == (dpExtra._entries.size() - 1));

      Entry oldEntry = dpExtra._entries.remove(entryIdx);
      dpExtra._totalEntrySize -= oldEntry.size();
      // note, we don't need to futz with the _entryPrefix on removal because
      // the prefix is always still valid after removal
      mayUpdatePrefix = false;
      break;
    }
    default:
      throw new RuntimeException("unknown update type " + upType);
    }
    
    if(cacheDataPage.isEmpty()) {
      // this page is dead
      removeDataPage(cacheDataPage);
    } else {
      if(updateFirst) {
        updateFirstEntry(cacheDataPage);
      }
      if(updateLast) {
        dpMain._lastEntry = dpExtra.getLastEntry();
      }
      if(mayUpdatePrefix && (updateFirst || updateLast)) {
        // update the prefix
        dpExtra._entryPrefix = findNewPrefix(dpExtra._entryPrefix, newEntry);
      }
    }
  }

  private void removeDataPage(CacheDataPage cacheDataPage)
    throws IOException
  {
    DataPageMain dpMain = cacheDataPage._main;
    DataPageExtra dpExtra = cacheDataPage._extra;

    if(dpMain.isRoot()) {
      // clear out this page (we don't actually remove it)
      dpMain._firstEntry = null;
      dpMain._lastEntry = null;
      dpExtra._entryPrefix = EMPTY_PREFIX;
      if(dpExtra._totalEntrySize != 0) {
        throw new IllegalStateException("Empty page but size is not 0? " +
                                        cacheDataPage);
      }
      return;
    }

    // remove this page from it's parent page
    removeParentEntry(cacheDataPage);

    // remove this page from any next/prev pages
    removeFromPeers(cacheDataPage);
  }

  private void removeFromPeers(CacheDataPage cacheDataPage)
    throws IOException
  {
    DataPageMain dpMain = cacheDataPage._main;

    Integer prevPageNumber = dpMain._prevPageNumber;
    Integer nextPageNumber = dpMain._nextPageNumber;
    
    DataPageMain prevMain = dpMain.getPrevPage();
    if(prevMain != null) {
      setModified(new CacheDataPage(prevMain));
      prevMain._nextPageNumber = nextPageNumber;
    }

    DataPageMain nextMain = dpMain.getNextPage();
    if(nextMain != null) {
      setModified(new CacheDataPage(nextMain));
      nextMain._prevPageNumber = prevPageNumber;
    }
  }

  private void updateFirstEntry(CacheDataPage cacheDataPage)
    throws IOException
  {
    DataPageMain dpMain = cacheDataPage._main;
    if(!dpMain.isRoot()) {
      DataPageExtra dpExtra = cacheDataPage._extra;

      Entry oldEntry = dpMain._firstEntry;
      dpMain._firstEntry = dpExtra.getFirstEntry();
      DataPageMain parentMain = dpMain.getParentPage();
      replaceParentEntry(new CacheDataPage(parentMain),
                         cacheDataPage,
                         oldEntry, dpMain._firstEntry);
    }
  }

  private void removeParentEntry(CacheDataPage childDataPage)
    throws IOException
  {
    DataPageMain childMain = childDataPage._main;
    updateParentEntry(new CacheDataPage(childMain.getParentPage()),
                      childDataPage, childMain._firstEntry,
                      null, UpdateType.REMOVE);
  }
  
  private void addParentEntry(CacheDataPage parentDataPage,
                              CacheDataPage childDataPage,
                              Entry newEntry)
    throws IOException
  {
    updateParentEntry(parentDataPage, childDataPage, null, newEntry,
                      UpdateType.ADD);
  }
  
  private void replaceParentEntry(CacheDataPage parentDataPage,
                                 CacheDataPage childDataPage,
                                 Entry oldEntry, Entry newEntry)
    throws IOException
  {
    updateParentEntry(parentDataPage, childDataPage, oldEntry, newEntry,
                      UpdateType.REPLACE);
  }
  
  private void updateParentEntry(CacheDataPage parentDataPage,
                                 CacheDataPage childDataPage,
                                 Entry oldEntry, Entry newEntry,
                                 UpdateType upType)
    throws IOException
  {
    DataPageMain childMain = childDataPage._main;
    DataPageMain parentMain = parentDataPage._main;
    DataPageExtra parentExtra = parentDataPage._extra;

    if(childMain.isTail()) {
      int newChildTailPageNumber =
        ((upType == UpdateType.REMOVE) ?
         INVALID_INDEX_PAGE_NUMBER :
         childMain._pageNumber);
      if((int)parentMain._childTailPageNumber != newChildTailPageNumber) {
        setModified(parentDataPage);
        parentMain._childTailPageNumber = newChildTailPageNumber;
      }

      // nothing more to do
      return;
    }
    
    if(oldEntry != null) {
      oldEntry = oldEntry.asNodeEntry(childMain._pageNumber);
    }
    if(newEntry != null) {
      newEntry = newEntry.asNodeEntry(childMain._pageNumber);
    }

    boolean expectFound = true;
    int idx = 0;
    
    switch(upType) {
    case ADD:
      expectFound = false;
      idx = parentExtra.findEntry(newEntry);
      break;

    case REPLACE:
    case REMOVE:
      idx = parentExtra.findEntry(oldEntry);
      break;
    
    default:
      throw new RuntimeException("unknown update type " + upType);
    }
        
    if(idx < 0) {
      if(expectFound) {
        throw new IllegalStateException(
            "Could not find child entry in parent; childEntry " + oldEntry +
            "; parent " + parentDataPage);
      }
      idx = missingIndexToInsertionPoint(idx);
    } else {
      if(!expectFound) {
        throw new IllegalStateException(
            "Unexpectedly found child entry in parent; childEntry " +
            newEntry + "; parent " + parentDataPage);
      }
    }
    updateEntry(parentDataPage, idx, newEntry, upType);
  }
  
  
  private void validateEntryForPage(DataPageMain dataPage, Entry entry) {
    if(dataPage._leaf != entry.isLeafEntry()) {
      throw new IllegalStateException(
          "Trying to update page with wrong entry type; pageLeaf " +
          dataPage._leaf + ", entryLeaf " + entry.isLeafEntry());
    }
  }

  private void splitDataPage(CacheDataPage cacheDataPage)
    throws IOException
  {
    // FIXME, writeme
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
          curPage = curPage.getChildPage(curPage._firstEntry);
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
            curPage = curPage.getChildPage(curPage._lastEntry);
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
        curPage = curPage.getChildPage(nodeEntry);
        
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
    
    public boolean isTail() throws IOException
    {
      resolveParent();
      return _tail;
    }
    
    public DataPageMain getParentPage() throws IOException
    {
      resolveParent();
      return IndexPageCache.this.getDataPage(_parentPageNumber);
    }

    public void setParentPage(Integer parentPageNumber, boolean isTail) {
      if(_parentPageNumber == null) {
        _parentPageNumber = parentPageNumber;
        _tail = isTail;
      }
    }
    
    public DataPageMain getPrevPage() throws IOException
    {
      return IndexPageCache.this.getDataPage(_prevPageNumber);
    }
    
    public DataPageMain getNextPage() throws IOException
    {
      return IndexPageCache.this.getDataPage(_nextPageNumber);
    }
    
    public DataPageMain getChildPage(Entry e) throws IOException
    {
      return IndexPageCache.this.getChildDataPage(
          e.getSubPageNumber(), this, false);
    }
    
    public DataPageMain getChildTailPage() throws IOException
    {
      return IndexPageCache.this.getChildDataPage(
          _childTailPageNumber, this, true);
    }
    
    public DataPageExtra getExtra() throws IOException
    {
      DataPageExtra extra = _extra.get();
      if(extra == null) {
        extra = readDataPage(_pageNumber)._extra;
        setExtra(extra, false);
      }
      
      return extra;
    }

    public void setExtra(DataPageExtra extra, boolean isNew) throws IOException
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

    private void resolveParent() throws IOException {
      if(_parentPageNumber == null) {
        // the act of searching for the first entry should resolve any parent
        // pages along the path
        findCacheDataPage(_firstEntry);
        if(_parentPageNumber == null) {
          throw new IllegalStateException("Parent was not resolved");
        }
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
