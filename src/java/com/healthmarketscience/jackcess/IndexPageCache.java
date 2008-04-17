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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;


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
    _rootPage.initParentPage(INVALID_INDEX_PAGE_NUMBER, false);
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

        // we might be adding to this list while iterating, so we can't use an
        // iteratoor
      for(int i = 0; i < _modifiedPages.size(); ++i) {

        CacheDataPage cacheDataPage = _modifiedPages.get(i);

        if(!cacheDataPage.isLeaf()) {
          // see if we need to update any child tail status
          DataPageMain dpMain = cacheDataPage._main;
          int size = cacheDataPage._extra._entryView.size();
          if(dpMain.hasChildTail()) {
            if(size == 1) {
              demoteTail(cacheDataPage);
            } 
          } else {
            if(size > 1) {
              promoteTail(cacheDataPage);
            }
          }
        }
        
        // look for pages with more entries than can fit on a page
        if(cacheDataPage.getTotalEntrySize() > maxPageEntrySize) {

          // make sure the prefix is up-to-date (this may have gotten
          // discarded by one of the update entry methods)
          cacheDataPage._extra.updateEntryPrefix();

          // now, see if the page will fit when compressed
          if(cacheDataPage.getCompressedEntrySize() > maxPageEntrySize) {
            // need to split this page
            splitPages = true;
            splitDataPage(cacheDataPage);
          }
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
      child.initParentPage(parent._pageNumber, isTail);
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
    // free this database page
    getPageChannel().deallocatePage(cacheDataPage._main._pageNumber);

    // discard from our cache
    _dataPages.remove(cacheDataPage._main._pageNumber);
    
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
    dataPage.setExtra(extra);
    
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

    Entry oldLastEntry = dpExtra._entryView.getLast();
    Entry oldEntry = null;
    int entrySizeDiff = 0;

    switch(upType) {
    case ADD:
      dpExtra._entryView.add(entryIdx, newEntry);
      entrySizeDiff += newEntry.size();
      break;

    case REPLACE:
      oldEntry = dpExtra._entryView.set(entryIdx, newEntry);
      entrySizeDiff += newEntry.size() - oldEntry.size();
      break;

    case REMOVE: {
      oldEntry = dpExtra._entryView.remove(entryIdx);
      entrySizeDiff -= oldEntry.size();
      break;
    }
    default:
      throw new RuntimeException("unknown update type " + upType);
    }

    boolean updateLast = (oldLastEntry != dpExtra._entryView.getLast());
    
    // child tail entry updates do not modify the page
    if(!updateLast || !dpMain.hasChildTail()) {
      dpExtra._totalEntrySize += entrySizeDiff;
      setModified(cacheDataPage);

      // for now, just clear the prefix, we'll fix it later
      dpExtra._entryPrefix = EMPTY_PREFIX;
    }

    if(dpExtra._entryView.isEmpty()) {
      // this page is dead
      removeDataPage(cacheDataPage, oldLastEntry);
      return;
    }

    // determine if we need to update our parent page 
    if(!updateLast || dpMain.isRoot()) {
      // no parent
      return;
    }

    // the update to the last entry needs to be propagated to our parent
    replaceParentEntry(new CacheDataPage(dpMain.getParentPage()),
                       cacheDataPage, oldLastEntry);
  }

  private void removeDataPage(CacheDataPage cacheDataPage,
                              Entry oldLastEntry)
    throws IOException
  {
    DataPageMain dpMain = cacheDataPage._main;
    DataPageExtra dpExtra = cacheDataPage._extra;

    if(dpMain.hasChildTail()) {
      throw new IllegalStateException("Still has child tail?");
    }

    if(dpExtra._totalEntrySize != 0) {
      throw new IllegalStateException("Empty page but size is not 0? " +
                                      dpExtra._totalEntrySize + ", " +
                                      cacheDataPage);
    }
    
    if(dpMain.isRoot()) {
      // clear out this page (we don't actually remove it)
      dpExtra._entryPrefix = EMPTY_PREFIX;
      // when the root page becomes empty, it becomes a leaf page again
      dpMain._leaf = true;
      return;
    }

    // remove this page from its parent page
    updateParentEntry(new CacheDataPage(dpMain.getParentPage()),
                      cacheDataPage, oldLastEntry, null, UpdateType.REMOVE);

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

  private void removeParentEntry(CacheDataPage childDataPage)
    throws IOException
  {
    DataPageMain childMain = childDataPage._main;
    DataPageExtra childExtra = childDataPage._extra;
    updateParentEntry(new CacheDataPage(childMain.getParentPage()),
                      childDataPage, childExtra._entryView.getLast(),
                      null, UpdateType.REMOVE);
  }
  
  private void addParentEntry(CacheDataPage parentDataPage,
                              CacheDataPage childDataPage)
    throws IOException
  {
    DataPageExtra childExtra = childDataPage._extra;
    updateParentEntry(parentDataPage, childDataPage, null,
                      childExtra._entryView.getLast(), UpdateType.ADD);
  }
  
  private void replaceParentEntry(CacheDataPage parentDataPage,
                                  CacheDataPage childDataPage,
                                  Entry oldEntry)
    throws IOException
  {
    DataPageExtra childExtra = childDataPage._extra;
    updateParentEntry(parentDataPage, childDataPage, oldEntry,
                      childExtra._entryView.getLast(), UpdateType.REPLACE);
  }
  
  private void updateParentEntry(CacheDataPage parentDataPage,
                                 CacheDataPage childDataPage,
                                 Entry oldEntry, Entry newEntry,
                                 UpdateType upType)
    throws IOException
  {
    DataPageMain childMain = childDataPage._main;
    DataPageExtra parentExtra = parentDataPage._extra;

    if(childMain.isTail() && (upType != UpdateType.REMOVE)) {
      // for add or replace, update the child tail info before updating the
      // parent entries
      updateParentTail(parentDataPage, childDataPage, upType);
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
      idx = parentExtra._entryView.find(newEntry);
      break;

    case REPLACE:
    case REMOVE:
      idx = parentExtra._entryView.find(oldEntry);
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
      if(childMain.isTail() && (upType == UpdateType.ADD)) {
        // we add a tail entry by using the index (size + 1)
        ++idx;
      }
    } else {
      if(!expectFound) {
        throw new IllegalStateException(
            "Unexpectedly found child entry in parent; childEntry " +
            newEntry + "; parent " + parentDataPage);
      }
    }
    updateEntry(parentDataPage, idx, newEntry, upType);

    if(childMain.isTail() && (upType == UpdateType.REMOVE)) {
      // for remove, update the child tail info after updating the parent
      // entries
      updateParentTail(parentDataPage, childDataPage, upType);
    }
  }

  private void updateParentTail(CacheDataPage parentDataPage,
                                CacheDataPage childDataPage,
                                UpdateType upType)
    throws IOException
  {
    DataPageMain childMain = childDataPage._main;
    DataPageMain parentMain = parentDataPage._main;

    int newChildTailPageNumber =
      ((upType == UpdateType.REMOVE) ?
       INVALID_INDEX_PAGE_NUMBER :
       childMain._pageNumber);
    if(!parentMain.isChildTailPageNumber(newChildTailPageNumber)) {
      setModified(parentDataPage);
      parentMain._childTailPageNumber = newChildTailPageNumber;
    }
  }
  
  private void validateEntryForPage(DataPageMain dpMain, Entry entry) {
    if(dpMain._leaf != entry.isLeafEntry()) {
      throw new IllegalStateException(
          "Trying to update page with wrong entry type; pageLeaf " +
          dpMain._leaf + ", entryLeaf " + entry.isLeafEntry());
    }
  }

  private void splitDataPage(CacheDataPage origDataPage)
    throws IOException
  {
    DataPageMain origMain = origDataPage._main;
    DataPageExtra origExtra = origDataPage._extra;

    int numEntries = origExtra._entries.size();
    if(numEntries < 2) {
      throw new IllegalStateException(
          "Cannot split page with less than 2 entries " + origDataPage);
    }
    
    if(origMain.isRoot()) {
      // we can't split the root page directly, so we need to put another page
      // between the root page and its sub-pages, and then split that page.
      CacheDataPage newDataPage = nestRootDataPage(origDataPage);

      // now, split this new page instead
      origDataPage = newDataPage;
      origMain = newDataPage._main;
      origExtra = newDataPage._extra;
    }

    // note, there are many, many ways this could be improved/tweaked.  for
    // now, we just want it to be functional...
    // so, we will naively move half the entries from one page to a new page.

    DataPageMain parentMain = origMain.getParentPage();
    CacheDataPage newDataPage = allocateNewCacheDataPage(
        parentMain._pageNumber, origMain._leaf);
    DataPageMain newMain = newDataPage._main;
    DataPageExtra newExtra = newDataPage._extra;
    
    List<Entry> headEntries =
      origExtra._entries.subList(0, ((numEntries + 1) / 2));

    // move first half of the entries from old page to new page
    for(Entry headEntry : headEntries) {
      newExtra._totalEntrySize += headEntry.size();
      newExtra._entries.add(headEntry);
    }
    newExtra.setEntryView(newMain);

    // remove the moved entries from the old page
    headEntries.clear();
    origExtra._entryPrefix = EMPTY_PREFIX;
    origExtra._totalEntrySize -= newExtra._totalEntrySize;

    // insert this new page between the old page and any previous page
    addToPeersBefore(newDataPage, origDataPage);

    if(!newMain._leaf) {
      // reparent the children pages of the new page
      reparentChildren(newDataPage);

      // if the children of this page are also node pages, then the next/prev
      // links should not cross parent boundaries (the leaf pages are linked
      // from beginning to end, but child node pages are only linked within
      // the same parent)
      DataPageMain childMain = newMain.getChildPage(
          newExtra._entryView.getLast());
      if(!childMain._leaf) {
        separateFromNextPeer(new CacheDataPage(childMain));
      }
    }

    // lastly, we need to add the new page to the parent page's entries
    CacheDataPage parentDataPage = new CacheDataPage(parentMain);    
    addParentEntry(parentDataPage, newDataPage);
  }

  private CacheDataPage nestRootDataPage(CacheDataPage rootDataPage)
    throws IOException
  {
    DataPageMain rootMain = rootDataPage._main;
    DataPageExtra rootExtra = rootDataPage._extra;

    if(!rootMain.isRoot()) {
      throw new IllegalArgumentException("should be called with root, duh");
    }
    
    CacheDataPage newDataPage =
      allocateNewCacheDataPage(rootMain._pageNumber, rootMain._leaf);
    DataPageMain newMain = newDataPage._main;
    DataPageExtra newExtra = newDataPage._extra;

    // move entries to new page
    newMain._childTailPageNumber = rootMain._childTailPageNumber;
    newExtra._entries = rootExtra._entries;
    newExtra._entryPrefix = rootExtra._entryPrefix;
    newExtra._totalEntrySize = rootExtra._totalEntrySize;
    newExtra.setEntryView(newMain);

    if(!newMain._leaf) {
      // we need to re-parent all the child pages
      reparentChildren(newDataPage);
    }
      
    // clear the root page
    rootMain._leaf = false;
    rootMain._childTailPageNumber = INVALID_INDEX_PAGE_NUMBER;
    rootExtra._entries = new ArrayList<Entry>();
    rootExtra._entryPrefix = EMPTY_PREFIX;
    rootExtra._totalEntrySize = 0;
    rootExtra.setEntryView(rootMain);

    // add the new page as the first child of the root page
    addParentEntry(rootDataPage, newDataPage);

    return newDataPage;
  }
  
  private CacheDataPage allocateNewCacheDataPage(Integer parentPageNumber,
                                                 boolean isLeaf)
    throws IOException
  {
    DataPageMain dpMain = new DataPageMain(getPageChannel().allocateNewPage());
    DataPageExtra dpExtra = new DataPageExtra();
    dpMain.initParentPage(parentPageNumber, false);
    dpMain._leaf = isLeaf;
    dpMain._prevPageNumber = INVALID_INDEX_PAGE_NUMBER;
    dpMain._nextPageNumber = INVALID_INDEX_PAGE_NUMBER;
    dpMain._childTailPageNumber = INVALID_INDEX_PAGE_NUMBER;
    dpExtra._entries = new ArrayList<Entry>();
    dpExtra._entryPrefix = EMPTY_PREFIX;
    dpMain.setExtra(dpExtra);

    // add to our page cache
    _dataPages.put(dpMain._pageNumber, dpMain);

    // needs to be written out
    CacheDataPage cacheDataPage = new CacheDataPage(dpMain, dpExtra);
    setModified(cacheDataPage);

    return cacheDataPage;
  }

  private void addToPeersBefore(CacheDataPage newDataPage,
                                CacheDataPage origDataPage)
    throws IOException
  {
    DataPageMain origMain = origDataPage._main;
    DataPageMain newMain = newDataPage._main;

    DataPageMain prevMain = origMain.getPrevPage();

    newMain._nextPageNumber = origMain._pageNumber;
    newMain._prevPageNumber = origMain._prevPageNumber;
    origMain._prevPageNumber = newMain._pageNumber;
    
    if(prevMain != null) {
      setModified(new CacheDataPage(prevMain));
      prevMain._nextPageNumber = newMain._pageNumber;
    }
  }

  private void separateFromNextPeer(CacheDataPage cacheDataPage)
    throws IOException
  {
    DataPageMain dpMain = cacheDataPage._main;

    setModified(cacheDataPage);

    DataPageMain nextMain = dpMain.getNextPage();
    setModified(new CacheDataPage(nextMain));

    nextMain._prevPageNumber = INVALID_INDEX_PAGE_NUMBER;
    dpMain._nextPageNumber = INVALID_INDEX_PAGE_NUMBER;
  }
  
  private void reparentChildren(CacheDataPage cacheDataPage)
    throws IOException
  {
    DataPageMain dpMain = cacheDataPage._main;
    DataPageExtra dpExtra = cacheDataPage._extra;

    // note, the "parent" page number is not actually persisted, so we do not
    // need to mark any updated pages as modified.  for the same reason, we
    // don't need to load the pages if not already loaded
    for(Entry entry : dpExtra._entryView) {
      Integer childPageNumber = entry.getSubPageNumber();
      DataPageMain childMain = _dataPages.get(childPageNumber);
      if(childMain != null) {
        childMain.setParentPage(dpMain._pageNumber,
                                dpMain.isChildTailPageNumber(childPageNumber));
      }
    }
  }

  private void demoteTail(CacheDataPage cacheDataPage)
    throws IOException
  {
    // there's only one entry on the page, and it's the tail.  make it a
    // normal entry
    DataPageMain dpMain = cacheDataPage._main;

    DataPageMain tailMain = dpMain.getChildTailPage();
    CacheDataPage tailDataPage = new CacheDataPage(tailMain);
    removeParentEntry(tailDataPage);
    
    tailMain.setParentPage(dpMain._pageNumber, false);

    addParentEntry(cacheDataPage, tailDataPage);
  }
  
  private void promoteTail(CacheDataPage cacheDataPage)
    throws IOException
  {
    // there's not tail currently on this page, make last entry a tail
    DataPageMain dpMain = cacheDataPage._main;
    DataPageExtra dpExtra = cacheDataPage._extra;

    DataPageMain lastMain = dpMain.getChildPage(dpExtra._entryView.getLast());
    CacheDataPage lastDataPage = new CacheDataPage(lastMain);
    removeParentEntry(lastDataPage);
    
    lastMain.setParentPage(dpMain._pageNumber, true);

    addParentEntry(cacheDataPage, lastDataPage);
  }
  
  public CacheDataPage findCacheDataPage(Entry e)
    throws IOException
  {
    DataPageMain curPage = _rootPage;
    while(true) {

      if(curPage._leaf) {
        // nowhere to go from here
        return new CacheDataPage(curPage);
      }
      
      DataPageExtra extra = curPage.getExtra();

      // need to descend
      int idx = extra._entryView.find(e);
      if(idx < 0) {
        idx = missingIndexToInsertionPoint(idx);
        if(idx == extra._entryView.size()) {
          // just move to last child page
          --idx;
        }
      }

      Entry nodeEntry = extra._entryView.get(idx);
      curPage = curPage.getChildPage(nodeEntry);
    }
  }

  private void setModified(CacheDataPage cacheDataPage)
  {
    if(!cacheDataPage._extra._modified) {
      _modifiedPages.add(cacheDataPage);
      cacheDataPage._extra._modified = true;
    }
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

  /**
   * Used by unit tests to validate the internal status of the index.
   */
  void validate() throws IOException {
    for(DataPageMain dpMain : _dataPages.values()) {
      DataPageExtra dpExtra = dpMain.getExtra();
      validateEntries(dpExtra);
      validateChildren(dpMain, dpExtra);
      validatePeers(dpMain);
    }
  }

  private void validateEntries(DataPageExtra dpExtra) throws IOException {
    int entrySize = 0;
    Entry prevEntry = Index.FIRST_ENTRY;
    for(Entry e : dpExtra._entries) {
      entrySize += e.size();
      if(prevEntry.compareTo(e) >= 0) {
        throw new IOException("Unexpected order in index entries, " +
                              prevEntry + " >= " + e);
      }
      prevEntry = e;
    }
    if(entrySize != dpExtra._totalEntrySize) {
      throw new IllegalStateException("Expected size " + entrySize +
                                      " but was " + dpExtra._totalEntrySize);
    }
  }

  private void validateChildren(DataPageMain dpMain,
                                DataPageExtra dpExtra) throws IOException {
    int childTailPageNumber = dpMain._childTailPageNumber;
    if(dpMain._leaf) {
      if(childTailPageNumber != INVALID_INDEX_PAGE_NUMBER) {
        throw new IllegalStateException("Leaf page has tail");
      }
      return;
    }
    for(Entry e : dpExtra._entryView) {
      validateEntryForPage(dpMain, e);
      Integer subPageNumber = e.getSubPageNumber();
      DataPageMain childMain = _dataPages.get(subPageNumber);
      if(childMain != null) {
        if(childMain._parentPageNumber != null) {
          if((int)childMain._parentPageNumber != dpMain._pageNumber) {
            throw new IllegalStateException("Child's parent is incorrect " +
                                            childMain);
          }
          boolean expectTail = ((int)subPageNumber == childTailPageNumber);
          if(expectTail != childMain._tail) {
            throw new IllegalStateException("Child tail status incorrect " +
                                            childMain);
          }
        }
        Entry lastEntry = childMain.getExtra()._entryView.getLast();
        if(e.compareTo(lastEntry) != 0) {
          throw new IllegalStateException("Invalid entry " + e +
                                          " but child is " + lastEntry);
        }
      }
    }
  }

  private void validatePeers(DataPageMain dpMain) throws IOException {
    DataPageMain prevMain = _dataPages.get(dpMain._prevPageNumber);
    if(prevMain != null) {
      if((int)prevMain._nextPageNumber != dpMain._pageNumber) {
        throw new IllegalStateException("Prev page " + prevMain +
                                        " does not ref " + dpMain);
      }
      validatePeerStatus(dpMain, prevMain);
    }
    
    DataPageMain nextMain = _dataPages.get(dpMain._nextPageNumber);
    if(nextMain != null) {
      if((int)nextMain._prevPageNumber != dpMain._pageNumber) {
        throw new IllegalStateException("Next page " + nextMain +
                                        " does not ref " + dpMain);
      }
      validatePeerStatus(dpMain, nextMain);
    }
  }

  private void validatePeerStatus(DataPageMain dpMain, DataPageMain peerMain)
    throws IOException
  {
    if(dpMain._leaf != peerMain._leaf) {
      throw new IllegalStateException("Mismatched peer status " +
                                      dpMain._leaf + " " + peerMain._leaf);
    }
    if(!dpMain._leaf) {
      if((dpMain._parentPageNumber != null) &&
         (peerMain._parentPageNumber != null) &&
         ((int)dpMain._parentPageNumber != (int)peerMain._parentPageNumber)) {
        throw new IllegalStateException("Mismatched node parents " +
                                        dpMain._parentPageNumber + " " +
                                        peerMain._parentPageNumber);
      }
    }
  }
  
  private void dumpPage(StringBuilder rtn, DataPageMain dpMain) {
    try {
      CacheDataPage cacheDataPage = new CacheDataPage(dpMain);
      rtn.append(cacheDataPage).append("\n");
      if(!dpMain._leaf) {
        for(Entry e : cacheDataPage._extra._entryView) {
          DataPageMain childMain = dpMain.getChildPage(e);
          dumpPage(rtn, childMain);
        }
      }
    } catch(IOException e) {
      rtn.append("Page[" + dpMain._pageNumber + "]: " + e);
    }
  }
  
  @Override
  public String toString() {
    if(_rootPage == null) {
      return "Cache: (uninitialized)";
    }
    
    StringBuilder rtn = new StringBuilder("Cache: \n");
    dumpPage(rtn, _rootPage);
    return rtn.toString();
  }

  private class DataPageMain
  {
    public final int _pageNumber;
    public Integer _prevPageNumber;
    public Integer _nextPageNumber;
    public Integer _childTailPageNumber;
    public Integer _parentPageNumber;
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

    public boolean hasChildTail() {
      return((int)_childTailPageNumber != INVALID_INDEX_PAGE_NUMBER);
    }

    public boolean isChildTailPageNumber(int pageNumber) {
      return((int)_childTailPageNumber == pageNumber);
    }
    
    public DataPageMain getParentPage() throws IOException
    {
      resolveParent();
      return IndexPageCache.this.getDataPage(_parentPageNumber);
    }

    public void initParentPage(Integer parentPageNumber, boolean isTail) {
      // only set if not already set
      if(_parentPageNumber == null) {
        setParentPage(parentPageNumber, isTail);
      }
    }
    
    public void setParentPage(Integer parentPageNumber, boolean isTail) {
      _parentPageNumber = parentPageNumber;
      _tail = isTail;
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
      Integer childPageNumber = e.getSubPageNumber();
      return IndexPageCache.this.getChildDataPage(
          childPageNumber, this, isChildTailPageNumber(childPageNumber));
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
        setExtra(extra);
      }
      
      return extra;
    }

    public void setExtra(DataPageExtra extra) throws IOException
    {
      extra.setEntryView(this);
      _extra = new SoftReference<DataPageExtra>(extra);
    }

    private void resolveParent() throws IOException {
      if(_parentPageNumber == null) {
        // the act of searching for the last entry should resolve any parent
        // pages along the path
        findCacheDataPage(getExtra()._entryView.getLast());
        if(_parentPageNumber == null) {
          throw new IllegalStateException("Parent was not resolved");
        }
      }
    }

    @Override
    public String toString() {
      return (_leaf ? "Leaf" : "Node") + "DPMain[" + _pageNumber +
        "] " + _prevPageNumber + ", " + _nextPageNumber + ", (" +
        _childTailPageNumber + ")";
    }
  }

  private static class DataPageExtra
  {
    /** sorted collection of index entries.  this is kept in a list instead of
        a SortedSet because the SortedSet has lame traversal utilities */
    public List<Entry> _entries;
    public EntryListView _entryView;
    public byte[] _entryPrefix;
    public int _totalEntrySize;
    public boolean _modified;

    private DataPageExtra()
    {
    }

    public void setEntryView(DataPageMain main) throws IOException {
      _entryView = new EntryListView(main, this);
    }
    
    public void updateEntryPrefix() {
      if(_entryPrefix.length == 0) {
        // prefix is only related to *real* entries, tail not included
        _entryPrefix = findCommonPrefix(_entries.get(0),
                                        _entries.get(_entries.size() - 1));
      }
    }
    
    @Override
    public String toString() {
      return "DPExtra: " + _entryView;
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

  private static class EntryListView extends AbstractList<Entry>
    implements RandomAccess
  {
    private final DataPageExtra _extra;
    private Entry _childTailEntry;

    private EntryListView(DataPageMain main, DataPageExtra extra)
      throws IOException
    {
      if(main.hasChildTail()) {
        _childTailEntry = main.getChildTailPage().getExtra()._entryView
          .getLast().asNodeEntry(main._childTailPageNumber);
      }
      _extra = extra;
    }

    @Override
    public int size() {
      int size = _extra._entries.size();
      if(hasChildTail()) {
        ++size;
      }
      return size;
    }

    @Override
    public Entry get(int idx) {
      return (isCurrentChildTailIndex(idx) ?
              _childTailEntry :
              _extra._entries.get(idx));
    }

    @Override
    public Entry set(int idx, Entry newEntry) {
      return (isCurrentChildTailIndex(idx) ?
              setChildTailEntry(newEntry) :
              _extra._entries.set(idx, newEntry));
    }
    
    @Override
    public void add(int idx, Entry newEntry) {
      if(isNewChildTailIndex(idx)) {
        setChildTailEntry(newEntry);
      } else {
        _extra._entries.add(idx, newEntry);
      }
    }
    
    @Override
    public Entry remove(int idx) {
      return (isCurrentChildTailIndex(idx) ?
              setChildTailEntry(null) :
              _extra._entries.remove(idx));
    }

    public Entry setChildTailEntry(Entry newEntry) {
      Entry old = _childTailEntry;
      _childTailEntry = newEntry;
      return old;
    }

    public Entry getChildTailEntry() {
      return _childTailEntry;
    }
    
    private boolean hasChildTail() {
      return(_childTailEntry != null);
    }
    
    private boolean isCurrentChildTailIndex(int idx) {
      return(idx == _extra._entries.size());
    }

    private boolean isNewChildTailIndex(int idx) {
      return(idx == (_extra._entries.size() + 1));
    }

    public Entry getLast() {
      return(hasChildTail() ? _childTailEntry :
             (!_extra._entries.isEmpty() ?
              _extra._entries.get(_extra._entries.size() - 1) : null));
    }

    public int find(Entry e) {
      return Collections.binarySearch(this, e);
    }

  }

}
