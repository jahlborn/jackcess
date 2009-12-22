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
  
  public PageChannel getPageChannel() {
    return getIndex().getPageChannel();
  }

  /**
   * Sets the root page for this index, must be called before normal usage.
   *
   * @param pageNumber the root page number
   */
  public void setRootPageNumber(int pageNumber) throws IOException {
    _rootPage = getDataPage(pageNumber);
    // root page has no parent
    _rootPage.initParentPage(INVALID_INDEX_PAGE_NUMBER, false);
  }
  
  /**
   * Writes any outstanding changes for this index to the file.
   */
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

  /**
   * Handles any modified pages which are empty as the first pass during a
   * {@link #write} call.  All empty pages are removed from the _modifiedPages
   * collection by this method.
   */
  private void handleEmptyPages() throws IOException
  {
    for(Iterator<CacheDataPage> iter = _modifiedPages.iterator();
        iter.hasNext(); ) {
      CacheDataPage cacheDataPage = iter.next();
      if(cacheDataPage._extra._entryView.isEmpty()) {
        if(!cacheDataPage._main.isRoot()) {
          deleteDataPage(cacheDataPage);
        } else {
          writeDataPage(cacheDataPage);
        }
        iter.remove();
      }
    }
  }
  
  /**
   * Prepares any non-empty modified pages for writing as the second pass
   * during a {@link #write} call.  Updates entry prefixes, promotes/demotes
   * tail pages, and splits pages as needed.
   */
  private void preparePagesForWriting() throws IOException
  {
    boolean splitPages = false;
    int maxPageEntrySize = getIndex().getMaxPageEntrySize();

    // we need to continue looping through all the pages until we do not split
    // any pages (because a split may cascade up the tree)
    do {
      splitPages = false;

      // we might be adding to this list while iterating, so we can't use an
      // iterator
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

  /**
   * Writes any non-empty modified pages as the last pass during a
   * {@link #write} call.  Clears the _modifiedPages collection when finised.
   */
  private void writeDataPages() throws IOException
  {
    for(CacheDataPage cacheDataPage : _modifiedPages) {
      if(cacheDataPage._extra._entryView.isEmpty()) {
        throw new IllegalStateException("Unexpected empty page " +
                                        cacheDataPage);
      }
      writeDataPage(cacheDataPage);
    }
    _modifiedPages.clear();
  }

  /**
   * Returns a CacheDataPage for the given page number, may be {@code null} if
   * the given page number is invalid.  Loads the given page if necessary.
   */
  public CacheDataPage getCacheDataPage(Integer pageNumber)
    throws IOException
  {
    DataPageMain main = getDataPage(pageNumber);
    return((main != null) ? new CacheDataPage(main) : null);
  }
  
  /**
   * Returns a DataPageMain for the given page number, may be {@code null} if
   * the given page number is invalid.  Loads the given page if necessary.
   */
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

  /**
   * Removes the entry with the given index from the given page.
   *
   * @param cacheDataPage the page from which to remove the entry
   * @param entryIdx the index of the entry to remove
   */
  private void removeEntry(CacheDataPage cacheDataPage, int entryIdx)
    throws IOException
  {
    updateEntry(cacheDataPage, entryIdx, null, UpdateType.REMOVE);
  }

  /**
   * Adds the entry to the given page at the given index.
   *
   * @param cacheDataPage the page to which to add the entry
   * @param entryIdx the index at which to add the entry
   * @param newEntry the entry to add
   */
  private void addEntry(CacheDataPage cacheDataPage,
                        int entryIdx,
                        Entry newEntry)
    throws IOException
  {
    updateEntry(cacheDataPage, entryIdx, newEntry, UpdateType.ADD);
  }
  
  /**
   * Updates the entries on the given page according to the given updateType.
   *
   * @param cacheDataPage the page to update
   * @param entryIdx the index at which to add/remove/replace the entry
   * @param newEntry the entry to add/replace
   * @param upType the type of update to make
   */
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

    // note, it's slightly ucky, but we need to load the parent page before we
    // start mucking with our entries because our parent may use our entries.
    CacheDataPage parentDataPage = (!dpMain.isRoot() ?
                                    new CacheDataPage(dpMain.getParentPage()) :
                                    null);
    
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
      removeDataPage(parentDataPage, cacheDataPage, oldLastEntry);
      return;
    }

    // determine if we need to update our parent page 
    if(!updateLast || dpMain.isRoot()) {
      // no parent
      return;
    }

    // the update to the last entry needs to be propagated to our parent
    replaceParentEntry(parentDataPage, cacheDataPage, oldLastEntry);
  }

  /**
   * Removes an index page which has become empty.  If this page is the root
   * page, just clears it.
   *
   * @param parentDataPage the parent of the removed page
   * @param cacheDataPage the page to remove
   * @param oldLastEntry the last entry for this page (before it was removed)
   */
  private void removeDataPage(CacheDataPage parentDataPage,
                              CacheDataPage cacheDataPage,
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
    updateParentEntry(parentDataPage, cacheDataPage, oldLastEntry, null,
                      UpdateType.REMOVE);

    // remove this page from any next/prev pages
    removeFromPeers(cacheDataPage);
  }

  /**
   * Removes a now empty index page from its next and previous peers.
   *
   * @param cacheDataPage the page to remove
   */
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

  /**
   * Adds an entry for the given child page to the given parent page.
   *
   * @param parentDataPage the parent page to which to add the entry
   * @param childDataPage the child from which to get the entry to add
   */
  private void addParentEntry(CacheDataPage parentDataPage,
                              CacheDataPage childDataPage)
    throws IOException
  {
    DataPageExtra childExtra = childDataPage._extra;
    updateParentEntry(parentDataPage, childDataPage, null,
                      childExtra._entryView.getLast(), UpdateType.ADD);
  }
  
  /**
   * Replaces the entry for the given child page in the given parent page.
   *
   * @param parentDataPage the parent page in which to replace the entry
   * @param childDataPage the child for which the entry is being replaced
   * @param oldEntry the old child entry for the child page
   */
  private void replaceParentEntry(CacheDataPage parentDataPage,
                                  CacheDataPage childDataPage,
                                  Entry oldEntry)
    throws IOException
  {
    DataPageExtra childExtra = childDataPage._extra;
    updateParentEntry(parentDataPage, childDataPage, oldEntry,
                      childExtra._entryView.getLast(), UpdateType.REPLACE);
  }
  
  /**
   * Updates the entry for the given child page in the given parent page
   * according to the given updateType.
   *
   * @param parentDataPage the parent page in which to update the entry
   * @param childDataPage the child for which the entry is being updated
   * @param oldEntry the old child entry to remove/replace
   * @param newEntry the new child entry to replace/add
   * @param upType the type of update to make
   */
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

  /**
   * Updates the child tail info in the given parent page according to the
   * given updateType.
   *
   * @param parentDataPage the parent page in which to update the child tail
   * @param childDataPage the child to add/replace
   * @param upType the type of update to make
   */
  private void updateParentTail(CacheDataPage parentDataPage,
                                CacheDataPage childDataPage,
                                UpdateType upType)
    throws IOException
  {
    DataPageMain parentMain = parentDataPage._main;

    int newChildTailPageNumber =
      ((upType == UpdateType.REMOVE) ?
       INVALID_INDEX_PAGE_NUMBER :
       childDataPage._main._pageNumber);
    if(!parentMain.isChildTailPageNumber(newChildTailPageNumber)) {
      setModified(parentDataPage);
      parentMain._childTailPageNumber = newChildTailPageNumber;
    }
  }
  
  /**
   * Verifies that the given entry type (node/leaf) is valid for the given
   * page (node/leaf).
   *
   * @param dpMain the page to which the entry will be added
   * @param entry the entry being added
   * @throws IllegalStateException if the entry type does not match the page
   *         type
   */
  private void validateEntryForPage(DataPageMain dpMain, Entry entry) {
    if(dpMain._leaf != entry.isLeafEntry()) {
      throw new IllegalStateException(
          "Trying to update page with wrong entry type; pageLeaf " +
          dpMain._leaf + ", entryLeaf " + entry.isLeafEntry());
    }
  }

  /**
   * Splits an index page which has too many entries on it.
   *
   * @param origDataPage the page to split
   */
  private void splitDataPage(CacheDataPage origDataPage)
    throws IOException
  {
    DataPageMain origMain = origDataPage._main;
    DataPageExtra origExtra = origDataPage._extra;

    setModified(origDataPage);
    
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

    // note, it's slightly ucky, but we need to load the parent page before we
    // start mucking with our entries because our parent may use our entries.
    DataPageMain parentMain = origMain.getParentPage();
    CacheDataPage parentDataPage = new CacheDataPage(parentMain);
    
    // note, there are many, many ways this could be improved/tweaked.  for
    // now, we just want it to be functional...
    // so, we will naively move half the entries from one page to a new page.

    CacheDataPage newDataPage = allocateNewCacheDataPage(
        parentMain._pageNumber, origMain._leaf);
    DataPageMain newMain = newDataPage._main;
    DataPageExtra newExtra = newDataPage._extra;
    
    List<Entry> headEntries =
      origExtra._entries.subList(0, ((numEntries + 1) / 2));

    // move first half of the entries from old page to new page (so we do not
    // need to muck with any tail entries)
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
    addParentEntry(parentDataPage, newDataPage);
  }

  /**
   * Copies the current root page info into a new page and nests this page
   * under the root page.  This must be done when the root page needs to be
   * split.
   *
   * @param rootDataPage the root data page
   * 
   * @return the newly created page nested under the root page
   */
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
  
  /**
   * Allocates a new index page with the given parent page and type.
   *
   * @param parentPageNumber the parent page for the new page
   * @param isLeaf whether or not the new page is a leaf page
   *
   * @return the newly created page
   */
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

    // update owned pages cache
    _index.addOwnedPage(dpMain._pageNumber);

    // needs to be written out
    CacheDataPage cacheDataPage = new CacheDataPage(dpMain, dpExtra);
    setModified(cacheDataPage);

    return cacheDataPage;
  }

  /**
   * Inserts the new page as a peer between the given original page and any
   * previous peer page.
   *
   * @param newDataPage the new index page
   * @param origDataPage the current index page
   */
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

  /**
   * Separates the given index page from any next peer page.
   *
   * @param cacheDataPage the index page to be separated
   */
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
  
  /**
   * Sets the parent info for the children of the given page to the given
   * page.
   *
   * @param cacheDataPage the page whose children need to be updated
   */
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

  /**
   * Makes the tail entry of the given page a normal entry on that page, done
   * when there is only one entry left on a page, and it is the tail.
   *
   * @param cacheDataPage the page whose tail must be updated
   */
  private void demoteTail(CacheDataPage cacheDataPage)
    throws IOException
  {
    // there's only one entry on the page, and it's the tail.  make it a
    // normal entry
    DataPageMain dpMain = cacheDataPage._main;
    DataPageExtra dpExtra = cacheDataPage._extra;

    setModified(cacheDataPage);
    
    DataPageMain tailMain = dpMain.getChildTailPage();
    CacheDataPage tailDataPage = new CacheDataPage(tailMain);

    // move the tail entry to the last normal entry
    updateParentTail(cacheDataPage, tailDataPage, UpdateType.REMOVE);
    Entry tailEntry = dpExtra._entryView.demoteTail();
    dpExtra._totalEntrySize += tailEntry.size();
    dpExtra._entryPrefix = EMPTY_PREFIX;
    
    tailMain.setParentPage(dpMain._pageNumber, false);
  }
  
  /**
   * Makes the last normal entry of the given page the tail entry on that
   * page, done when there are multiple entries on a page and no tail entry.
   *
   * @param cacheDataPage the page whose tail must be updated
   */
  private void promoteTail(CacheDataPage cacheDataPage)
    throws IOException
  {
    // there's not tail currently on this page, make last entry a tail
    DataPageMain dpMain = cacheDataPage._main;
    DataPageExtra dpExtra = cacheDataPage._extra;

    setModified(cacheDataPage);
    
    DataPageMain lastMain = dpMain.getChildPage(dpExtra._entryView.getLast());
    CacheDataPage lastDataPage = new CacheDataPage(lastMain);

    // move the "last" normal entry to the tail entry
    updateParentTail(cacheDataPage, lastDataPage, UpdateType.ADD);
    Entry lastEntry = dpExtra._entryView.promoteTail();
    dpExtra._totalEntrySize -= lastEntry.size();
    dpExtra._entryPrefix = EMPTY_PREFIX;

    lastMain.setParentPage(dpMain._pageNumber, true);
  }
  
  /**
   * Finds the index page on which the given entry does or should reside.
   *
   * @param e the entry to find
   */
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

  /**
   * Marks the given index page as modified and saves it for writing, if
   * necessary (if the page is already marked, does nothing).
   *
   * @param cacheDataPage the modified index page
   */
  private void setModified(CacheDataPage cacheDataPage)
  {
    if(!cacheDataPage._extra._modified) {
      _modifiedPages.add(cacheDataPage);
      cacheDataPage._extra._modified = true;
    }
  }

  /**
   * Finds the valid entry prefix given the first/last entries on an index
   * page.
   *
   * @param e1 the first entry on the page
   * @param e2 the last entry on the page
   *
   * @return a valid entry prefix for the page
   */
  private static byte[] findCommonPrefix(Entry e1, Entry e2)
  {
    byte[] b1 = e1.getEntryBytes();
    byte[] b2 = e2.getEntryBytes();
    
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

  /**
   * Validates the entries for an index page
   *
   * @param dpExtra the entries to validate
   */
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

  /**
   * Validates the children for an index page
   *
   * @param dpMain the index page
   * @param dpExtra the child entries to validate
   */
  private void validateChildren(DataPageMain dpMain,
                                DataPageExtra dpExtra) throws IOException {
    int childTailPageNumber = dpMain._childTailPageNumber;
    if(dpMain._leaf) {
      if(childTailPageNumber != INVALID_INDEX_PAGE_NUMBER) {
        throw new IllegalStateException("Leaf page has tail " + dpMain);
      }
      return;
    }
    if((dpExtra._entryView.size() == 1) && dpMain.hasChildTail()) {
      throw new IllegalStateException("Single child is tail " + dpMain);
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

  /**
   * Validates the peer pages for an index page.
   *
   * @param dpMain the index page
   */
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

  /**
   * Validates the given peer page against the given index page
   *
   * @param dpMain the index page
   * @param peerMain the peer index page
   */
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
  
  /**
   * Dumps the given index page to a StringBuilder
   *
   * @param rtn the StringBuilder to update
   * @param dpMain the index page to dump
   */
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

  /**
   * Keeps track of the main info for an index page.
   */
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
      return getChildPage(childPageNumber,
                          isChildTailPageNumber(childPageNumber));
    }
    
    public DataPageMain getChildTailPage() throws IOException
    {
      return getChildPage(_childTailPageNumber, true);
    }

    /**
     * Returns a child page for the given page number, updating its parent
     * info if necessary.
     */
    private DataPageMain getChildPage(Integer childPageNumber, boolean isTail)
      throws IOException
    {
      DataPageMain child = getDataPage(childPageNumber);
      if(child != null) {
        // set the parent info for this child (if necessary)
        child.initParentPage(_pageNumber, isTail);
      }
      return child;
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

  /**
   * Keeps track of the extra info for an index page.  This info (if
   * unmodified) may be re-read from disk as necessary.
   */
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

  /**
   * IndexPageCache implementation of an Index {@link DataPage}.
   */
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

  /**
   * A view of an index page's entries which combines the normal entries and
   * tail entry into one collection.
   */
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

    private List<Entry> getEntries() {
      return _extra._entries;
    }
    
    @Override
    public int size() {
      int size = getEntries().size();
      if(hasChildTail()) {
        ++size;
      }
      return size;
    }

    @Override
    public Entry get(int idx) {
      return (isCurrentChildTailIndex(idx) ?
              _childTailEntry :
              getEntries().get(idx));
    }

    @Override
    public Entry set(int idx, Entry newEntry) {
      return (isCurrentChildTailIndex(idx) ?
              setChildTailEntry(newEntry) :
              getEntries().set(idx, newEntry));
    }
    
    @Override
    public void add(int idx, Entry newEntry) {
      // note, we will never add to the "tail" entry, that will always be
      // handled through promoteTail
      getEntries().add(idx, newEntry);
    }
    
    @Override
    public Entry remove(int idx) {
      return (isCurrentChildTailIndex(idx) ?
              setChildTailEntry(null) :
              getEntries().remove(idx));
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
      return(idx == getEntries().size());
    }

    public Entry getLast() {
      return(hasChildTail() ? _childTailEntry :
             (!getEntries().isEmpty() ?
              getEntries().get(getEntries().size() - 1) : null));
    }

    public Entry demoteTail() {
      Entry tail = _childTailEntry;
      _childTailEntry = null;
      getEntries().add(tail);
      return tail;
    }
    
    public Entry promoteTail() {
      Entry last = getEntries().remove(getEntries().size() - 1);
      _childTailEntry = last;
      return last;
    }
    
    public int find(Entry e) {
      return Collections.binarySearch(this, e);
    }

  }

}
