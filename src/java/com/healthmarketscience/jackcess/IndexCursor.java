/*
Copyright (c) 2011 James Ahlborn

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
*/

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.Table.RowState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Cursor backed by an index with extended traversal options.
 *
 * @author James Ahlborn
 */
public class IndexCursor extends Cursor 
{
  private static final Log LOG = LogFactory.getLog(IndexCursor.class);  

  /** IndexDirHandler for forward traversal */
  private final IndexDirHandler _forwardDirHandler =
    new ForwardIndexDirHandler();
  /** IndexDirHandler for backward traversal */
  private final IndexDirHandler _reverseDirHandler =
    new ReverseIndexDirHandler();
  /** logical index which this cursor is using */
  private final Index _index;
  /** Cursor over the entries of the relevant index */
  private final IndexData.EntryCursor _entryCursor;
  /** column names for the index entry columns */
  private Set<String> _indexEntryPattern;

  private IndexCursor(Table table, Index index,
                      IndexData.EntryCursor entryCursor)
    throws IOException
  {
    super(new Id(table, index), table,
          new IndexPosition(entryCursor.getFirstEntry()),
          new IndexPosition(entryCursor.getLastEntry()));
    _index = index;
    _index.initialize();
    _entryCursor = entryCursor;
  }

  /**
   * Creates an indexed cursor for the given table.
   * <p>
   * Note, index based table traversal may not include all rows, as certain
   * types of indexes do not include all entries (namely, some indexes ignore
   * null entries, see {@link Index#shouldIgnoreNulls}).
   * 
   * @param table the table over which this cursor will traverse
   * @param index index for the table which will define traversal order as
   *              well as enhance certain lookups
   */
  public static IndexCursor createCursor(Table table, Index index)
    throws IOException
  {
    return createCursor(table, index, null, null);
  }
  
  /**
   * Creates an indexed cursor for the given table, narrowed to the given
   * range.
   * <p>
   * Note, index based table traversal may not include all rows, as certain
   * types of indexes do not include all entries (namely, some indexes ignore
   * null entries, see {@link Index#shouldIgnoreNulls}).
   * 
   * @param table the table over which this cursor will traverse
   * @param index index for the table which will define traversal order as
   *              well as enhance certain lookups
   * @param startRow the first row of data for the cursor (inclusive), or
   *                 {@code null} for the first entry
   * @param endRow the last row of data for the cursor (inclusive), or
   *               {@code null} for the last entry
   */
  public static IndexCursor createCursor(
      Table table, Index index, Object[] startRow, Object[] endRow)
    throws IOException
  {
    return createCursor(table, index, startRow, true, endRow, true);
  }
  
  /**
   * Creates an indexed cursor for the given table, narrowed to the given
   * range.
   * <p>
   * Note, index based table traversal may not include all rows, as certain
   * types of indexes do not include all entries (namely, some indexes ignore
   * null entries, see {@link Index#shouldIgnoreNulls}).
   * 
   * @param table the table over which this cursor will traverse
   * @param index index for the table which will define traversal order as
   *              well as enhance certain lookups
   * @param startRow the first row of data for the cursor, or {@code null} for
   *                 the first entry
   * @param startInclusive whether or not startRow is inclusive or exclusive
   * @param endRow the last row of data for the cursor, or {@code null} for
   *               the last entry
   * @param endInclusive whether or not endRow is inclusive or exclusive
   */
  public static IndexCursor createCursor(Table table, Index index,
                                         Object[] startRow,
                                         boolean startInclusive,
                                         Object[] endRow,
                                         boolean endInclusive)
    throws IOException
  {
    if(table != index.getTable()) {
      throw new IllegalArgumentException(
          "Given index is not for given table: " + index + ", " + table);
    }
    if(!table.getFormat().INDEXES_SUPPORTED) {
      throw new IllegalArgumentException(
          "JetFormat " + table.getFormat() + 
          " does not currently support index lookups");
    }
    if(index.getIndexData().isReadOnly()) {
      throw new IllegalArgumentException(
          "Given index " + index + 
          " is not usable for indexed lookups because it is read-only");
    }
    IndexCursor cursor = new IndexCursor(table, index,
                                         index.cursor(startRow, startInclusive,
                                                      endRow, endInclusive));
    // init the column matcher appropriately for the index type
    cursor.setColumnMatcher(null);
    return cursor;
  }  

  public Index getIndex() {
    return _index;
  }

  /**
   * @deprecated renamed to {@link #findFirstRowByEntry(Object...)} to be more
   * clear
   */
  @Deprecated
  public boolean findRowByEntry(Object... entryValues) 
    throws IOException 
  {
    return findFirstRowByEntry(entryValues);
  }

  /**
   * Moves to the first row (as defined by the cursor) where the index entries
   * match the given values.  If a match is not found (or an exception is
   * thrown), the cursor is restored to its previous state.
   * <p>
   * Warning, this method <i>always</i> starts searching from the beginning of
   * the Table (you cannot use it to find successive matches).
   *
   * @param entryValues the column values for the index's columns.
   * @return {@code true} if a valid row was found with the given values,
   *         {@code false} if no row was found
   */
  public boolean findFirstRowByEntry(Object... entryValues) 
    throws IOException 
  {
    Position curPos = _curPos;
    Position prevPos = _prevPos;
    boolean found = false;
    try {
      found = findFirstRowByEntryImpl(toRowValues(entryValues), true);
      return found;
    } finally {
      if(!found) {
        try {
          restorePosition(curPos, prevPos);
        } catch(IOException e) {
          LOG.error("Failed restoring position", e);
        }
      }
    }
  }

  /**
   * Moves to the first row (as defined by the cursor) where the index entries
   * are >= the given values.  If a an exception is thrown, the cursor is
   * restored to its previous state.
   *
   * @param entryValues the column values for the index's columns.
   */
  public void findClosestRowByEntry(Object... entryValues) 
    throws IOException 
  {
    Position curPos = _curPos;
    Position prevPos = _prevPos;
    boolean found = false;
    try {
      findFirstRowByEntryImpl(toRowValues(entryValues), false);
      found = true;
    } finally {
      if(!found) {
        try {
          restorePosition(curPos, prevPos);
        } catch(IOException e) {
          LOG.error("Failed restoring position", e);
        }
      }
    }
  }

  /**
   * Returns {@code true} if the current row matches the given index entries.
   * 
   * @param entryValues the column values for the index's columns.
   */
  public boolean currentRowMatchesEntry(Object... entryValues) 
    throws IOException 
  {
    return currentRowMatchesEntryImpl(toRowValues(entryValues));
  }
  
  /**
   * Returns a modifiable Iterator which will iterate through all the rows of
   * this table which match the given index entries.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String,Object>> entryIterator(Object... entryValues)
  {
    return entryIterator((Collection<String>)null, entryValues);
  }
  
  /**
   * Returns a modifiable Iterator which will iterate through all the rows of
   * this table which match the given index entries, returning only the given
   * columns.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Map<String,Object>> entryIterator(
      Collection<String> columnNames, Object... entryValues)
  {
    return new EntryIterator(columnNames, toRowValues(entryValues));
  }
  
  /**
   * Returns an Iterable whose iterator() method returns the result of a call
   * to {@link #entryIterator(Object...)}
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Map<String,Object>> entryIterable(Object... entryValues)
  {
    return entryIterable((Collection<String>)null, entryValues);
  }
  
  /**
   * Returns an Iterable whose iterator() method returns the result of a call
   * to {@link #entryIterator(Collection,Object...)}
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Map<String,Object>> entryIterable(
      final Collection<String> columnNames, final Object... entryValues)
  {
    return new Iterable<Map<String, Object>>() {
      public Iterator<Map<String, Object>> iterator() {
        return new EntryIterator(columnNames, toRowValues(entryValues));
      }
    };
  }
  
  @Override
  protected IndexDirHandler getDirHandler(boolean moveForward) {
    return (moveForward ? _forwardDirHandler : _reverseDirHandler);
  }
    
  @Override
  protected boolean isUpToDate() {
    return(super.isUpToDate() && _entryCursor.isUpToDate());
  }
    
  @Override
  protected void reset(boolean moveForward) {
    _entryCursor.reset(moveForward);
    super.reset(moveForward);
  }

  @Override
  protected void restorePositionImpl(Position curPos, Position prevPos)
    throws IOException
  {
    if(!(curPos instanceof IndexPosition) ||
       !(prevPos instanceof IndexPosition)) {
      throw new IllegalArgumentException(
          "Restored positions must be index positions");
    }
    _entryCursor.restorePosition(((IndexPosition)curPos).getEntry(),
                                 ((IndexPosition)prevPos).getEntry());
    super.restorePositionImpl(curPos, prevPos);
  }

  @Override
  protected boolean findNextRowImpl(Column columnPattern, Object valuePattern)
    throws IOException
  {
    if(!isBeforeFirst()) {
      // use the default table scan for finding rows mid-cursor
      return super.findNextRowImpl(columnPattern, valuePattern);
    }

    // searching for the first match
    Object[] rowValues = _entryCursor.getIndexData().constructIndexRow(
        columnPattern.getName(), valuePattern);

    if(rowValues == null) {
      // bummer, use the default table scan
      return super.findNextRowImpl(columnPattern, valuePattern);
    }
      
    // sweet, we can use our index
    if(!findPotentialRow(rowValues, true)) {
      return false;
    }

    // either we found a row with the given value, or none exist in the
    // table
    return currentRowMatches(columnPattern, valuePattern);
  }

  /**
   * Moves to the first row (as defined by the cursor) where the index entries
   * match the given values.  Caller manages save/restore on failure.
   *
   * @param rowValues the column values built from the index column values
   * @param requireMatch whether or not an exact match is found
   * @return {@code true} if a valid row was found with the given values,
   *         {@code false} if no row was found
   */
  protected boolean findFirstRowByEntryImpl(Object[] rowValues,
                                            boolean requireMatch) 
    throws IOException 
  {
    if(!findPotentialRow(rowValues, requireMatch)) {
      return false;
    } else if(!requireMatch) {
      // nothing more to do, we have moved to the closest row
      return true;
    }

    return currentRowMatchesEntryImpl(rowValues);
  }

  @Override
  protected boolean findNextRowImpl(Map<String,?> rowPattern)
    throws IOException
  {
    if(!isBeforeFirst()) {
      // use the default table scan for finding rows mid-cursor
      return super.findNextRowImpl(rowPattern);
    }

    // searching for the first match
    IndexData indexData = _entryCursor.getIndexData();
    Object[] rowValues = indexData.constructIndexRow(rowPattern);

    if(rowValues == null) {
      // bummer, use the default table scan
      return super.findNextRowImpl(rowPattern);
    }

    // sweet, we can use our index
    if(!findPotentialRow(rowValues, true)) {
      // at end of index, no potential matches
      return false;
    }

    // find actual matching row
    Map<String,?> indexRowPattern = null;
    if(rowPattern.size() == indexData.getColumns().size()) {
      // the rowPattern matches our index columns exactly, so we can
      // streamline our testing below
      indexRowPattern = rowPattern;
    } else {
      // the rowPattern has more columns than just the index, so we need to
      // do more work when testing below
      Map<String,Object> tmpRowPattern = new LinkedHashMap<String,Object>();
      indexRowPattern = tmpRowPattern;
      for(IndexData.ColumnDescriptor idxCol : indexData.getColumns()) {
        tmpRowPattern.put(idxCol.getName(), rowValues[idxCol.getColumnIndex()]);
      }
    }
      
    // there may be multiple columns which fit the pattern subset used by
    // the index, so we need to keep checking until our index values no
    // longer match
    do {

      if(!currentRowMatches(indexRowPattern)) {
        // there are no more rows which could possibly match
        break;
      }

      // note, if rowPattern == indexRowPattern, no need to do an extra
      // comparison with the current row
      if((rowPattern == indexRowPattern) || currentRowMatches(rowPattern)) {
        // found it!
        return true;
      }

    } while(moveToNextRow());
        
    // none of the potential rows matched
    return false;
  }

  private boolean currentRowMatchesEntryImpl(Object[] rowValues)
    throws IOException
  {
    if(_indexEntryPattern == null) {
      // init our set of index column names
      _indexEntryPattern = new HashSet<String>();
      for(IndexData.ColumnDescriptor col : getIndex().getColumns()) {
        _indexEntryPattern.add(col.getName());
      }
    }

    // check the next row to see if it actually matches
    Map<String,Object> row = getCurrentRow(_indexEntryPattern);

    for(IndexData.ColumnDescriptor col : getIndex().getColumns()) {
      String columnName = col.getName();
      Object patValue = rowValues[col.getColumnIndex()];
      Object rowValue = row.get(columnName);
      if(!_columnMatcher.matches(getTable(), columnName,
                                 patValue, rowValue)) {
        return false;
      }
    }

    return true;    
  }
  
  private boolean findPotentialRow(Object[] rowValues, boolean requireMatch)
    throws IOException
  {
    _entryCursor.beforeEntry(rowValues);
    IndexData.Entry startEntry = _entryCursor.getNextEntry();
    if(requireMatch && !startEntry.getRowId().isValid()) {
      // at end of index, no potential matches
      return false;
    }
    // move to position and check it out
    restorePosition(new IndexPosition(startEntry));
    return true;
  }

  private Object[] toRowValues(Object[] entryValues)
  {
    return _entryCursor.getIndexData().constructIndexRowFromEntry(entryValues);
  }
  
  @Override
  protected Position findAnotherPosition(RowState rowState, Position curPos,
                                         boolean moveForward)
    throws IOException
  {
    IndexDirHandler handler = getDirHandler(moveForward);
    IndexPosition endPos = (IndexPosition)handler.getEndPosition();
    IndexData.Entry entry = handler.getAnotherEntry();
    return ((!entry.equals(endPos.getEntry())) ?
            new IndexPosition(entry) : endPos);
  }

  @Override
  protected ColumnMatcher getDefaultColumnMatcher() {
    if(getIndex().isUnique()) {
      // text indexes are case-insensitive, therefore we should always use a
      // case-insensitive matcher for unique indexes.
      return CaseInsensitiveColumnMatcher.INSTANCE;
    }
    return SimpleColumnMatcher.INSTANCE;
  }

  /**
   * Handles moving the table index cursor in a given direction.  Separates
   * cursor logic from value storage.
   */
  private abstract class IndexDirHandler extends DirHandler {
    public abstract IndexData.Entry getAnotherEntry()
      throws IOException;
  }
    
  /**
   * Handles moving the table index cursor forward.
   */
  private final class ForwardIndexDirHandler extends IndexDirHandler {
    @Override
    public Position getBeginningPosition() {
      return getFirstPosition();
    }
    @Override
    public Position getEndPosition() {
      return getLastPosition();
    }
    @Override
    public IndexData.Entry getAnotherEntry() throws IOException {
      return _entryCursor.getNextEntry();
    }
  }
    
  /**
   * Handles moving the table index cursor backward.
   */
  private final class ReverseIndexDirHandler extends IndexDirHandler {
    @Override
    public Position getBeginningPosition() {
      return getLastPosition();
    }
    @Override
    public Position getEndPosition() {
      return getFirstPosition();
    }
    @Override
    public IndexData.Entry getAnotherEntry() throws IOException {
      return _entryCursor.getPreviousEntry();
    }
  }    
    
  /**
   * Value object which maintains the current position of an IndexCursor.
   */
  private static final class IndexPosition extends Position
  {
    private final IndexData.Entry _entry;
    
    private IndexPosition(IndexData.Entry entry) {
      _entry = entry;
    }

    @Override
    public RowId getRowId() {
      return getEntry().getRowId();
    }
    
    public IndexData.Entry getEntry() {
      return _entry;
    }
    
    @Override
    protected boolean equalsImpl(Object o) {
      return getEntry().equals(((IndexPosition)o).getEntry());
    }

    @Override
    public String toString() {
      return "Entry = " + getEntry();
    }
  }

  /**
   * Row iterator (by matching entry) for this cursor, modifiable.
   */
  private final class EntryIterator extends BaseIterator
  {
    private final Object[] _rowValues;
    
    private EntryIterator(Collection<String> columnNames, Object[] rowValues)
    {
      super(columnNames);
      _rowValues = rowValues;
      try {
        _hasNext = findFirstRowByEntryImpl(rowValues, true);
        _validRow = _hasNext;
      } catch(IOException e) {
          throw new IllegalStateException(e);
      }
    }

    @Override
    protected boolean findNext() throws IOException {
      return (moveToNextRow() && currentRowMatchesEntryImpl(_rowValues));
    }    
  }

  
}
