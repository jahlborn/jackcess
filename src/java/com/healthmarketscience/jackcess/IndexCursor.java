/*
Copyright (c) 2013 James Ahlborn

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
import java.util.Iterator;
import java.util.Map;

/**
 * Cursor backed by an index with extended traversal options.
 *
 * @author James Ahlborn
 */
public interface IndexCursor extends Cursor
{

  public Index getIndex();

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
    throws IOException;

  /**
   * Moves to the first row (as defined by the cursor) where the index entries
   * are >= the given values.  If a an exception is thrown, the cursor is
   * restored to its previous state.
   *
   * @param entryValues the column values for the index's columns.
   */
  public void findClosestRowByEntry(Object... entryValues) 
    throws IOException;

  /**
   * Returns {@code true} if the current row matches the given index entries.
   * 
   * @param entryValues the column values for the index's columns.
   */
  public boolean currentRowMatchesEntry(Object... entryValues) 
    throws IOException;

  /**
   * Returns a modifiable Iterator which will iterate through all the rows of
   * this table which match the given index entries.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Row> entryIterator(Object... entryValues);

  /**
   * Returns a modifiable Iterator which will iterate through all the rows of
   * this table which match the given index entries, returning only the given
   * columns.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Row> entryIterator(
      Collection<String> columnNames, Object... entryValues);

  /**
   * Returns an Iterable whose iterator() method returns the result of a call
   * to {@link #entryIterator(Object...)}
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Row> entryIterable(Object... entryValues);

  /**
   * Returns an Iterable whose iterator() method returns the result of a call
   * to {@link #entryIterator(Collection,Object...)}
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Row> entryIterable(
      Collection<String> columnNames, Object... entryValues);

}
