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

import com.healthmarketscience.jackcess.util.EntryIterableBuilder;

/**
 * Cursor backed by an {@link Index} with extended traversal options.  Table
 * traversal will be in the order defined by the backing index.  Lookups which
 * utilize the columns of the index will be fast.
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
   * Convenience method for constructing a new EntryIterableBuilder for this
   * cursor.  An EntryIterableBuilder provides a variety of options for more
   * flexible iteration based on a specific index entry.
   * 
   * @param entryValues the column values for the index's columns.
   */
  public EntryIterableBuilder newEntryIterable(Object... entryValues);
}
