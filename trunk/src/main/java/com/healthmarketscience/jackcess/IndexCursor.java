/*
Copyright (c) 2013 James Ahlborn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
 * @usage _general_class_
 */
public interface IndexCursor extends Cursor
{

  public Index getIndex();

  /**
   * Finds the first row (as defined by the cursor) where the index entries
   * match the given values.  If a match is not found (or an exception is
   * thrown), the cursor is restored to its previous state.
   *
   * @param entryValues the column values for the index's columns.
   * @return the matching row or {@code null} if a match could not be found.
   */
  public Row findRowByEntry(Object... entryValues)
    throws IOException;

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
   * are &gt;= the given values.  If a an exception is thrown, the cursor is
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
