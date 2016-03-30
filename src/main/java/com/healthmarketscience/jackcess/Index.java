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
import java.util.List;

/**
 * Access database index definition.  A {@link Table} has a list of Index
 * instances.  Indexes can enable fast searches and ordered traversal on a
 * Table (for the indexed columns).  These features can be utilized via an
 * {@link IndexCursor}.
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public interface Index 
{

  public Table getTable();

  public String getName();

  public boolean isPrimaryKey();

  public boolean isForeignKey();

  /**
   * @return the Columns for this index (unmodifiable)
   */
  public List<? extends Index.Column> getColumns();

  /**
   * @return the Index referenced by this Index's ForeignKeyReference (if it
   *         has one), otherwise {@code null}.
   */
  public Index getReferencedIndex() throws IOException;

  /**
   * Whether or not {@code null} values are actually recorded in the index.
   */
  public boolean shouldIgnoreNulls();

  /**
   * Whether or not index entries must be unique.
   * <p>
   * Some notes about uniqueness:
   * <ul>
   * <li>Access does not seem to consider multiple {@code null} entries
   *     invalid for a unique index</li>
   * <li>text indexes collapse case, and Access seems to compare <b>only</b>
   *     the index entry bytes, therefore two strings which differ only in
   *     case <i>will violate</i> the unique constraint</li>
   * </ul>
   */
  public boolean isUnique();

  /**
   * Whether or not values are required for index columns.
   */
  public boolean isRequired();

  /**
   * Convenience method for constructing a new CursorBuilder for this Index.
   */
  public CursorBuilder newCursor();

  /**
   * Information about a Column in an Index
   */
  public interface Column {

    public com.healthmarketscience.jackcess.Column getColumn();

    public boolean isAscending();

    public int getColumnIndex();
    
    public String getName();
  }
}
