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
import java.util.List;

/**
 *
 * @author James Ahlborn
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
