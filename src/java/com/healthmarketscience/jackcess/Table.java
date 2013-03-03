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
import java.util.Map;

/**
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public abstract class Table implements Iterable<Map<String, Object>>
{
  /**
   * enum which controls the ordering of the columns in a table.
   * @usage _intermediate_class_
   */
  public enum ColumnOrder {
    /** columns are ordered based on the order of the data in the table (this
        order does not change as columns are added to the table). */
    DATA, 
    /** columns are ordered based on the "display" order (this order can be
        changed arbitrarily) */
    DISPLAY;
  }

  /**
   * @return The name of the table
   * @usage _general_method_
   */
  public abstract String getName();

  /**
   * Whether or not this table has been marked as hidden.
   * @usage _general_method_
   */
  public abstract boolean isHidden();

  /**
   * @usage _general_method_
   */
  public abstract int getColumnCount();

  /**
   * @usage _general_method_
   */
  public abstract Database getDatabase();

  /**
   * Gets the currently configured ErrorHandler (always non-{@code null}).
   * This will be used to handle all errors unless overridden at the Cursor
   * level.
   * @usage _intermediate_method_
   */
  public abstract ErrorHandler getErrorHandler();

  /**
   * Sets a new ErrorHandler.  If {@code null}, resets to using the
   * ErrorHandler configured at the Database level.
   * @usage _intermediate_method_
   */
  public abstract void setErrorHandler(ErrorHandler newErrorHandler);

  /**
   * @return All of the columns in this table (unmodifiable List)
   * @usage _general_method_
   */
  public abstract List<Column> getColumns();

  /**
   * @return the column with the given name
   * @usage _general_method_
   */
  public abstract Column getColumn(String name);

  /**
   * @return the properties for this table
   * @usage _general_method_
   */
  public abstract PropertyMap getProperties() throws IOException;

  /**
   * @return All of the Indexes on this table (unmodifiable List)
   * @usage _intermediate_method_
   */
  public abstract List<? extends Index> getIndexes();

  /**
   * @return the index with the given name
   * @throws IllegalArgumentException if there is no index with the given name
   * @usage _intermediate_method_
   */
  public abstract Index getIndex(String name);

  /**
   * @return the primary key index for this table
   * @throws IllegalArgumentException if there is no primary key index on this
   *         table
   * @usage _intermediate_method_
   */
  public abstract Index getPrimaryKeyIndex();

  /**
   * @return the foreign key index joining this table to the given other table
   * @throws IllegalArgumentException if there is no relationship between this
   *         table and the given table
   * @usage _intermediate_method_
   */
  public abstract Index getForeignKeyIndex(Table otherTable);

  /**
   * Converts a map of columnName -> columnValue to an array of row values
   * appropriate for a call to {@link #addRow(Object...)}.
   * @usage _general_method_
   */
  public abstract Object[] asRow(Map<String,?> rowMap);

  /**
   * Converts a map of columnName -> columnValue to an array of row values
   * appropriate for a call to {@link #updateCurrentRow(Object...)}.
   * @usage _general_method_
   */
  public abstract Object[] asUpdateRow(Map<String,?> rowMap);

  /**
   * Adds a single row to this table and writes it to disk.  The values are
   * expected to be given in the order that the Columns are listed by the
   * {@link #getColumns} method.  This is by default the storage order of the
   * Columns in the database, however this order can be influenced by setting
   * the ColumnOrder via {@link DatabaseImpl#setColumnOrder} prior to opening the
   * Table.  The {@link #asRow} method can be used to easily convert a row Map into the
   * appropriate row array for this Table.
   * <p>
   * Note, if this table has an auto-number column, the value generated will be
   * put back into the given row array (assuming the given row array is at
   * least as long as the number of Columns in this Table).
   *
   * @param row row values for a single row.  the given row array will be
   *            modified if this table contains an auto-number column,
   *            otherwise it will not be modified.
   * @usage _general_method_
   */
  public abstract void addRow(Object... row) throws IOException;

  /**
   * Add multiple rows to this table, only writing to disk after all
   * rows have been written, and every time a data page is filled.  This
   * is much more efficient than calling <code>addRow</code> multiple times.
   * <p>
   * Note, if this table has an auto-number column, the values written will be
   * put back into the given row arrays (assuming the given row array is at
   * least as long as the number of Columns in this Table).
   *
   * @see #addRow(Object...) for more details on row arrays
   * 
   * @param rows List of Object[] row values.  the rows will be modified if
   *             this table contains an auto-number column, otherwise they
   *             will not be modified.
   * @usage _general_method_
   */
  public abstract void addRows(List<? extends Object[]> rows) throws IOException;

  /**
   * @usage _general_method_
   */
  public abstract int getRowCount();
}
