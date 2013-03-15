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

import com.healthmarketscience.jackcess.util.ErrorHandler;
import com.healthmarketscience.jackcess.util.ColumnMatcher;

/**
 * Manages iteration for a Table.  Different cursors provide different methods
 * of traversing a table.  Cursors should be fairly robust in the face of
 * table modification during traversal (although depending on how the table is
 * traversed, row updates may or may not be seen).  Multiple cursors may
 * traverse the same table simultaneously.
 * <p>
 * The {@link CursorBuilder} provides a variety of static utility methods to
 * construct cursors with given characteristics or easily search for specific
 * values as well as friendly and flexible construction options.
 * <p>
 * Is not thread-safe.
 *
 * @author James Ahlborn
 */
public interface Cursor extends Iterable<Row>
{

  public Id getId();

  public Table getTable();

  /**
   * Gets the currently configured ErrorHandler (always non-{@code null}).
   * This will be used to handle all errors.
   */
  public ErrorHandler getErrorHandler();

  /**
   * Sets a new ErrorHandler.  If {@code null}, resets to using the
   * ErrorHandler configured at the Table level.
   */
  public void setErrorHandler(ErrorHandler newErrorHandler);

  /**
   * Returns the currently configured ColumnMatcher, always non-{@code null}.
   */
  public ColumnMatcher getColumnMatcher();

  /**
   * Sets a new ColumnMatcher.  If {@code null}, resets to using the default
   * matcher (default depends on Cursor type).
   */
  public void setColumnMatcher(ColumnMatcher columnMatcher);

  /**
   * Returns the current state of the cursor which can be restored at a future
   * point in time by a call to {@link #restoreSavepoint}.
   * <p>
   * Savepoints may be used across different cursor instances for the same
   * table, but they must have the same {@link Id}.
   */
  public Savepoint getSavepoint();

  /**
   * Moves the cursor to a savepoint previously returned from
   * {@link #getSavepoint}.
   * @throws IllegalArgumentException if the given savepoint does not have a
   *         cursorId equal to this cursor's id
   */
  public void restoreSavepoint(Savepoint savepoint)
    throws IOException;

  /**
   * Resets this cursor for forward traversal.  Calls {@link #beforeFirst}.
   */
  public void reset();

  /**
   * Resets this cursor for forward traversal (sets cursor to before the first
   * row).
   */
  public void beforeFirst();

  /**
   * Resets this cursor for reverse traversal (sets cursor to after the last
   * row).
   */
  public void afterLast();

  /**
   * Returns {@code true} if the cursor is currently positioned before the
   * first row, {@code false} otherwise.
   */
  public boolean isBeforeFirst() throws IOException;

  /**
   * Returns {@code true} if the cursor is currently positioned after the
   * last row, {@code false} otherwise.
   */
  public boolean isAfterLast() throws IOException;

  /**
   * Returns {@code true} if the row at which the cursor is currently
   * positioned is deleted, {@code false} otherwise (including invalid rows).
   */
  public boolean isCurrentRowDeleted() throws IOException;

  /**
   * Returns an Iterable whose iterator() method calls {@link #afterLast} on
   * this cursor and returns a modifiable Iterator which will iterate through
   * all the rows of this table in reverse order.  Use of the Iterator follows
   * the same restrictions as a call to {@link #getPreviousRow}.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Row> reverseIterable();
  
  /**
   * Returns an Iterable whose iterator() method calls {@link #afterLast} on
   * this table and returns a modifiable Iterator which will iterate through
   * all the rows of this table in reverse order, returning only the given
   * columns.  Use of the Iterator follows the same restrictions as a call to
   * {@link #getPreviousRow}.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Row> reverseIterable(
      Collection<String> columnNames);

  /**
   * Calls {@link #beforeFirst} on this cursor and returns a modifiable
   * Iterator which will iterate through all the rows of this table.  Use of
   * the Iterator follows the same restrictions as a call to
   * {@link #getNextRow}.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterator<Row> iterator();

  /**
   * Returns an Iterable whose iterator() method calls {@link #beforeFirst} on
   * this table and returns a modifiable Iterator which will iterate through
   * all the rows of this table, returning only the given columns.  Use of the
   * Iterator follows the same restrictions as a call to {@link #getNextRow}.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Row> iterable(
      Collection<String> columnNames);

  /**
   * Returns an Iterable whose iterator() method calls {@link #beforeFirst} on
   * this cursor and returns a modifiable Iterator which will iterate through
   * all the rows of this table which match the given column pattern.  Use of
   * the Iterator follows the same restrictions as a call to {@link
   * #getNextRow}.  See {@link #findFirstRow(Column,Object)} for details on
   * #the columnPattern.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Row> columnMatchIterable(
      Column columnPattern, Object valuePattern);

  /**
   * Returns an Iterable whose iterator() method calls {@link #beforeFirst} on
   * this table and returns a modifiable Iterator which will iterate through
   * all the rows of this table which match the given column pattern,
   * returning only the given columns.  Use of the Iterator follows the same
   * restrictions as a call to {@link #getNextRow}.  See {@link
   * #findFirstRow(Column,Object)} for details on the columnPattern.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Row> columnMatchIterable(
      Collection<String> columnNames,
      Column columnPattern, Object valuePattern);

  /**
   * Returns an Iterable whose iterator() method calls {@link #beforeFirst} on
   * this cursor and returns a modifiable Iterator which will iterate through
   * all the rows of this table which match the given row pattern.  Use of the
   * Iterator follows the same restrictions as a call to {@link #getNextRow}.
   * See {@link #findFirstRow(Map)} for details on the rowPattern.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Row> rowMatchIterable(
      Map<String,?> rowPattern);

  /**
   * Returns an Iterable whose iterator() method calls {@link #beforeFirst} on
   * this table and returns a modifiable Iterator which will iterate through
   * all the rows of this table which match the given row pattern, returning
   * only the given columns.  Use of the Iterator follows the same
   * restrictions as a call to {@link #getNextRow}.  See {@link
   * #findFirstRow(Map)} for details on the rowPattern.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   */
  public Iterable<Row> rowMatchIterable(
      Collection<String> columnNames,
      Map<String,?> rowPattern);

  /**
   * Delete the current row.
   * <p/>
   * Note, re-deleting an already deleted row is allowed (it does nothing).
   * @throws IllegalStateException if the current row is not valid (at
   *         beginning or end of table)
   */
  public void deleteCurrentRow() throws IOException;

  /**
   * Update the current row.
   * @throws IllegalStateException if the current row is not valid (at
   *         beginning or end of table), or deleted.
   */
  public void updateCurrentRow(Object... row) throws IOException;

  /**
   * Update the current row.
   * @throws IllegalStateException if the current row is not valid (at
   *         beginning or end of table), or deleted.
   */
  public void updateCurrentRowFromMap(Map<String,?> row) throws IOException;

  /**
   * Moves to the next row in the table and returns it.
   * @return The next row in this table (Column name -> Column value), or
   *         {@code null} if no next row is found
   */
  public Row getNextRow() throws IOException;

  /**
   * Moves to the next row in the table and returns it.
   * @param columnNames Only column names in this collection will be returned
   * @return The next row in this table (Column name -> Column value), or
   *         {@code null} if no next row is found
   */
  public Row getNextRow(Collection<String> columnNames) 
    throws IOException;

  /**
   * Moves to the previous row in the table and returns it.
   * @return The previous row in this table (Column name -> Column value), or
   *         {@code null} if no previous row is found
   */
  public Row getPreviousRow() throws IOException;

  /**
   * Moves to the previous row in the table and returns it.
   * @param columnNames Only column names in this collection will be returned
   * @return The previous row in this table (Column name -> Column value), or
   *         {@code null} if no previous row is found
   */
  public Row getPreviousRow(Collection<String> columnNames) 
    throws IOException;

  /**
   * Moves to the next row as defined by this cursor.
   * @return {@code true} if a valid next row was found, {@code false}
   *         otherwise
   */
  public boolean moveToNextRow() throws IOException;

  /**
   * Moves to the previous row as defined by this cursor.
   * @return {@code true} if a valid previous row was found, {@code false}
   *         otherwise
   */
  public boolean moveToPreviousRow() throws IOException;

  /**
   * Moves to the first row (as defined by the cursor) where the given column
   * has the given value.  This may be more efficient on some cursors than
   * others.  If a match is not found (or an exception is thrown), the cursor
   * is restored to its previous state.
   * <p>
   * Warning, this method <i>always</i> starts searching from the beginning of
   * the Table (you cannot use it to find successive matches).
   *
   * @param columnPattern column from the table for this cursor which is being
   *                      matched by the valuePattern
   * @param valuePattern value which is equal to the corresponding value in
   *                     the matched row
   * @return {@code true} if a valid row was found with the given value,
   *         {@code false} if no row was found
   */
  public boolean findFirstRow(Column columnPattern, Object valuePattern)
    throws IOException;

  /**
   * Moves to the next row (as defined by the cursor) where the given column
   * has the given value.  This may be more efficient on some cursors than
   * others.  If a match is not found (or an exception is thrown), the cursor
   * is restored to its previous state.
   *
   * @param columnPattern column from the table for this cursor which is being
   *                      matched by the valuePattern
   * @param valuePattern value which is equal to the corresponding value in
   *                     the matched row
   * @return {@code true} if a valid row was found with the given value,
   *         {@code false} if no row was found
   */
  public boolean findNextRow(Column columnPattern, Object valuePattern)
    throws IOException;

  /**
   * Moves to the first row (as defined by the cursor) where the given columns
   * have the given values.  This may be more efficient on some cursors than
   * others.  If a match is not found (or an exception is thrown), the cursor
   * is restored to its previous state.
   * <p>
   * Warning, this method <i>always</i> starts searching from the beginning of
   * the Table (you cannot use it to find successive matches).
   *
   * @param rowPattern column names and values which must be equal to the
   *                   corresponding values in the matched row
   * @return {@code true} if a valid row was found with the given values,
   *         {@code false} if no row was found
   */
  public boolean findFirstRow(Map<String,?> rowPattern) throws IOException;

  /**
   * Moves to the next row (as defined by the cursor) where the given columns
   * have the given values.  This may be more efficient on some cursors than
   * others.  If a match is not found (or an exception is thrown), the cursor
   * is restored to its previous state.
   *
   * @param rowPattern column names and values which must be equal to the
   *                   corresponding values in the matched row
   * @return {@code true} if a valid row was found with the given values,
   *         {@code false} if no row was found
   */
  public boolean findNextRow(Map<String,?> rowPattern) throws IOException;

  /**
   * Returns {@code true} if the current row matches the given pattern.
   * @param columnPattern column from the table for this cursor which is being
   *                      matched by the valuePattern
   * @param valuePattern value which is tested for equality with the
   *                     corresponding value in the current row
   */
  public boolean currentRowMatches(Column columnPattern, Object valuePattern)
    throws IOException;

  /**
   * Returns {@code true} if the current row matches the given pattern.
   * @param rowPattern column names and values which must be equal to the
   *                   corresponding values in the current row
   */
  public boolean currentRowMatches(Map<String,?> rowPattern) throws IOException;

  /**
   * Moves forward as many rows as possible up to the given number of rows.
   * @return the number of rows moved.
   */
  public int moveNextRows(int numRows) throws IOException;

  /**
   * Moves backward as many rows as possible up to the given number of rows.
   * @return the number of rows moved.
   */
  public int movePreviousRows(int numRows) throws IOException;

  /**
   * Returns the current row in this cursor (Column name -> Column value).
   */
  public Row getCurrentRow() throws IOException;

  /**
   * Returns the current row in this cursor (Column name -> Column value).
   * @param columnNames Only column names in this collection will be returned
   */
  public Row getCurrentRow(Collection<String> columnNames)
    throws IOException;

  /**
   * Returns the given column from the current row.
   */
  public Object getCurrentRowValue(Column column) throws IOException;

  /**
   * Updates a single value in the current row.
   * @throws IllegalStateException if the current row is not valid (at
   *         beginning or end of table), or deleted.
   */
  public void setCurrentRowValue(Column column, Object value)
    throws IOException;

  /**
   * Identifier for a cursor.  Will be equal to any other cursor of the same
   * type for the same table.  Primarily used to check the validity of a
   * Savepoint.
   */
  public interface Id
  {    
  }

  /**
   * Value object which maintains the current position of the cursor.
   */
  public interface Position
  {    
    /**
     * Returns the unique RowId of the position of the cursor.
     */
    public RowId getRowId();
  }

  /**
   * Value object which represents a complete save state of the cursor.
   * Savepoints are created by calling {@link Cursor#getSavepoint} and used by
   * calling {@link Cursor#restoreSavepoint} to return the the cursor state at
   * the time the Savepoint was created.
   */
  public interface Savepoint
  {
    public Id getCursorId();

    public Position getCurrentPosition();
  }

}
