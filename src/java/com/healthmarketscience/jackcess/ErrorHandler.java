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

/**
 * Handler for errors encountered while reading a column of row data from a
 * Table.  An instance of this class may be configured at the Database, Table,
 * or Cursor level to customize error handling as desired.  The default
 * instance used is {@link Database#DEFAULT_ERROR_HANDLER}, which just
 * rethrows any exceptions encountered.
 * 
 * @author James Ahlborn
 */
public interface ErrorHandler 
{

  /**
   * Handles an error encountered while reading a column of data from a Table
   * row.  Handler may either throw an exception (which will be propagated
   * back to the caller) or return a replacement for this row's column value
   * (in which case the row will continue to be read normally).
   *
   * @param column the info for the column being read
   * @param columnData the actual column data for the column being read (which
   *                   may be {@code null} depending on when the exception
   *                   was thrown during the reading process)
   * @param rowState the current row state for the caller
   * @param error the error that was encountered
   *
   * @return replacement for this row's column
   */
  public Object handleRowError(Column column,
                               byte[] columnData,
                               Table.RowState rowState,
                               Exception error)
    throws IOException;

}
