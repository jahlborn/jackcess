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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of ErrorHandler which is useful for generating debug
 * information about bad row data (great for bug reports!).  After logging a
 * debug entry for the failed column, it will return some sort of replacement
 * value, see {@link ReplacementErrorHandler}.
 * 
 * @author James Ahlborn
 */
public class DebugErrorHandler extends ReplacementErrorHandler
{
  private static final Log LOG = LogFactory.getLog(DebugErrorHandler.class); 

  /**
   * Constructs a DebugErrorHandler which replaces all errored values with
   * {@code null}.
   */
  public DebugErrorHandler() {
  }

  /**
   * Constructs a DebugErrorHandler which replaces all errored values with the
   * given Object.
   */
  public DebugErrorHandler(Object replacement) {
    super(replacement);
  }

  @Override
  public Object handleRowError(Column column,
                               byte[] columnData,
                               Table.RowState rowState,
                               Exception error)
    throws IOException
  {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Failed reading column " + column + ", row " +
                rowState + ", bytes " +
                ((columnData != null) ?
                 ByteUtil.toHexString(columnData) : "null"),
                error);
    }

    return super.handleRowError(column, columnData, rowState, error);
  }

}
