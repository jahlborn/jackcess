// Copyright (c) 2006 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Interface which allows customization of the behavior of the
 * <code>Database<</code> import/copy methods.
 *
 * @author James Ahlborn
 */
public interface ImportFilter {

  /**
   * The columns that should be used to create the imported table.
   * @param destColumns the columns as determined by the import code, may be
   *                    directly modified and returned
   * @param srcColumns the sql metadata, only available if importing from a
   *                   JDBC source
   * @return the columns to use when creating the import table
   */
  public List<Column> filterColumns(List<Column> destColumns,
                                    ResultSetMetaData srcColumns)
     throws SQLException, IOException;

  /**
   * The desired values for the row.
   * @param row the row data as determined by the import code, may be directly
   *            modified
   * @return the row data as it should be written to the import table
   */
  public Object[] filterRow(Object[] row)
    throws SQLException, IOException;

}
