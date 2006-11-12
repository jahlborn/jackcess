// Copyright (c) 2006 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Simple concrete implementation of ImportFilter which just returns the given
 * values.
 *
 * @author James Ahlborn
 */
public class SimpleImportFilter implements ImportFilter {

  public static final SimpleImportFilter INSTANCE = new SimpleImportFilter();
  
  public SimpleImportFilter() {
  }
  
  public List<Column> filterColumns(List<Column> destColumns,
                                    ResultSetMetaData srcColumns)
     throws SQLException, IOException
  {
    return destColumns;
  }

  public Object[] filterRow(Object[] row)
    throws SQLException, IOException
  {
    return row;
  }

}
