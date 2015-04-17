/*
Copyright (c) 2007 Health Market Science, Inc.

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

package com.healthmarketscience.jackcess.util;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import com.healthmarketscience.jackcess.ColumnBuilder;

/**
 * Interface which allows customization of the behavior of the
 * {@link ImportUtil} import methods.
 *
 * @author James Ahlborn
 * @usage _general_class_
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
  public List<ColumnBuilder> filterColumns(List<ColumnBuilder> destColumns,
                                           ResultSetMetaData srcColumns)
     throws SQLException, IOException;

  /**
   * The desired values for the row.
   * @param row the row data as determined by the import code, may be directly
   *            modified
   * @return the row data as it should be written to the import table.  if
   *         {@code null}, the row will be skipped
   */
  public Object[] filterRow(Object[] row)
    throws SQLException, IOException;

}
