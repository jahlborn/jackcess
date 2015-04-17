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
 * Simple concrete implementation of ImportFilter which just returns the given
 * values.
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public class SimpleImportFilter implements ImportFilter {

  public static final SimpleImportFilter INSTANCE = new SimpleImportFilter();
  
  public SimpleImportFilter() {
  }

  public List<ColumnBuilder> filterColumns(List<ColumnBuilder> destColumns,
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
