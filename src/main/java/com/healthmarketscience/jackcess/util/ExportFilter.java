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
import java.util.List;
import com.healthmarketscience.jackcess.Column;

/**
 * Interface which allows customization of the behavior of the
 * {@link ExportUtil} export methods.
 * 
 * @author James Ahlborn
 * @usage _general_class_
 */
public interface ExportFilter {

  /**
   * The columns that should be used to create the exported file.
   * 
   * @param columns
   *          the columns as determined by the export code, may be directly
   *          modified and returned
   * @return the columns to use when creating the export file
   */
  public List<Column> filterColumns(List<Column> columns) 
    throws IOException;

  /**
   * The desired values for the row.
   * 
   * @param row
   *          the row data as determined by the import code, may be directly
   *          modified
   * @return the row data as it should be written to the import table.  if
   *         {@code null}, the row will be skipped
   */
  public Object[] filterRow(Object[] row) throws IOException;

}
