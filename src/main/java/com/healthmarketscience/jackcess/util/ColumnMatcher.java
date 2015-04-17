/*
Copyright (c) 2010 James Ahlborn

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

import com.healthmarketscience.jackcess.Table;

/**
 * Interface for handling comparisons between column values.
 *
 * @author James Ahlborn
 * @usage _intermediate_class_
 */
public interface ColumnMatcher 
{

  /**
   * Returns {@code true} if the given value1 should be considered a match for
   * the given value2 for the given column in the given table, {@code false}
   * otherwise.
   *
   * @param table the relevant table
   * @param columnName the name of the relevant column within the table
   * @param value1 the first value to match (may be {@code null})
   * @param value2 the second value to match (may be {@code null})
   */
  public boolean matches(Table table, String columnName, Object value1,
                         Object value2);
}
