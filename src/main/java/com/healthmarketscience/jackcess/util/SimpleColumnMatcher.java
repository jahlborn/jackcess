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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;

/**
 * Simple concrete implementation of ColumnMatcher which tests for equality.
 * If initial comparison fails, attempts to coerce the values to a common type
 * for comparison.
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public class SimpleColumnMatcher implements ColumnMatcher {

  public static final SimpleColumnMatcher INSTANCE = new SimpleColumnMatcher();

  public SimpleColumnMatcher() {
  }

  public boolean matches(Table table, String columnName, Object value1,
                         Object value2)
  {
    if(equals(value1, value2)) {
      return true;
    }

    if((value1 != null) && (value2 != null) &&
       (value1.getClass() != value2.getClass())) {

      // the values aren't the same type, try coercing them to "internal"
      // values and try again
      DataType dataType = table.getColumn(columnName).getType();
      try {
        DatabaseImpl db = (DatabaseImpl)table.getDatabase();
        Object internalV1 = ColumnImpl.toInternalValue(dataType, value1, db);
        Object internalV2 = ColumnImpl.toInternalValue(dataType, value2, db);

        return equals(internalV1, internalV2);
      } catch(IOException e) {
        // ignored, just go with the original result
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if the two objects are equal, handling {@code null}
   * and objects of type {@code byte[]}.
   */
  private static boolean equals(Object o1, Object o2)
  {
    return (Objects.equals(o1, o2) ||
            ((o1 instanceof byte[]) && (o2 instanceof byte[]) &&
             Arrays.equals((byte[])o1, (byte[])o2)));
  }

}
