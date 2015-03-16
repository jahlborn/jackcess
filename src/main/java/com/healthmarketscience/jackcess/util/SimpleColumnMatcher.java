/*
Copyright (c) 2010 James Ahlborn

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

package com.healthmarketscience.jackcess.util;

import java.io.IOException;
import java.util.Arrays;

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import org.apache.commons.lang.ObjectUtils;

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
        Object internalV1 = ColumnImpl.toInternalValue(dataType, value1);
        Object internalV2 = ColumnImpl.toInternalValue(dataType, value2);
        
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
    return (ObjectUtils.equals(o1, o2) || 
            ((o1 instanceof byte[]) && (o2 instanceof byte[]) &&
             Arrays.equals((byte[])o1, (byte[])o2)));
  }

}
