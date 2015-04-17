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

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.RuntimeIOException;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

/**
 * Concrete implementation of ColumnMatcher which tests textual columns
 * case-insensitively ({@link DataType#TEXT} and {@link DataType#MEMO}), and
 * all other columns using simple equality.
 * 
 * @author James Ahlborn
 * @usage _general_class_
 */
public class CaseInsensitiveColumnMatcher implements ColumnMatcher {

  public static final CaseInsensitiveColumnMatcher INSTANCE = 
    new CaseInsensitiveColumnMatcher();
  

  public CaseInsensitiveColumnMatcher() {
  }

  public boolean matches(Table table, String columnName, Object value1,
                         Object value2)
  {
    if(!table.getColumn(columnName).getType().isTextual()) {
      // use simple equality
      return SimpleColumnMatcher.INSTANCE.matches(table, columnName, 
                                                  value1, value2);
    }

    // convert both values to Strings and compare case-insensitively
    try {
      CharSequence cs1 = ColumnImpl.toCharSequence(value1);
      CharSequence cs2 = ColumnImpl.toCharSequence(value2);

      return((cs1 == cs2) ||
             ((cs1 != null) && (cs2 != null) &&
              cs1.toString().equalsIgnoreCase(cs2.toString())));
    } catch(IOException e) {
      throw new RuntimeIOException("Could not read column " + columnName 
                                   + " value", e);
    }
  }

}
