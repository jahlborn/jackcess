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

package com.healthmarketscience.jackcess;

import java.io.IOException;

/**
 * Concrete implementation of ColumnMatcher which tests textual columns
 * case-insensitively ({@link DataType#TEXT} and {@link DataType#MEMO}), and
 * all other columns using simple equality.
 * 
 * @author James Ahlborn
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
      CharSequence cs1 = Column.toCharSequence(value1);
      CharSequence cs2 = Column.toCharSequence(value2);

      return((cs1 == cs2) ||
             ((cs1 != null) && (cs2 != null) &&
              cs1.toString().equalsIgnoreCase(cs2.toString())));
    } catch(IOException e) {
      throw new IllegalStateException("Could not read column " + columnName 
                                      + " value", e);
    }
  }

}
