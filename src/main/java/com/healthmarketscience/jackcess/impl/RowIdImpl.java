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

package com.healthmarketscience.jackcess.impl;

import java.io.Serializable;

import com.healthmarketscience.jackcess.RowId;


/**
 * Uniquely identifies a row of data within the access database.
 *
 * @author James Ahlborn
 */
public class RowIdImpl implements RowId, Serializable
{
  private static final long serialVersionUID = 20131014L;  

  /** special page number which will sort before any other valid page
      number */
  public static final int FIRST_PAGE_NUMBER = -1;
  /** special page number which will sort after any other valid page
      number */
  public static final int LAST_PAGE_NUMBER = -2;

  /** special row number representing an invalid row number */
  public static final int INVALID_ROW_NUMBER = -1;

  /** type attributes for RowIds which simplify comparisons */
  public enum Type {
    /** comparable type indicating this RowId should always compare less than
        normal RowIds */
    ALWAYS_FIRST,
    /** comparable type indicating this RowId should always compare
        normally */
    NORMAL,
    /** comparable type indicating this RowId should always compare greater
        than normal RowIds */
    ALWAYS_LAST;
  }
  
  /** special rowId which will sort before any other valid rowId */
  public static final RowIdImpl FIRST_ROW_ID = new RowIdImpl(
      FIRST_PAGE_NUMBER, INVALID_ROW_NUMBER);

  /** special rowId which will sort after any other valid rowId */
  public static final RowIdImpl LAST_ROW_ID = new RowIdImpl(
      LAST_PAGE_NUMBER, INVALID_ROW_NUMBER);

  private final int _pageNumber;
  private final int _rowNumber;
  private final Type _type;
  
  /**
   * Creates a new <code>RowId</code> instance.
   *
   */
  public RowIdImpl(int pageNumber,int rowNumber) {
    _pageNumber = pageNumber;
    _rowNumber = rowNumber;
    _type = ((_pageNumber == FIRST_PAGE_NUMBER) ? Type.ALWAYS_FIRST :
             ((_pageNumber == LAST_PAGE_NUMBER) ? Type.ALWAYS_LAST :
              Type.NORMAL));
  }

  public int getPageNumber() {
    return _pageNumber;
  }

  public int getRowNumber() {
    return _rowNumber;
  }

  /**
   * Returns {@code true} if this rowId potentially represents an actual row
   * of data, {@code false} otherwise.
   */
  public boolean isValid() {
    return((getRowNumber() >= 0) && (getPageNumber() >= 0));
  }

  public Type getType() {
    return _type;
  }

  @Override
  public int compareTo(RowId other) {
    return compareTo((RowIdImpl)other);
  }

  public int compareTo(RowIdImpl other) {
    int compare = getType().compareTo(other.getType());
    if (compare == 0) {
      compare = Integer.compare(getPageNumber(), other.getPageNumber());
    }
    if (compare == 0) {
      compare = Integer.compare(getRowNumber(), other.getRowNumber());
    }
    return compare;
  }

  @Override
  public int hashCode() {
    return getPageNumber() ^ getRowNumber();
  }

  @Override
  public boolean equals(Object o) {
    return ((this == o) ||
            ((o != null) && (getClass() == o.getClass()) &&
             (getPageNumber() == ((RowIdImpl)o).getPageNumber()) &&
             (getRowNumber() == ((RowIdImpl)o).getRowNumber())));
  }
  
  @Override
  public String toString() {
    return getPageNumber() + ":" + getRowNumber();
  }
  
}
