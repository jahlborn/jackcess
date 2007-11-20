// Copyright (c) 2007 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import org.apache.commons.lang.builder.CompareToBuilder;


/**
 * Uniquely identifies a row of data within the access database.
 *
 * @author james
 */
public class RowId implements Comparable<RowId>
{
  public static final int INVALID_ROW_NUMBER = -1;

  private final int _pageNumber;
  private final int _rowNumber;
  
  /**
   * Creates a new <code>RowId</code> instance.
   *
   */
  public RowId(int pageNumber,int rowNumber) {
    _pageNumber = pageNumber;
    _rowNumber = rowNumber;
  }

  public int getPageNumber() {
    return _pageNumber;
  }

  public int getRowNumber() {
    return _rowNumber;
  }

  public boolean isValidRow() {
    return(getRowNumber() != INVALID_ROW_NUMBER);
  }
  
  public int compareTo(RowId other) {
      return new CompareToBuilder()
        .append(getPageNumber(), other.getPageNumber())
        .append(getRowNumber(), other.getRowNumber())
        .toComparison();
  }

  @Override
  public int hashCode() {
    return getPageNumber() ^ getRowNumber();
  }

  @Override
  public boolean equals(Object o) {
    return ((this == o) ||
            ((o != null) && (getClass() == o.getClass()) &&
             (getPageNumber() == ((RowId)o).getPageNumber()) &&
             (getRowNumber() == ((RowId)o).getRowNumber())));
  }
  
  @Override
  public String toString() {
    return getPageNumber() + ":" + getRowNumber();
  }
  
}
