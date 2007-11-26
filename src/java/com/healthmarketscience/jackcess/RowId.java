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
  /** special page number which will sort before any other valid page
      number */
  public static final int FIRST_PAGE_NUMBER = -1;
  /** special page number which will sort after any other valid page
      number */
  public static final int LAST_PAGE_NUMBER = -2;

  /** special row number representing an invalid row number */
  public static final int INVALID_ROW_NUMBER = -1;

  /** special rowId which will sort before any other valid rowId */
  public static final RowId FIRST_ROW_ID = new RowId(
      FIRST_PAGE_NUMBER, INVALID_ROW_NUMBER);

  /** special rowId which will sort after any other valid rowId */
  public static final RowId LAST_ROW_ID = new RowId(
      LAST_PAGE_NUMBER, INVALID_ROW_NUMBER);

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

  /**
   * Returns {@code true} if this rowId potentially represents an actual row
   * of data, {@code false} otherwise.
   */
  public boolean isValid() {
    return((getRowNumber() >= 0) && (getPageNumber() >= 0));
  }

  /**
   * Returns the page number comparable as a normal integer, handling
   * "special" page numbers (e.g. first, last).
   */
  private int getComparablePageNumber() {
    // using max int is valid for last page number because it is way out of
    // range for any valid access database file
    return((getPageNumber() >= FIRST_PAGE_NUMBER) ?
           getPageNumber() : Integer.MAX_VALUE);
  }
  
  public int compareTo(RowId other) {
    return new CompareToBuilder()
      .append(getComparablePageNumber(), other.getComparablePageNumber())
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
