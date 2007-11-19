// Copyright (c) 2007 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

/**
 * Describe class Cursor here.
 *
 *
 * @author james
 */
public class Cursor {

  /** owning table */
  private final Table _table;
//   /** Number of the current row in a data page */
//   private int _currentRowInPage = INVALID_ROW_NUMBER;
//   /** Number of rows left to be read on the current page */
//   private short _rowsLeftOnPage = 0;
//   /** State used for reading the table rows */
//   private RowState _rowState;
//   /** Iterator over the pages that this table owns */
//   private UsageMap.PageIterator _ownedPagesIterator;
  
  /**
   * Creates a new <code>Cursor</code> instance.
   *
   */
  public Cursor(Table table) {
    _table = table;
  }

}
