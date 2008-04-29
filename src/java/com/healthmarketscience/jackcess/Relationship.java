/*
Copyright (c) 2008 Health Market Science, Inc.

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

You can contact Health Market Science at info@healthmarketscience.com
or at the following address:

Health Market Science
2700 Horizon Drive
Suite 200
King of Prussia, PA 19406
*/

package com.healthmarketscience.jackcess;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * Information about a relationship between two tables in the database.
 *
 * @author James Ahlborn
 */
public class Relationship {

  /** flag indicating one-to-one relationship */
  private static final int ONE_TO_ONE_FLAG =               0x00000001;
  /** flag indicating no referential integrity */
  private static final int NO_REFERENTIAL_INTEGRITY_FLAG = 0x00000002;
  /** flag indicating cascading updates (requires referential integrity) */
  private static final int CASCADE_UPDATES_FLAG =          0x00000100;
  /** flag indicating cascading deletes (requires referential integrity) */
  private static final int CASCADE_DELETES_FLAG =          0x00001000;
  /** flag indicating left outer join */
  private static final int LEFT_OUTER_JOIN_FLAG =          0x01000000;
  /** flag indicating right outer join */
  private static final int RIGHT_OUTER_JOIN_FLAG =         0x02000000;

  /** the name of this relationship */
  private final String _name;
  /** the "from" table in this relationship */
  private final Table _fromTable;
  /** the "to" table in this relationship */
  private final Table _toTable;
  /** the columns in the "from" table in this relationship (aligned w/
      toColumns list) */
  private List<Column> _toColumns;
  /** the columns in the "to" table in this relationship (aligned w/
      toColumns list) */
  private List<Column> _fromColumns;
  /** the various flags describing this relationship */
  private final int _flags;

  public Relationship(String name, Table fromTable, Table toTable, int flags,
                      int numCols)
  {
    _name = name;
    _fromTable = fromTable;
    _fromColumns = new ArrayList<Column>(
        Collections.nCopies(numCols, (Column)null));
    _toTable = toTable;
    _toColumns = new ArrayList<Column>(
        Collections.nCopies(numCols, (Column)null));
    _flags = flags;
  }

  public String getName() {
    return _name;
  }
  
  public Table getFromTable() {
    return _fromTable;
  }

  public List<Column> getFromColumns() {
    return _fromColumns;
  }

  public Table getToTable() {
    return _toTable;
  }

  public List<Column> getToColumns() {
    return _toColumns;
  }

  public int getFlags() {
    return _flags;
  }

  public boolean isOneToOne() {
    return hasFlag(ONE_TO_ONE_FLAG);
  }

  public boolean hasReferentialIntegrity() {
    return !hasFlag(NO_REFERENTIAL_INTEGRITY_FLAG);
  }

  public boolean cascadeUpdates() {
    return hasFlag(CASCADE_UPDATES_FLAG);
  }
  
  public boolean cascadeDeletes() {
    return hasFlag(CASCADE_DELETES_FLAG);
  }

  public boolean isLeftOuterJoin() {
    return hasFlag(LEFT_OUTER_JOIN_FLAG);
  }

  public boolean isRightOuterJoin() {
    return hasFlag(RIGHT_OUTER_JOIN_FLAG);
  }
  
  private boolean hasFlag(int flagMask) {
    return((getFlags() & flagMask) != 0);
  }

  @Override
  public String toString() {
    StringBuilder rtn = new StringBuilder();
    rtn.append("\tName: " + _name);
    rtn.append("\n\tFromTable: " + _fromTable.getName());
    rtn.append("\n\tFromColumns: " + _fromColumns);
    rtn.append("\n\tToTable: " + _toTable.getName());
    rtn.append("\n\tToColumns: " + _toColumns);
    rtn.append("\n\tFlags: " + Integer.toHexString(_flags));
    rtn.append("\n\n");
    return rtn.toString();
  }
  
}
