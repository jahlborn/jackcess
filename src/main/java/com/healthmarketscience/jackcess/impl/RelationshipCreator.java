/*
Copyright (c) 2016 James Ahlborn

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.jackcess.ConstraintViolationException;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.RelationshipBuilder;
import com.healthmarketscience.jackcess.Row;

/**
 * Helper class used to maintain state during relationship creation.
 *
 * @author James Ahlborn
 */
public class RelationshipCreator extends DBMutator
{
  private final static int CASCADE_FLAGS =
    RelationshipImpl.CASCADE_DELETES_FLAG |
    RelationshipImpl.CASCADE_UPDATES_FLAG |
    RelationshipImpl.CASCADE_NULL_FLAG;

  // for the purposes of choosing a backing index for a foreign key, there are
  // certain index flags that can be ignored (we don't care how they are set)
  private final static byte IGNORED_PRIMARY_INDEX_FLAGS = 
    IndexData.IGNORE_NULLS_INDEX_FLAG | IndexData.REQUIRED_INDEX_FLAG;
  private final static byte IGNORED_SECONDARY_INDEX_FLAGS = 
    IGNORED_PRIMARY_INDEX_FLAGS | IndexData.UNIQUE_INDEX_FLAG;
  
  private TableImpl _primaryTable;
  private TableImpl _secondaryTable;
  private RelationshipBuilder _relationship;
  private List<ColumnImpl> _primaryCols; 
  private List<ColumnImpl> _secondaryCols;
  private int _flags;
  private String _name;
    
  public RelationshipCreator(DatabaseImpl database) 
  {
    super(database);
  }

  public TableImpl getPrimaryTable() {
    return _primaryTable;
  }

  public TableImpl getSecondaryTable() {
    return _secondaryTable;
  }

  public boolean hasReferentialIntegrity() {
    return _relationship.hasReferentialIntegrity();
  }

  public RelationshipImpl createRelationshipImpl(String name) {
    _name = name;
    RelationshipImpl newRel = new RelationshipImpl(
        name, _primaryTable, _secondaryTable, _flags, 
        _primaryCols, _secondaryCols);
    return newRel;
  }

  /**
   * Creates the relationship in the database.
   * @usage _advanced_method_
   */
  public RelationshipImpl createRelationship(RelationshipBuilder relationship) 
    throws IOException 
  {
    _relationship = relationship;
    
    validate();

    _flags = _relationship.getFlags();
    // need to determine the one-to-one flag on our own
    if(isOneToOne()) {
      _flags |= RelationshipImpl.ONE_TO_ONE_FLAG;
    }

    getPageChannel().startExclusiveWrite();
    try {

      RelationshipImpl newRel = getDatabase().writeRelationship(this);

      if(hasReferentialIntegrity()) {
        addPrimaryIndex();
        addSecondaryIndex();
      }

      return newRel;

    } finally {
      getPageChannel().finishWrite();
    }
  }

  private void addPrimaryIndex() throws IOException {
    TableUpdater updater = new TableUpdater(_primaryTable);
    updater.setForeignKey(createFKReference(true));
    updater.addIndex(createPrimaryIndex(), true, 
                     IGNORED_PRIMARY_INDEX_FLAGS, (byte)0);
  }

  private void addSecondaryIndex() throws IOException {
    TableUpdater updater = new TableUpdater(_secondaryTable);
    updater.setForeignKey(createFKReference(false));
    updater.addIndex(createSecondaryIndex(), true, 
                     IGNORED_SECONDARY_INDEX_FLAGS, (byte)0);
  }

  private IndexImpl.ForeignKeyReference createFKReference(boolean isPrimary) {
    byte tableType = 0;
    int otherTableNum = 0;
    int otherIdxNum = 0;
    if(isPrimary) {
      tableType = IndexImpl.FK_PRIMARY_TABLE_TYPE;
      otherTableNum = _secondaryTable.getTableDefPageNumber();
      // we create the primary index first, so the secondary index does not
      // exist yet
      otherIdxNum = _secondaryTable.getLogicalIndexCount();
    } else {
      tableType = IndexImpl.FK_SECONDARY_TABLE_TYPE;
      otherTableNum = _primaryTable.getTableDefPageNumber();
      // at this point, we've already created the primary index, it's the last
      // one on the primary table
      otherIdxNum = _primaryTable.getLogicalIndexCount() - 1;
    }
    boolean cascadeUpdates = ((_flags & RelationshipImpl.CASCADE_UPDATES_FLAG) != 0);
    boolean cascadeDeletes = ((_flags & RelationshipImpl.CASCADE_DELETES_FLAG) != 0);
    boolean cascadeNull = ((_flags & RelationshipImpl.CASCADE_NULL_FLAG) != 0);

    return new IndexImpl.ForeignKeyReference(
        tableType, otherIdxNum, otherTableNum, cascadeUpdates, cascadeDeletes, 
        cascadeNull);
  }

  private void validate() throws IOException {

    _primaryTable = getDatabase().getTable(_relationship.getFromTable());
    _secondaryTable = getDatabase().getTable(_relationship.getToTable());
    
    if((_primaryTable == null) || (_secondaryTable == null)) {
      throw new IllegalArgumentException(withErrorContext(
          "Two valid tables are required in relationship"));
    }

    _primaryCols = getColumns(_primaryTable, _relationship.getFromColumns());
    _secondaryCols = getColumns(_secondaryTable, _relationship.getToColumns());
    
    if((_primaryCols == null) || (_primaryCols.isEmpty()) || 
       (_secondaryCols == null) || (_secondaryCols.isEmpty())) {
      throw new IllegalArgumentException(withErrorContext(
          "Missing columns in relationship"));
    }

    if(_primaryCols.size() != _secondaryCols.size()) {
      throw new IllegalArgumentException(withErrorContext(
          "Must have same number of columns on each side of relationship"));
    }

    for(int i = 0; i < _primaryCols.size(); ++i) {
      ColumnImpl pcol = _primaryCols.get(i);
      ColumnImpl scol = _primaryCols.get(i);

      if(pcol.getType() != scol.getType()) {
        throw new IllegalArgumentException(withErrorContext(
            "Matched columns must have the same data type"));
      }
    }

    if(!hasReferentialIntegrity()) {

      if((_relationship.getFlags() & CASCADE_FLAGS) != 0) {
        throw new IllegalArgumentException(withErrorContext(
            "Cascade flags cannot be enabled if referential integrity is not enforced"));
      }
      
      return;
    }

    // for now, we will require the unique index on the primary table (just
    // like access does).  we could just create it auto-magically...
    IndexImpl primaryIdx = getPrimaryUniqueIndex();
    if(primaryIdx == null) {
      throw new IllegalArgumentException(withErrorContext(
          "Missing unique index on primary table required to enforce integrity"));
    }

    // while relationships can have "dupe" columns, indexes (and therefore
    // integrity enforced relationships) cannot
    if((new HashSet<String>(getColumnNames(_primaryCols)).size() != 
        _primaryCols.size()) ||
       (new HashSet<String>(getColumnNames(_secondaryCols)).size() != 
        _secondaryCols.size())) {
      throw new IllegalArgumentException(withErrorContext(
          "Cannot have duplicate columns in an integrity enforced relationship"));
    }
    
    // TODO: future, check for enforce cycles?

    // check referential integrity
    IndexCursor primaryCursor = primaryIdx.newCursor().toIndexCursor();
    Object[] entryValues = new Object[_secondaryCols.size()];
    for(Row row : _secondaryTable.newCursor().toCursor()
          .newIterable().addColumns(_secondaryCols)) {
      // grab the secondary table values
      boolean hasValues = false;
      for(int i = 0; i < _secondaryCols.size(); ++i) {
        entryValues[i] = _secondaryCols.get(i).getRowValue(row);
        hasValues = hasValues || (entryValues[i] != null);
      }

      if(!hasValues) {
        // we can ignore null entries
        continue;
      }

      // check that they exist in the primary table
      if(!primaryCursor.findFirstRowByEntry(entryValues)) {
        throw new ConstraintViolationException(withErrorContext(
            "Integrity constraint violation found for relationship"));
      }
    }

  }

  private IndexBuilder createPrimaryIndex() {
    String name = createPrimaryIndexName();
    return createIndex(name, _primaryCols)
      .setUnique()
      .setType(IndexImpl.FOREIGN_KEY_INDEX_TYPE);
  }
  
  private IndexBuilder createSecondaryIndex() {
    // secondary index uses relationship name
    return createIndex(_name, _secondaryCols)
      .setType(IndexImpl.FOREIGN_KEY_INDEX_TYPE);
  }
  
  private static IndexBuilder createIndex(String name, List<ColumnImpl> cols) {
    IndexBuilder idx = new IndexBuilder(name);
    for(ColumnImpl col : cols) {
      idx.addColumns(col.getName());
    }
    return idx;
  }

  private String createPrimaryIndexName() {
    Set<String> idxNames = TableUpdater.getIndexNames(_primaryTable, null);

    // primary naming scheme: ".rB", .rC", ".rD", "rE" ...
    String baseName = ".r";
    String suffix = "B";

    while(true) {
      String idxName = baseName + suffix;
      if(!idxNames.contains(DatabaseImpl.toLookupName(idxName))) {
        return idxName;
      }

      char c = (char)(suffix.charAt(0) + 1);
      if(c == '[') {
        c = 'a';
      }
      suffix = "" + c;
    }    
  }

  private static List<ColumnImpl> getColumns(TableImpl table, 
                                             List<String> colNames) {
    List<ColumnImpl> cols = new ArrayList<ColumnImpl>();
    for(String colName : colNames) {
      cols.add(table.getColumn(colName));
    }
    return cols;
  }

  private static List<String> getColumnNames(List<ColumnImpl> cols) {
    List<String> colNames = new ArrayList<String>();
    for(ColumnImpl col : cols) {
      colNames.add(col.getName());
    }
    return colNames;
  }

  private boolean isOneToOne() {
    // a relationship is one to one if the two sides of the relationship have
    // unique indexes on the relevant columns
    if(getPrimaryUniqueIndex() == null) {
      return false;
    }
    IndexImpl idx = _secondaryTable.findIndexForColumns(
        getColumnNames(_secondaryCols), true);
    return (idx != null);
  }

  private IndexImpl getPrimaryUniqueIndex() {
    return _primaryTable.findIndexForColumns(getColumnNames(_primaryCols), true);
  }

  private static String getTableErrorContext(
      TableImpl table, List<ColumnImpl> cols,
      String tableName, Collection<String> colNames) {
    if(table != null) {
      tableName = table.getName();
    }
    if(cols != null) {
      colNames = getColumnNames(cols);
    }

    return CustomToStringStyle.valueBuilder(tableName)
      .append(null, cols)
      .toString();
  }
  
  private String withErrorContext(String msg) {
    return msg + "(Rel=" +
      getTableErrorContext(_primaryTable, _primaryCols, 
                           _relationship.getFromTable(),
                           _relationship.getFromColumns()) + " -> " +
      getTableErrorContext(_secondaryTable, _secondaryCols, 
                           _relationship.getToTable(),
                           _relationship.getToColumns()) + ")";
  }
}
