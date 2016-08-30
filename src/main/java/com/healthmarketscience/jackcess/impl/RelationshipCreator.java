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
 *
 * @author James Ahlborn
 */
public class RelationshipCreator extends DBMutator
{
  private final static int CASCADE_FLAGS =
    RelationshipImpl.CASCADE_DELETES_FLAG |
    RelationshipImpl.CASCADE_UPDATES_FLAG |
    RelationshipImpl.CASCADE_NULL_FLAG;
    
  
  private TableImpl _primaryTable;
  private TableImpl _secondaryTable;
  private RelationshipBuilder _relationship;
  private List<ColumnImpl> _primaryCols; 
  private List<ColumnImpl> _secondaryCols;
  private int _flags;
    
  // - primary table must have unique index
  // - primary table index name ".rC", ".rD"...
  // - secondary index name "<PTable><STable>"
  // - add <name>1, <name>2 after names to make unique (index names and
  //   relationship names)
  // - enforcing rel integrity can't have dupe cols 

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

  public RelationshipImpl createRelationshipImpl(String name) {
    RelationshipImpl newRel = new RelationshipImpl(
        name, _secondaryTable, _primaryTable, _flags, 
        _secondaryCols, _primaryCols);
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

      if(_relationship.hasReferentialIntegrity()) {
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
    updater.addIndex(createPrimaryIndex(), true);
  }

  private void addSecondaryIndex() throws IOException {
    TableUpdater updater = new TableUpdater(_secondaryTable);
    updater.setForeignKey(createFKReference(false));
    updater.addIndex(createSecondaryIndex(), true);
  }

  private IndexImpl.ForeignKeyReference createFKReference(boolean isPrimary) {
    byte tableType = 0;
    int otherTableNum = 0;
    int otherIdxNum = 0;
    if(isPrimary) {
      tableType = IndexImpl.PRIMARY_TABLE_TYPE;
      otherTableNum = _secondaryTable.getTableDefPageNumber();
      otherIdxNum = _secondaryTable.getLogicalIndexCount();
    } else {
      tableType = IndexImpl.SECONDARY_TABLE_TYPE;
      otherTableNum = _primaryTable.getTableDefPageNumber();
      otherIdxNum = _primaryTable.getLogicalIndexCount();
    }
    boolean cascadeUpdates = ((_flags & RelationshipImpl.CASCADE_UPDATES_FLAG) != 0);
    boolean cascadeDeletes = ((_flags & RelationshipImpl.CASCADE_DELETES_FLAG) != 0);
    boolean cascadeNull = ((_flags & RelationshipImpl.CASCADE_NULL_FLAG) != 0);

    return new IndexImpl.ForeignKeyReference(
        tableType, otherIdxNum, otherTableNum, cascadeUpdates, cascadeDeletes, 
        cascadeNull);
  }

  private void validate() throws IOException {

    _primaryTable = getDatabase().getTable(_relationship.getToTable());
    _secondaryTable = getDatabase().getTable(_relationship.getFromTable());
    
    if((_primaryTable == null) || (_secondaryTable == null)) {
      throw new IllegalArgumentException(withErrorContext(
          "Two valid tables are required in relationship"));
    }

    _primaryCols = getColumns(_primaryTable, _relationship.getToColumns());
    _secondaryCols = getColumns(_secondaryTable, _relationship.getFromColumns());
    
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

    if(!_relationship.hasReferentialIntegrity()) {

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
      // grab the from table values
      boolean hasValues = false;
      for(int i = 0; i < _secondaryCols.size(); ++i) {
        entryValues[i] = _secondaryCols.get(i).getRowValue(row);
        hasValues = hasValues || (entryValues[i] != null);
      }

      if(!hasValues) {
        // we can ignore null entries
        continue;
      }

      if(!primaryCursor.findFirstRowByEntry(entryValues)) {
        throw new ConstraintViolationException(withErrorContext(
            "Integrity constraint violation found for relationship"));
      }
    }

  }

  private IndexBuilder createPrimaryIndex() {
    String name = getUniqueIndexName(_primaryTable);
    // FIXME?
    return createIndex(name, _primaryCols)
      .setUnique()
      .setType(IndexImpl.FOREIGN_KEY_INDEX_TYPE);
  }
  
  private IndexBuilder createSecondaryIndex() {
    String name = getUniqueIndexName(_secondaryTable);
    // FIXME?

    return createIndex(name, _secondaryCols)
      .setType(IndexImpl.FOREIGN_KEY_INDEX_TYPE);
  }
  
  private static IndexBuilder createIndex(String name, List<ColumnImpl> cols) {
    IndexBuilder idx = new IndexBuilder(name);
    for(ColumnImpl col : cols) {
      idx.addColumns(col.getName());
    }
    return idx;
  }

  private String getUniqueIndexName(TableImpl table) {
    Set<String> idxNames = TableUpdater.getIndexNames(table, null);

    boolean isPrimary = (table == _primaryTable);
    String baseName = null;
    String suffix = null;
    if(isPrimary) {
      // primary naming scheme: ".rC", ".rD", "rE" ...
      baseName = ".r";
      suffix = "C";
    } else {
      // secondary naming scheme: "<t1><t2>", "<t1><t2>1", "<t1><t2>2"
      baseName = _primaryTable.getName() + _secondaryTable.getName();
      suffix = "";
    }

    int count = 0;
    while(true) {
      String idxName = baseName + suffix;
      if(!idxNames.contains(idxName.toUpperCase())) {
        return idxName;
      }

      ++count;
      if(isPrimary) {
        char c = (char)(suffix.charAt(0) + 1);
        if(c == '[') {
          c = 'a';
        }
        suffix = "" + c;
      } else {
        suffix = "" + count;
      }      
    }    

    // FIXME, truncate to max index name length
  }

  private static List<ColumnImpl> getColumns(TableImpl table, List<String> colNames) {
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
                           _relationship.getToTable(),
                           _relationship.getToColumns()) + " <- " +
      getTableErrorContext(_secondaryTable, _secondaryCols, 
                           _relationship.getFromTable(),
                           _relationship.getFromColumns()) + ")";
  }
}
