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
import java.util.List;
import com.healthmarketscience.jackcess.RelationshipBuilder;

/**
 *
 * @author James Ahlborn
 */
public class RelationshipCreator extends DBMutator
{
  private TableImpl _primaryTable;
  private TableImpl _secondaryTable;
  private RelationshipBuilder _relationship;
  private List<ColumnImpl> _primaryCols; 
  private List<ColumnImpl> _secondaryCols;
  private int _flags;

  public RelationshipCreator(DatabaseImpl database) 
  {
    super(database);
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

    // FIXME 
    return null;
  }

  private void validate() throws IOException {

    if((_primaryTable == null) || (_secondaryTable == null)) {
      throw new IllegalArgumentException(
          "Two tables are required in relationship");
    }
    if(_primaryTable.getDatabase() != _secondaryTable.getDatabase()) {
      throw new IllegalArgumentException("Tables are not from same database");
    }

    if((_primaryCols == null) || (_primaryCols.isEmpty()) || 
       (_secondaryCols == null) || (_secondaryCols.isEmpty())) {
      throw new IllegalArgumentException("Missing columns in relationship");
    }

    if(_primaryCols.size() != _secondaryCols.size()) {
      throw new IllegalArgumentException(
          "Must have same number of columns on each side of relationship");
    }

    for(int i = 0; i < _primaryCols.size(); ++i) {
      ColumnImpl pcol = _primaryCols.get(i);
      ColumnImpl scol = _primaryCols.get(i);

      if(pcol.getType() != scol.getType()) {
        throw new IllegalArgumentException(
            "Matched columns must have the same data type");
      }
    }

    if(!_relationship.hasReferentialIntegrity()) {
      return;
    }

    

    // - same number cols
    // - cols come from right tables, tables from right db
    // - (cols can be duped in index)
    // - cols have same data types
    // - if enforce, require unique index on primary (auto-create?), index on secondary
    // - advanced, check for enforce cycles?
    // - index must be ascending

    // FIXME
  }
}
