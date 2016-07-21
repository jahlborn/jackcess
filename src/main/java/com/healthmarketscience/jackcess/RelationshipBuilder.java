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

package com.healthmarketscience.jackcess;

import java.io.IOException;

import com.healthmarketscience.jackcess.impl.RelationshipImpl;

/**
 *
 * @author James Ahlborn
 */
public class RelationshipBuilder 
{
  /** relationship flags (default to "don't enforce") */
  private int _flags = RelationshipImpl.NO_REFERENTIAL_INTEGRITY_FLAG;

  // - primary table must have unique index
  // - primary table index name ".rC", ".rD"...
  // - secondary index name "<PTable><STable>"
  // - add <name>1, <name>2 after names to make unique (index names and
  //   relationship names)

  public RelationshipBuilder() 
  {
  }

  public RelationshipBuilder setReferentialIntegrity() {
    return clearFlag(RelationshipImpl.NO_REFERENTIAL_INTEGRITY_FLAG);
  }

  public RelationshipBuilder setCascadeDeletes() {
    return setFlag(RelationshipImpl.CASCADE_DELETES_FLAG);
  }
  
  public RelationshipBuilder setCascadeUpdates() {
    return setFlag(RelationshipImpl.CASCADE_UPDATES_FLAG);
  }

  public RelationshipBuilder setCascadeNullOnDelete() {
    return setFlag(RelationshipImpl.CASCADE_NULL_FLAG);
  }  
  
  public RelationshipBuilder setJoinType(Relationship.JoinType joinType) {
    switch(joinType) {
    case INNER:
      // nothing to do
      break;
    case LEFT_OUTER:
      _flags |= RelationshipImpl.LEFT_OUTER_JOIN_FLAG;
      break;
    case RIGHT_OUTER:
      _flags |= RelationshipImpl.RIGHT_OUTER_JOIN_FLAG;
      break;
    default:
      throw new RuntimeException("unexpected join type " + joinType);
    }
    return this;
  }

  public boolean hasReferentialIntegrity() {
    return !hasFlag(RelationshipImpl.NO_REFERENTIAL_INTEGRITY_FLAG);
  }

  public int getFlags() {
    return _flags;
  }

  /**
   * Creates a new Relationship in the given Database with the currently
   * configured attributes.
   */
  public Relationship toRelationship(Database db)
    throws IOException
  {
    

    // FIXME writeme
    return null;
  }

  private RelationshipBuilder setFlag(int flagMask) {
    _flags |= flagMask;
    return this;
  }

  private RelationshipBuilder clearFlag(int flagMask) {
    _flags &= ~flagMask;
    return this;
  }

  private boolean hasFlag(int flagMask) {
    return((_flags & flagMask) != 0);
  }

}
