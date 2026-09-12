/*
Copyright (c) 2018 James Ahlborn

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

import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.Identifier;
import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public abstract class RowEvalContext extends BaseEvalContext
{
  private Object[] _row;

  public RowEvalContext(DatabaseImpl db) {
    super(db.getEvalContext());
  }

  protected void setRow(Object[] row) {
    _row = row;
  }

  protected void reset() {
    _row = null;
  }

  @Override
  public Value getIdentifierValue(Identifier identifier) {

    TableImpl table = getTable();

    // we only support getting column values in this table from the current
    // row
    if(!table.isThisTable(identifier) ||
       (identifier.getPropertyName() != null)) {
      throw new EvalException("Cannot access fields outside this table for " +
                              identifier);
    }

    ColumnImpl col = table.getColumn(identifier.getObjectName());

    Object val = col.getRowValue(_row);

    return toValue(val, col.getType());
  }

  protected abstract TableImpl getTable();
}
