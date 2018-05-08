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

import java.io.IOException;

import com.healthmarketscience.jackcess.InvalidValueException;
import com.healthmarketscience.jackcess.impl.expr.Expressionator;

/**
 *
 * @author James Ahlborn
 */
public class RowValidatorEvalContext extends RowEvalContext
{
  private final TableImpl _table;
  private String _helpStr;

  public RowValidatorEvalContext(TableImpl table) {
    super(table.getDatabase());
    _table = table;
  }

  RowValidatorEvalContext setExpr(String exprStr, String helpStr) {
    setExpr(Expressionator.Type.RECORD_VALIDATOR, exprStr);
    _helpStr = helpStr;
    return this;
  }

  @Override
  protected TableImpl getTable() {
    return _table;
  }

  public void validate(Object[] row) throws IOException {
    try {
      setRow(row);
      Boolean result = (Boolean)eval();
      // FIXME how to handle null?
      if(!result) {
        String msg = ((_helpStr != null) ? _helpStr : "Invalid row");
        throw new InvalidValueException(withErrorContext(msg));
      }
    } finally {
      reset();
    }
  }

  @Override
  protected String withErrorContext(String msg) {
    return _table.withErrorContext(msg);
  }
}
