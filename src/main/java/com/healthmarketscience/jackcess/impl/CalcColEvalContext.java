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

import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.expr.Expressionator;

/**
 *
 * @author James Ahlborn
 */
public class CalcColEvalContext extends RowEvalContext
{
  private final ColumnImpl _col;

  public CalcColEvalContext(ColumnImpl col) {
    super(col.getDatabase());
    _col = col;
  }

  CalcColEvalContext setExpr(String exprStr) {
    setExpr(Expressionator.Type.EXPRESSION, exprStr);
    return this;
  }

  @Override
  protected TableImpl getTable() {
    return _col.getTable();
  }

  @Override
  public Value.Type getResultType() {
    return toValueType(_col.getType());
  }

  public Object eval(Object[] row) throws IOException {
    try {
      setRow(row);
      return eval();
    } finally {
      reset();
    }
  }

  @Override
  protected String withErrorContext(String msg) {
    return _col.withErrorContext(msg);
  }
}
