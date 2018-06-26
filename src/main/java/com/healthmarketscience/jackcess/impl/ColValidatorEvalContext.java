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

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.InvalidValueException;
import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.Identifier;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.expr.Expressionator;
import com.healthmarketscience.jackcess.util.ColumnValidator;

/**
 *
 * @author James Ahlborn
 */
public class ColValidatorEvalContext extends ColEvalContext
{
  private String _helpStr;
  private Object _val;

  public ColValidatorEvalContext(ColumnImpl col) {
    super(col);
  }

  ColValidatorEvalContext setExpr(String exprStr, String helpStr) {
    setExpr(Expressionator.Type.FIELD_VALIDATOR, exprStr);
    _helpStr = helpStr;
    return this;
  }

  ColumnValidator toColumnValidator(ColumnValidator delegate) {
    return new InternalColumnValidator(delegate) {
      @Override
      protected Object internalValidate(Column col, Object val)
        throws IOException {
        return ColValidatorEvalContext.this.validate(col, val);
      }
      @Override
      protected void appendToString(StringBuilder sb) {
        sb.append("expression=").append(ColValidatorEvalContext.this);
      }
    };
  }

  private void reset() {
    _val = null;
  }

  @Override
  public Value getThisColumnValue() {
    return toValue(_val, getCol().getType());
  }

  @Override
  public Value getIdentifierValue(Identifier identifier) {
    // col validators can only get "this" column, but they can refer to it by
    // name
    if(!getCol().isThisColumn(identifier)) {
      throw new EvalException("Cannot access other fields for " + identifier);
    }
    return getThisColumnValue();
  }

  private Object validate(Column col, Object val) throws IOException {
    try {
      _val = val;
      Boolean result = (Boolean)eval();
      if(!result) {
        String msg = ((_helpStr != null) ? _helpStr :
                      "Invalid column value '" + val + "'");
        throw new InvalidValueException(withErrorContext(msg));
      }
      return val;
    } finally {
      reset();
    }
  }
}
