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
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import javax.script.Bindings;

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.JackcessException;
import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.Expression;
import com.healthmarketscience.jackcess.expr.Identifier;
import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.NumericConfig;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.expr.Expressionator;
import com.healthmarketscience.jackcess.impl.expr.ValueSupport;

/**
 *
 * @author James Ahlborn
 */
public abstract class BaseEvalContext implements EvalContext
{
  /** map of all non-string data types */
  private static final Map<DataType,Value.Type> TYPE_MAP =
    new EnumMap<DataType,Value.Type>(DataType.class);

  static {
    TYPE_MAP.put(DataType.BOOLEAN,Value.Type.LONG);
    TYPE_MAP.put(DataType.BYTE,Value.Type.LONG);
    TYPE_MAP.put(DataType.INT,Value.Type.LONG);
    TYPE_MAP.put(DataType.LONG,Value.Type.LONG);
    TYPE_MAP.put(DataType.MONEY,Value.Type.DOUBLE);
    TYPE_MAP.put(DataType.FLOAT,Value.Type.DOUBLE);
    TYPE_MAP.put(DataType.DOUBLE,Value.Type.DOUBLE);
    TYPE_MAP.put(DataType.SHORT_DATE_TIME,Value.Type.DATE_TIME);
    TYPE_MAP.put(DataType.NUMERIC,Value.Type.BIG_DEC);
    TYPE_MAP.put(DataType.BIG_INT,Value.Type.BIG_DEC);
  }

  private final DBEvalContext _dbCtx;
  private Expression _expr;

  protected BaseEvalContext(DBEvalContext dbCtx) {
    _dbCtx = dbCtx;
  }

  void setExpr(Expressionator.Type exprType, String exprStr) {
    _expr = new RawExpr(exprType, exprStr);
  }

  protected DatabaseImpl getDatabase() {
    return _dbCtx.getDatabase();
  }

  public TemporalConfig getTemporalConfig() {
    return _dbCtx.getTemporalConfig();
  }

  public SimpleDateFormat createDateFormat(String formatStr) {
    return _dbCtx.createDateFormat(formatStr);
  }

  public Calendar getCalendar() {
    return _dbCtx.getCalendar();
  }

  public NumericConfig getNumericConfig() {
    return _dbCtx.getNumericConfig();
  }

  public DecimalFormat createDecimalFormat(String formatStr) {
    return _dbCtx.createDecimalFormat(formatStr);
  }

  public float getRandom(Integer seed) {
    return _dbCtx.getRandom(seed);
  }

  public Value.Type getResultType() {
    return null;
  }

  public Value getThisColumnValue() {
    throw new UnsupportedOperationException();
  }

  public Value getIdentifierValue(Identifier identifier) {
    throw new UnsupportedOperationException();
  }

  public Bindings getBindings() {
    return _dbCtx.getBindings();
  }

  public Object get(String key) {
    return _dbCtx.getBindings().get(key);
  }

  public void put(String key, Object value) {
    _dbCtx.getBindings().put(key, value);
  }

  public Object eval() throws IOException {
    try {
      return _expr.eval(this);
    } catch(Exception e) {
      String msg = withErrorContext(e.getMessage());
      throw new JackcessException(msg, e);
    }
  }

  public void collectIdentifiers(Collection<Identifier> identifiers) {
    _expr.collectIdentifiers(identifiers);
  }

  @Override
  public String toString() {
    return _expr.toString();
  }

  protected Value toValue(Object val, DataType dType) {
    try {
      val = ColumnImpl.toInternalValue(dType, val, getDatabase());
      if(val == null) {
        return ValueSupport.NULL_VAL;
      }

      Value.Type vType = toValueType(dType);
      switch(vType) {
      case STRING:
        return ValueSupport.toValue(val.toString());
      case DATE:
      case TIME:
      case DATE_TIME:
        return ValueSupport.toValue(vType, (Date)val);
      case LONG:
        Integer i = ((val instanceof Integer) ? (Integer)val :
                     ((Number)val).intValue());
        return ValueSupport.toValue(i);
      case DOUBLE:
        Double d = ((val instanceof Double) ? (Double)val :
                    ((Number)val).doubleValue());
        return ValueSupport.toValue(d);
      case BIG_DEC:
        BigDecimal bd = ColumnImpl.toBigDecimal(val, getDatabase());
        return ValueSupport.toValue(bd);
      default:
        throw new RuntimeException("Unexpected type " + vType);
      }
    } catch(IOException e) {
      throw new EvalException("Failed converting value to type " + dType, e);
    }
  }

  public static Value.Type toValueType(DataType dType) {
    Value.Type type = TYPE_MAP.get(dType);
    return ((type == null) ? Value.Type.STRING : type);
  }

  protected abstract String withErrorContext(String msg);

  private class RawExpr implements Expression
  {
    private final Expressionator.Type _exprType;
    private final String _exprStr;

    private RawExpr(Expressionator.Type exprType, String exprStr) {
      _exprType = exprType;
      _exprStr = exprStr;
    }

    private Expression getExpr() {
      // when the expression is parsed we replace the raw version
      Expression expr = Expressionator.parse(
          _exprType, _exprStr, getResultType(), _dbCtx);
      _expr = expr;
      return expr;
    }

    public Object eval(EvalContext ctx) {
      return getExpr().eval(ctx);
    }

    public String toDebugString(LocaleContext ctx) {
      return getExpr().toDebugString(ctx);
    }

    public String toRawString() {
      return _exprStr;
    }

    public String toCleanString(LocaleContext ctx) {
      return getExpr().toCleanString(ctx);
    }

    public boolean isConstant() {
      return getExpr().isConstant();
    }

    public void collectIdentifiers(Collection<Identifier> identifiers) {
      getExpr().collectIdentifiers(identifiers);
    }

    @Override
    public String toString() {
      return toRawString();
    }
  }
}
