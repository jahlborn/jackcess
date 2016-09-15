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

package com.healthmarketscience.jackcess.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import static com.healthmarketscience.jackcess.util.ExpressionTokenizer.Token;
import static com.healthmarketscience.jackcess.util.ExpressionTokenizer.TokenType;

/**
 *
 * @author James Ahlborn
 */
public class Expressionator 
{

  public enum Type {
    DEFAULT_VALUE, FIELD_VALIDATOR;
  }

  private enum WordType {
    OP, COMP, LOG_OP, CONST, SPEC_OP_PREFIX;
  }

  private static final String FUNC_START_DELIM = "(";
  private static final String FUNC_END_DELIM = ")";
  private static final String FUNC_PARAM_SEP = ",";

  private static final Map<String,WordType> WORD_TYPES = new HashMap<String,WordType>();

  static {
    setWordType(WordType.OP, "+", "-", "*", "/", "\\", "^", "&", "mod");
    setWordType(WordType.COMP, "<", "<=", ">", ">=", "=", "<>");
    setWordType(WordType.LOG_OP, "and", "or", "eqv", "not", "xor");
    setWordType(WordType.CONST, "true", "false", "null");
    setWordType(WordType.SPEC_OP_PREFIX, "is", "like", "between", "in");
  }


  private static final Expr THIS_COL_VALUE = new Expr() {
    @Override protected Object eval(RowContext ctx) {
      return ctx.getThisColumnValue();
    }
  };
  private static final Expr NULL_VALUE = new Expr() {
    @Override protected Object eval(RowContext ctx) {
      return null;
    }
  };
  private static final Expr TRUE_VALUE = new Expr() {
    @Override protected Object eval(RowContext ctx) {
      return Boolean.TRUE;
    }
  };
  private static final Expr FALSE_VALUE = new Expr() {
    @Override protected Object eval(RowContext ctx) {
      return Boolean.FALSE;
    }
  };

  private Expressionator() 
  {
  }

  public static String testTokenize(Type exprType, String exprStr, Database db) {
    
    List<Token> tokens = trimSpaces(
        ExpressionTokenizer.tokenize(exprType, exprStr, (DatabaseImpl)db));

    if(tokens == null) {
      // FIXME, NULL_EXPR?
      return null;
    }

    return tokens.toString();
  }

  public static Expr parse(Type exprType, String exprStr, Database db) {

    List<Token> tokens = trimSpaces(
        ExpressionTokenizer.tokenize(exprType, exprStr, (DatabaseImpl)db));

    if(tokens == null) {
      // FIXME, NULL_EXPR?
      return null;
    }

    TokBuf buf = new TokBuf(tokens);
    parseExpression(exprType, buf, isSimpleExpression(buf, exprType));
    
    // FIXME
    return null;
  }

  private static List<Token> trimSpaces(List<Token> tokens) {
    if(tokens == null) {
      return null;
    }

    // for the most part, spaces are superfluous except for one situation(?).
    // when they appear between a string literal and '(' they help distinguish
    // a function call from another expression form
    for(int i = 1; i < (tokens.size() - 1); ++i) {
      Token t = tokens.get(i);
      if(t.getType() == TokenType.SPACE) {
        if((tokens.get(i - 1).getType() == TokenType.STRING) &&
           isOp(tokens.get(i + 1), FUNC_START_DELIM)) {
          // we want to keep this space
        } else {
          tokens.remove(i);
          --i;
        }
      }
    }
    return tokens;
  }

  private static Expr parseExpression(Type exprType, TokBuf buf, 
                                      boolean isSimpleExpr)     
  {

    // FIXME, how do we handle order of ops when no parens?
    
    while(buf.hasNext()) {
      Token t = buf.next();

      switch(t.getType()) {
      case OBJ_NAME:
        break;
      case LITERAL:
        break;
      case OP:
        break;
      case STRING:
        WordType wordType = getWordType(t);
        if(wordType == null) {
          // literal string? or possibly function?
          Expr funcExpr = maybeParseFuncCall(t, buf, exprType, isSimpleExpr);
          if(funcExpr != null) {
            buf.setPendingExpr(funcExpr);
            continue;
          }
          // FIXME
        }
        break;
      case SPACE:
        // top-level space is irrelevant
        break;
      default:
        throw new RuntimeException("unknown token type " + t.getType());
      }
    }

    Expr expr = buf.takePendingExpr();
    if(expr == null) {
      throw new IllegalArgumentException("No expression found?");
    }

    return expr;
  }

  private static Expr maybeParseFuncCall(Token firstTok, TokBuf buf,
                                         Type exprType, boolean isSimpleExpr) {

    int startPos = buf.curPos();
    boolean foundFunc = false;

    try {
      Token t = buf.peekNext();
      if((t == null) || !isOp(t, FUNC_START_DELIM)) {
        // not a function call
        return null;
      }
        
      buf.next();
      List<TokBuf> paramBufs = findFuncCallParams(buf);

      List<Expr> params = Collections.emptyList();
      if(!paramBufs.isEmpty()) {
        params = new ArrayList<Expr>(paramBufs.size());
        for(TokBuf paramBuf : paramBufs) {
          params.add(parseExpression(exprType, paramBuf, isSimpleExpr));
        }
      }
        
      return new EFunc((String)firstTok.getValue(), params);

    } finally {
      if(!foundFunc) {
        buf.reset(startPos);
      }
    }
  }

  private static List<TokBuf> findFuncCallParams(TokBuf buf) {

    // simple case, no params
    Token t = buf.peekNext();
    if((t != null) && isOp(t, FUNC_END_DELIM)) {
      buf.next();
      return Collections.emptyList();
    }

    // find closing ")", handle nested parens
    List<TokBuf> params = new ArrayList<TokBuf>(3);
    int level = 1;
    int startPos = buf.curPos();
    while(buf.hasNext()) {

      t = buf.next();

      if(isOp(t, FUNC_START_DELIM)) {

        ++level;

      } else if(isOp(t, FUNC_END_DELIM)) {

        --level;
        if(level == 0) {
          params.add(buf.subBuf(startPos, buf.prevPos()));

          if(params.size() > 1) {
            // if there is more than one param and one of them is empty, then
            // something is messed up (note, it should not be possible to have
            // an empty param if there is only one since we trim superfluous
            // spaces)
            for(TokBuf paramBuf : params) {
              if(!paramBuf.hasNext()) {
                throw new IllegalArgumentException(
                    "Invalid empty parameter for function");
              }
            }
          }

          return params;
        }

      } else if((level == 1) && isOp(t, FUNC_PARAM_SEP)) {

        params.add(buf.subBuf(startPos, buf.prevPos()));
        startPos = buf.curPos();
      }
    }

    throw new IllegalArgumentException("Missing closing '" + FUNC_END_DELIM +
                                       "' for function call");
  }

  private static boolean isSimpleExpression(TokBuf buf, Type exprType) {
    if(exprType != Type.DEFAULT_VALUE) {
      return false;
    }

    // a leading "=" indicates "full" expression handling for a DEFAULT_VALUE
    Token t = buf.peekNext();
    if((t != null) && isOp(t, "=")) {
      buf.next();
      return false;
    }

    // this is a "simple" DEFAULT_VALUE
    return true;
  }

  private static boolean isOp(Token t, String opStr) {
    return ((t.getType() == TokenType.OP) && 
            opStr.equalsIgnoreCase((String)t.getValue()));
  }

  private static WordType getWordType(Token t) {
    return WORD_TYPES.get(((String)t.getValue()).toLowerCase());
  }

  private static void setWordType(WordType type, String... words) {
    for(String w : words) {
      WORD_TYPES.put(w, type);
    }
  }

  private static final class TokBuf
  {
    private final List<Token> _tokens;
    private final boolean _topLevel;
    private int _pos;
    private Expr _pendingExpr;

    private TokBuf(List<Token> tokens) {
      this(tokens, true);
    }

    private TokBuf(List<Token> tokens, boolean topLevel) {
      _tokens = tokens;
      _topLevel = topLevel;
    }

    public boolean isTopLevel() {
      return _topLevel;
    }

    public int curPos() {
      return _pos;
    }

    public int prevPos() {
      return _pos - 1;
    }

    public boolean hasNext() {
      return (_pos < _tokens.size());
    }

    public Token peekNext() {
      if(!hasNext()) {
        return null;
      }
      return _tokens.get(_pos);
    }

    public Token next() {
      return _tokens.get(_pos++);
    }

    public void reset(int pos) {
      _pos = pos;
    }

    public TokBuf subBuf(int start, int end) {
      return new TokBuf(_tokens.subList(start, end), false);
    }

    public void setPendingExpr(Expr expr) {
      if(_pendingExpr == null) {
        throw new IllegalArgumentException("Found multiple expressions with no operator");
      }
      _pendingExpr = expr;
    } 

    public Expr takePendingExpr() {
      Expr expr = _pendingExpr;
      _pendingExpr = null;
      return expr;
    }

    public boolean hasPendingExpr() {
      return (_pendingExpr != null);
    }
  }

  public static abstract class Expr
  {
    public Object evalDefault() {
      return eval(null);
    }

    public boolean evalCondition(RowContext ctx) {
      Object val = eval(ctx);

      if(val instanceof Boolean) {
        return (Boolean)val;
      }

      // a single value as a conditional expression seems to act like an
      // implicit "="
      return val.equals(ctx.getThisColumnValue());
    }

    protected abstract Object eval(RowContext ctx);
  }

  public interface RowContext
  {
    public Object getThisColumnValue();

    public Object getRowValue(String colName);
  }

  private static final class ELiteralValue extends Expr
  {
    private final Object _value;

    private ELiteralValue(Object value) {
      _value = value;
    }

    @Override
    public Object eval(RowContext ctx) {
      return _value;
    }
  }

  private static final class EColumnValue extends Expr
  {
    private final String _colName;

    private EColumnValue(String colName) {
      _colName = colName;
    }

    @Override
    public Object eval(RowContext ctx) {
      return ctx.getRowValue(_colName);
    }
  }

  private static abstract class EOp
  {
    
  }

  private static abstract class ECond
  {
    
  }

  private static class EParen extends Expr
  {
    private final Expr _expr;

    private EParen(Expr expr) {
      _expr = expr;
    }

    @Override
    protected Object eval(RowContext ctx) {
      return _expr.eval(ctx);
    }
  }

  private static class EFunc extends Expr
  {
    private final String _name;
    private final List<Expr> _params;

    private EFunc(String name, List<Expr> params) {
      _name = name;
      _params = params;
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME how do func results act for conditional values?

      return false;
    }
  }

}
