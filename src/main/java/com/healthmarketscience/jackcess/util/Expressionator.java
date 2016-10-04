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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
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

  // Useful links:
  // - syntax: https://support.office.com/en-us/article/Guide-to-expression-syntax-ebc770bc-8486-4adc-a9ec-7427cce39a90
  // - examples: https://support.office.com/en-us/article/Examples-of-expressions-d3901e11-c04e-4649-b40b-8b6ec5aed41f
  // - validation rule usage: https://support.office.com/en-us/article/Restrict-data-input-by-using-a-validation-rule-6c0b2ce1-76fa-4be0-8ae9-038b52652320

  // FIXME
  // - need to short-circuit AND/OR
  // - need to handle order of operations
  // - ^
  // - - (negate)
  // - * /
  // - \
  // - Mod
  // - + -
  // - &
  // - < > <> <= >= = Like Is
  // - Not
  // - And
  // - Or
  // - Xor
  // - Eqv
  // - In, Between ????

  public enum Type {
    DEFAULT_VALUE, FIELD_VALIDATOR, RECORD_VALIDATOR;
  }

  private enum WordType {
    OP, COMP, LOG_OP, CONST, SPEC_OP_PREFIX, DELIM;
  }

  private static final String FUNC_START_DELIM = "(";
  private static final String FUNC_END_DELIM = ")";
  private static final String OPEN_PAREN = "(";
  private static final String CLOSE_PAREN = ")";
  private static final String FUNC_PARAM_SEP = ",";

  private static final Map<String,WordType> WORD_TYPES = new HashMap<String,WordType>();

  static {
    setWordType(WordType.OP, "+", "-", "*", "/", "\\", "^", "&", "mod");
    setWordType(WordType.COMP, "<", "<=", ">", ">=", "=", "<>");
    setWordType(WordType.LOG_OP, "and", "or", "eqv", "xor");
    setWordType(WordType.CONST, "true", "false", "null");
    setWordType(WordType.SPEC_OP_PREFIX, "is", "like", "between", "in", "not");
    // "X is null", "X is not null", "X like P", "X between A and B",
    // "X not between A and B", "X in (A, B, C...)", "X not in (A, B, C...)",
    // "not X"
    setWordType(WordType.DELIM, ".", "!", ",", "(", ")");
  }

  private static final Map<String, Integer> PRECENDENCE = 
    buildPrecedenceMap(
        new String[]{"^"}, 
        new String[]{"-"}, // FIXME (negate)?
        new String[]{"*", "/"}, 
        new String[]{"\\"}, 
        new String[]{"mod"}, 
        new String[]{"+", "-"}, 
        new String[]{"&"}, 
        new String[]{"<", ">", "<>", "<=", ">=", "=", "like", "is"}, 
        new String[]{"not"}, 
        new String[]{"and"}, 
        new String[]{"or"}, 
        new String[]{"xor"}, 
        new String[]{"eqv"}, 
        new String[]{"in", "between"});

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

    // FIXME,restrictions:
    // - default value only accepts simple exprs, otherwise becomes literal text
    // - def val cannot refer to any columns
    // - field validation cannot refer to other columns
    // - record validation cannot refer to outside columns

    List<Token> tokens = trimSpaces(
        ExpressionTokenizer.tokenize(exprType, exprStr, (DatabaseImpl)db));

    if(tokens == null) {
      // FIXME, NULL_EXPR?
      return null;
    }

    TokBuf buf = new TokBuf(exprType, tokens);
    parseExpression(buf);
    
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
           isDelim(tokens.get(i + 1), FUNC_START_DELIM)) {
          // we want to keep this space
        } else {
          tokens.remove(i);
          --i;
        }
      }
    }
    return tokens;
  }

  private static Expr parseExpression(TokBuf buf)     
  {
    // FIXME, how do we handle order of ops when no parens?
    
    while(buf.hasNext()) {
      Token t = buf.next();

      switch(t.getType()) {
      case OBJ_NAME:

        buf.setPendingExpr(parseObjectReference(t, buf));
        break;

      case LITERAL:
        
        buf.setPendingExpr(new ELiteralValue(t.getValue()));
        break;
        
      case OP:

        WordType wordType = getWordType(t);
        if(wordType == null) {
          // shouldn't happen
          throw new RuntimeException("Invalid operator " + t);
        }

        // this can only be an OP or a COMP (those are the only words that the
        // tokenizer would define as TokenType.OP)
        switch(wordType) {
        case OP:
          parseOperatorExpression(t, buf);
          break;

        case COMP:

          if(!buf.hasPendingExpr() && (buf.getExprType() == Type.FIELD_VALIDATOR)) {
            // comparison operators for field validators can implicitly use
            // the current field value for the left value
            buf.setPendingExpr(THIS_COL_VALUE);
          }
          if(buf.hasPendingExpr()) {
            buf.setPendingExpr(parseCompOperator(t, buf));
          } else {
            throw new IllegalArgumentException(
                "Missing left expression for comparison operator " + 
                t.getValue() + " " + buf);
          }
          break;

        default:
          throw new RuntimeException("Unexpected OP word type " + wordType);
        }
        
        break;

      case DELIM:

        // the only "top-level" delim we expect to find is open paren, and
        // there shouldn't be any pending expression
        if(!isDelim(t, OPEN_PAREN) || buf.hasPendingExpr()) {
          throw new IllegalArgumentException("Unexpected delimiter " + 
                                             t.getValue() + " " + buf);
        }

        Expr subExpr = findParenExprs(buf, false).get(0);
        buf.setPendingExpr(new EParen(subExpr));
        break;
        
      case STRING:

        // see if it's a special word?
        wordType = getWordType(t);
        if(wordType == null) {

          // is it a function call?
          Expr funcExpr = maybeParseFuncCall(t, buf);
          if(funcExpr != null) {
            buf.setPendingExpr(funcExpr);
            continue;
          }

          // is it an object name?
          Token next = buf.peekNext();
          if((next != null) && isObjNameSep(next)) {
            buf.setPendingExpr(parseObjectReference(t, buf));
            continue;
          }

          // FIXME maybe obj name, maybe string?
          
        } else {

          // this could be anything but COMP or DELIM (all COMPs would be
          // returned as TokenType.OP and all DELIMs would be TokenType.DELIM)
          switch(wordType) {
          case OP:

            parseOperatorExpression(t, buf);
            break;
            
          case LOG_OP:

            if(buf.hasPendingExpr()) {
              buf.setPendingExpr(parseLogicalOperator(t, buf));
            } else {
              throw new IllegalArgumentException(
                  "Missing left expression for logical operator " + 
                  t.getValue() + " " + buf);
            }
            break;

          case CONST:

            if("true".equalsIgnoreCase(t.getValueStr())) {
              buf.setPendingExpr(TRUE_VALUE);
            } else if("false".equalsIgnoreCase(t.getValueStr())) {
              buf.setPendingExpr(FALSE_VALUE);
            } else if("false".equalsIgnoreCase(t.getValueStr())) {
              buf.setPendingExpr(TRUE_VALUE);
            } else {
              throw new RuntimeException("Unexpected CONST word "
                                         + t.getValue());
            }
            break;

          case SPEC_OP_PREFIX:
            // FIXME
            break;

          default:
            throw new RuntimeException("Unexpected STRING word type "
                                       + wordType);
          }
        
          // FIXME
        }

        break;
        
      case SPACE:
        // top-level space is irrelevant (and we strip them anyway)
        break;
        
      default:
        throw new RuntimeException("unknown token type " + t);
      }
    }

    Expr expr = buf.takePendingExpr();
    if(expr == null) {
      throw new IllegalArgumentException("No expression found? " + buf);
    }

    return expr;
  }

  private static Expr parseObjectReference(Token firstTok, TokBuf buf) {

    // object references may be joined by '.' or '!'. access syntac docs claim
    // object identifiers can be formatted like:
    //     "[Collection name]![Object name].[Property name]"
    // However, in practice, they only ever seem to be (at most) two levels
    // and only use '.'.
    Deque<String> objNames = new LinkedList<String>();
    objNames.add(firstTok.getValueStr());

    Token t = null;
    boolean atSep = false;
    while((t = buf.peekNext()) != null) {
      if(!atSep) {
        if(isObjNameSep(t)) {
          buf.next();
          atSep = true;
          continue;
        } 
      } else {
        if((t.getType() == TokenType.OBJ_NAME) ||
           (t.getType() == TokenType.STRING)) {
          buf.next();
          // always insert at beginning of list so names are in reverse order
          objNames.addFirst(t.getValueStr());
          atSep = false;
          continue;
        }
      }
      break;
    }

    if(atSep || (objNames.size() > 3)) {
      throw new IllegalArgumentException("Invalid object reference " + buf);
    }

    // names are in reverse order
    String fieldName = objNames.poll();
    String objName = objNames.poll();
    String collectionName = objNames.poll();

    return new EObjValue(collectionName, objName, fieldName);
  }
  
  private static Expr maybeParseFuncCall(Token firstTok, TokBuf buf) {

    int startPos = buf.curPos();
    boolean foundFunc = false;

    try {
      Token t = buf.peekNext();
      if((t == null) || !isDelim(t, FUNC_START_DELIM)) {
        // not a function call
        return null;
      }
        
      buf.next();
      List<Expr> params = findParenExprs(buf, true);
      return new EFunc(firstTok.getValueStr(), params);

    } finally {
      if(!foundFunc) {
        buf.reset(startPos);
      }
    }
  }

  private static List<Expr> findParenExprs(TokBuf buf, boolean isFunc) {

    if(isFunc) {
      // simple case, no nested expr
      Token t = buf.peekNext();
      if((t != null) && isDelim(t, CLOSE_PAREN)) {
        buf.next();
        return Collections.emptyList();
      }
    }

    // find closing ")", handle nested parens
    List<Expr> exprs = new ArrayList<Expr>(3);
    int level = 1;
    int startPos = buf.curPos();
    while(buf.hasNext()) {

      Token t = buf.next();

      if(isDelim(t, OPEN_PAREN)) {

        ++level;

      } else if(isDelim(t, CLOSE_PAREN)) {

        --level;
        if(level == 0) {
          TokBuf subBuf = buf.subBuf(startPos, buf.prevPos());
          exprs.add(parseExpression(subBuf));
          return exprs;
        }

      } else if(isFunc && (level == 1) && isDelim(t, FUNC_PARAM_SEP)) {

        TokBuf subBuf = buf.subBuf(startPos, buf.prevPos());
        exprs.add(parseExpression(subBuf));
        startPos = buf.curPos();
      }
    }

    String exprName = (isFunc ? "function call" : "parenthesized expression");
    throw new IllegalArgumentException("Missing closing '" + CLOSE_PAREN +
                                       "' for " + exprName + " " + buf);
  }

  private static void parseOperatorExpression(Token t, TokBuf buf) {

    // most ops are two argument except that '-' could be negation
    if(buf.hasPendingExpr()) {
      buf.setPendingExpr(parseBinaryOperator(t, buf));
    } else if(isOp(t, "-")) {
      buf.setPendingExpr(parseUnaryOperator(t, buf));
    } else {
      throw new IllegalArgumentException(
          "Missing left expression for binary operator " + t.getValue() + 
          " " + buf);
    }
  }

  private static Expr parseBinaryOperator(Token firstTok, TokBuf buf) {
    String op = firstTok.getValueStr();
    Expr leftExpr = buf.takePendingExpr();
    Expr rightExpr = parseExpression(buf);

    return new EBinaryOp(op, leftExpr, rightExpr);
  }

  private static Expr parseUnaryOperator(Token firstTok, TokBuf buf) {
    String op = firstTok.getValueStr();
    Expr val = parseExpression(buf);

    return new EUnaryOp(op, val);
  }

  private static Expr parseCompOperator(Token firstTok, TokBuf buf) {
    String op = firstTok.getValueStr();
    Expr leftExpr = buf.takePendingExpr();
    Expr rightExpr = parseExpression(buf);

    return new ECompOp(op, leftExpr, rightExpr);
  }

  private static Expr parseLogicalOperator(Token firstTok, TokBuf buf) {
    String op = firstTok.getValueStr();
    Expr leftExpr = buf.takePendingExpr();
    Expr rightExpr = parseExpression(buf);

    return new ELogicalOp(op, leftExpr, rightExpr);
  }

  private static boolean isObjNameSep(Token t) {
    return (isDelim(t, ".") || isDelim(t, "!"));
  }

  private static boolean isOp(Token t, String opStr) {
    return ((t.getType() == TokenType.OP) && 
            opStr.equalsIgnoreCase(t.getValueStr()));
  }

  private static boolean isDelim(Token t, String opStr) {
    return ((t.getType() == TokenType.DELIM) && 
            opStr.equalsIgnoreCase(t.getValueStr()));
  }

  private static WordType getWordType(Token t) {
    return WORD_TYPES.get(t.getValueStr().toLowerCase());
  }

  private static void setWordType(WordType type, String... words) {
    for(String w : words) {
      WORD_TYPES.put(w, type);
    }
  }

  private static final class TokBuf
  {
    private final Type _exprType;
    private final List<Token> _tokens;
    private final TokBuf _parent;
    private final int _parentOff;
    private int _pos;
    private Expr _pendingExpr;
    private final boolean _simpleExpr;

    private TokBuf(Type exprType, List<Token> tokens) {
      this(exprType, false, tokens, null, 0);
    }

    private TokBuf(List<Token> tokens, TokBuf parent, int parentOff) {
      this(parent._exprType, parent._simpleExpr, tokens, parent, parentOff);
    }

    private TokBuf(Type exprType, boolean simpleExpr, List<Token> tokens, 
                   TokBuf parent, int parentOff) {
      _exprType = exprType;
      _tokens = tokens;
      _parent = parent;
      _parentOff = parentOff;
      if(parent == null) {
        // "top-level" expression, determine if it is a simple expression or not
        simpleExpr = isSimpleExpression();
      }
      _simpleExpr = simpleExpr;
    }

    private boolean isSimpleExpression() {
      if(_exprType != Type.DEFAULT_VALUE) {
        return false;
      }

      // a leading "=" indicates "full" expression handling for a DEFAULT_VALUE
      Token t = peekNext();
      if((t != null) && isOp(t, "=")) {
        next();
        return false;
      }

      // this is a "simple" DEFAULT_VALUE
      return true;
    }

    public Type getExprType() {
      return _exprType;
    }

    public boolean isSimpleExpr() {
      return _simpleExpr;
    }

    public boolean isTopLevel() {
      return (_parent == null);
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
      return new TokBuf(_tokens.subList(start, end), this, start);
    }

    public void setPendingExpr(Expr expr) {
      if(_pendingExpr == null) {
        throw new IllegalArgumentException(
            "Found multiple expressions with no operator " + this);
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

    private Map.Entry<Integer,List<Token>> getTopPos() {
      int pos = _pos;
      List<Token> toks = _tokens;
      TokBuf cur = this;
      while(cur._parent != null) {
        pos += cur._parentOff;
        cur = cur._parent;
        toks = cur._tokens;
      }
      return ExpressionTokenizer.newEntry(pos, toks);
    }

    @Override
    public String toString() {
      
      Map.Entry<Integer,List<Token>> e = getTopPos();

      // TODO actually format expression?
      StringBuilder sb = new StringBuilder()
        .append("[token ").append(e.getKey()).append("] (");

      for(Iterator<Token> iter = e.getValue().iterator(); iter.hasNext(); ) {
        Token t = iter.next();
        sb.append("'").append(t.getValueStr()).append("'");
        if(iter.hasNext()) {
          sb.append(",");
        }
      }

      sb.append(")");

      return sb.toString();
    } 
  }

  private static final Map<String, Integer> buildPrecedenceMap(String[]... opArrs) {
    Map<String, Integer> prec = new HashMap<String, Integer>();

    int level = 0;
    for(String[] ops : opArrs) {
      for(String op : ops) {
        prec.put(op, level);
      }
      ++level;
    }

    return prec;
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

    public Object getRowValue(String collectionName, String objName,
                              String colName);
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

  private static final class EObjValue extends Expr
  {
    private final String _collectionName;
    private final String _objName;
    private final String _fieldName;


    private EObjValue(String collectionName, String objName, String fieldName) {
      _collectionName = collectionName;
      _objName = objName;
      _fieldName = fieldName;
    }

    @Override
    public Object eval(RowContext ctx) {
      return ctx.getRowValue(_collectionName, _objName, _fieldName);
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
      // FIXME how do func results act for conditional values? (literals become = tests)

      return false;
    }
  }

  private static class EBinaryOp extends Expr
  {
    private final String _op;
    private final Expr _left;
    private final Expr _right;

    private EBinaryOp(String op, Expr left, Expr right) {
      _op = op;
      _left = left;
      _right = right;
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }
  } 

  private static class EUnaryOp extends Expr
  {
    private final String _op;
    private final Expr _val;

    private EUnaryOp(String op, Expr val) {
      _op = op;
      _val = val;
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }
  } 

  private static class ECompOp extends Expr
  {
    private final String _op;
    private final Expr _left;
    private final Expr _right;

    private ECompOp(String op, Expr left, Expr right) {
      _op = op;
      _left = left;
      _right = right;
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }
  } 


  private static class ELogicalOp extends Expr
  {
    private final String _op;
    private final Expr _left;
    private final Expr _right;

    private ELogicalOp(String op, Expr left, Expr right) {
      _op = op;
      _left = left;
      _right = right;
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }
  } 
}
