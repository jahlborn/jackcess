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

  private enum SpecOp {
    NOT("Not"), IS_NULL("Is Null"), IS_NOT_NULL("Is Not Null"), LIKE("Like"), 
    BETWEEN("Between"), NOT_BETWEEN("Not Between"), IN("In"), 
    NOT_IN("Not In");

    private final String _str;

    private SpecOp(String str) {
      _str = str;
    }

    @Override
    public String toString() {
      return _str;
    }
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
    @Override protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append("<THIS_COL>");
    }
  };
  private static final Expr NULL_VALUE = new Expr() {
    @Override protected Object eval(RowContext ctx) {
      return null;
    }
    @Override protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append("Null");
    }
  };
  private static final Expr TRUE_VALUE = new Expr() {
    @Override protected Object eval(RowContext ctx) {
      return Boolean.TRUE;
    }
    @Override protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append("True");
    }
  };
  private static final Expr FALSE_VALUE = new Expr() {
    @Override protected Object eval(RowContext ctx) {
      return Boolean.FALSE;
    }
    @Override protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append("False");
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

    return parseExpression(new TokBuf(exprType, tokens), false);
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

  private static Expr parseExpression(TokBuf buf, boolean singleExpr)     
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

          } else {

            // is it an object name?
            Token next = buf.peekNext();
            if((next != null) && isObjNameSep(next)) {

              buf.setPendingExpr(parseObjectReference(t, buf));

            } else {
              
              // FIXME maybe obj name, maybe string?
              throw new UnsupportedOperationException("FIXME");
            }
          }
          
        } else {

          // this could be anything but COMP or DELIM (all COMPs would be
          // returned as TokenType.OP and all DELIMs would be TokenType.DELIM)
          switch(wordType) {
          case OP:

            parseOperatorExpression(t, buf);
            break;
            
          case LOG_OP:

            // FIXME, handle "between" expr ("and")

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

            parseSpecialOperator(t, buf);
            // FIXME
            break;

          default:
            throw new RuntimeException("Unexpected STRING word type "
                                       + wordType);
          }
        }

        break;
        
      case SPACE:
        // top-level space is irrelevant (and we strip them anyway)
        break;
        
      default:
        throw new RuntimeException("unknown token type " + t);
      }

      if(singleExpr && buf.hasPendingExpr()) {
        break;
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
      if(!isDelim(t, FUNC_START_DELIM)) {
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

  private static List<Expr> findParenExprs(
      TokBuf buf, boolean allowMulti) {

    if(allowMulti) {
      // simple case, no nested expr
      Token t = buf.peekNext();
      if(isDelim(t, CLOSE_PAREN)) {
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
          exprs.add(parseExpression(subBuf, false));
          return exprs;
        }

      } else if(allowMulti && (level == 1) && isDelim(t, FUNC_PARAM_SEP)) {

        TokBuf subBuf = buf.subBuf(startPos, buf.prevPos());
        exprs.add(parseExpression(subBuf, false));
        startPos = buf.curPos();
      }
    }

    throw new IllegalArgumentException("Missing closing '" + CLOSE_PAREN
                                       + " " + buf);
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
    Expr rightExpr = parseExpression(buf, true);

    return new EBinaryOp(op, leftExpr, rightExpr).resolveOrderOfOperations();
  }

  private static Expr parseUnaryOperator(Token firstTok, TokBuf buf) {
    String op = firstTok.getValueStr();
    Expr val = parseExpression(buf, true);

    return new EUnaryOp(op, val);
  }

  private static Expr parseCompOperator(Token firstTok, TokBuf buf) {
    String op = firstTok.getValueStr();
    Expr leftExpr = buf.takePendingExpr();
    Expr rightExpr = parseExpression(buf, true);

    return new ECompOp(op, leftExpr, rightExpr).resolveOrderOfOperations();
  }

  private static Expr parseLogicalOperator(Token firstTok, TokBuf buf) {
    String op = firstTok.getValueStr();
    Expr leftExpr = buf.takePendingExpr();
    Expr rightExpr = parseExpression(buf, true);

    return new ELogicalOp(op, leftExpr, rightExpr).resolveOrderOfOperations();
  }

  private static void parseSpecialOperator(Token firstTok, TokBuf buf) {
    
    SpecOp specOp = getSpecialOperator(firstTok, buf);

    if(specOp == SpecOp.NOT) {
      // this is the unary prefix operator
      buf.setPendingExpr(parseUnaryOperator(firstTok, buf));
      return;
    }

    if(!buf.hasPendingExpr() && (buf.getExprType() == Type.FIELD_VALIDATOR)) {
      // comparison operators for field validators can implicitly use
      // the current field value for the left value
      buf.setPendingExpr(THIS_COL_VALUE);
    }

    if(!buf.hasPendingExpr()) {
      throw new IllegalArgumentException(
          "Missing left expression for comparison operator " + 
          specOp + " " + buf);
    }


    Expr expr = buf.takePendingExpr();

    // FIXME
    Expr specOpExpr = null;
    switch(specOp) {
    case IS_NULL:
    case IS_NOT_NULL:
      specOpExpr = new ENullOp(specOp, expr);
      break;

    case LIKE:
      Token t = buf.next();
      // FIXME, create LITERAL_STRING TokenType?
      if(t.getType() != TokenType.LITERAL) {
        throw new IllegalArgumentException("Missing Like pattern " + buf);
      }
      specOpExpr = new ELikeOp(specOp, expr, t.getValueStr());
      break;

    case BETWEEN:
    case NOT_BETWEEN:
      
      // the "rest" of a between expression is of the form "X And Y".  since
      // that is the same as a normal "And" expression, we are going to cheat
      // and just parse the remaining

      if(true) {
      // FIXME
      throw new UnsupportedOperationException("FIXME");
      }

      break;

    case IN:
    case NOT_IN:

      // there might be a space before open paren
      t = buf.next();
      if(t.getType() == TokenType.SPACE) {
        t = buf.next();
      }
      if(!isDelim(t, OPEN_PAREN)) {
        throw new IllegalArgumentException("Malformed In expression " + buf);
      }

      List<Expr> exprs = findParenExprs(buf, true);
      specOpExpr = new EInOp(specOp, expr, exprs);
      break;

    default:
      throw new RuntimeException("Unexpected special op " + specOp);
    }

    buf.setPendingExpr(specOpExpr);
  }

  private static SpecOp getSpecialOperator(Token firstTok, TokBuf buf) {
    String opStr = firstTok.getValueStr().toLowerCase();

    if("is".equals(opStr)) {
      Token t = buf.peekNext();
      if(isString(t, "null")) {
        buf.next();
        return SpecOp.IS_NULL;
      } else if(isString(t, "not")) {
        buf.next();
        t = buf.peekNext();
        if(isString(t, "null")) {
          return SpecOp.IS_NOT_NULL;
        }
      }
    } else if("like".equals(opStr)) {
      return SpecOp.LIKE;
    } else if("between".equals(opStr)) {
      return SpecOp.BETWEEN;
    } else if("in".equals(opStr)) {
      return SpecOp.IN;
    } else if("not".equals(opStr)) {
      Token t = buf.peekNext();
      if(isString(t, "between")) {
        buf.next();
        return SpecOp.NOT_BETWEEN;
      } else if(isString(t, "in")) {
        buf.next();
        return SpecOp.NOT_IN;
      }
      return SpecOp.NOT;
    }

    throw new IllegalArgumentException(
        "Malformed special operator " + opStr + " " + buf);
  }

  private static boolean isObjNameSep(Token t) {
    return (isDelim(t, ".") || isDelim(t, "!"));
  }

  private static boolean isOp(Token t, String opStr) {
    return ((t != null) && (t.getType() == TokenType.OP) && 
            opStr.equalsIgnoreCase(t.getValueStr()));
  }

  private static boolean isDelim(Token t, String opStr) {
    return ((t != null) && (t.getType() == TokenType.DELIM) && 
            opStr.equalsIgnoreCase(t.getValueStr()));
  }

  private static boolean isString(Token t, String opStr) {
    return ((t != null) && (t.getType() == TokenType.STRING) && 
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
      if(isOp(t, "=")) {
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
      if(!hasNext()) {
        throw new IllegalArgumentException(
            "Unexpected end of expression " + this);
      }
      return _tokens.get(_pos++);
    }

    public void reset(int pos) {
      _pos = pos;
    }

    public TokBuf subBuf(int start, int end) {
      return new TokBuf(_tokens.subList(start, end), this, start);
    }

    public void setPendingExpr(Expr expr) {
      if(_pendingExpr != null) {
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

  private static boolean isHigherPrecendence(String op1, String op2) {
    int prec1 = PRECENDENCE.get(op1.toLowerCase());
    int prec2 = PRECENDENCE.get(op2.toLowerCase());

    // higher preceendence ops have lower numbers
    return (prec1 < prec2);
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

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb, false);
      return sb.toString();
    }
    
    public String toDebugString() {
      StringBuilder sb = new StringBuilder();
      toString(sb, true);
      return sb.toString();
    }
    
    protected void toString(StringBuilder sb, boolean isDebug) {
      if(isDebug) {
        sb.append("<").append(getClass().getSimpleName()).append(">{");
      }
      toExprString(sb, isDebug);
      if(isDebug) {
        sb.append("}");
      }
    }
    
    protected abstract Object eval(RowContext ctx);

    protected abstract void toExprString(StringBuilder sb, boolean isDebug);
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

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      // FIXME, stronger typing?
      if(_value instanceof String) {
        sb.append("\"").append(_value).append("\"");
      } else if(_value instanceof Date) {
        // FIXME Date,Time,DateTime formatting?
        sb.append("#").append(_value).append("#");
      } else {
        sb.append(_value);
      } 
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

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      if(_collectionName != null) {
        sb.append("[").append(_collectionName).append("].");
      }
      if(_objName != null) {
        sb.append("[").append(_objName).append("].");
      }
      sb.append("[").append(_fieldName).append("]");
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

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append("(");
      _expr.toString(sb, isDebug);
      sb.append(")");
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

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append(_name).append("(");

      if(!_params.isEmpty()) {
        Iterator<Expr> iter = _params.iterator();
        iter.next().toString(sb, isDebug);
        while(iter.hasNext()) {
          sb.append(",");
          iter.next().toString(sb, isDebug);
        }
      }
      
      sb.append(")");
    }
  }

  private static abstract class EBaseBinaryOp extends Expr
  {
    private final String _op;
    private Expr _left;
    private Expr _right;

    private EBaseBinaryOp(String op, Expr left, Expr right) {
      _op = op;
      _left = left;
      _right = right;
    }

    public Expr resolveOrderOfOperations() {

      // in order to get the precedence right, we need to first associate this
      // expression with the "rightmost" expression preceding it, then adjust
      // this expression "down" (lower precedence) as the precedence of the
      // operations dictates.  since we parse from left to right, the initial
      // "left" value isn't the immediate left expression, instead it's based
      // on how the preceding operator precedence worked out.  we need to
      // adjust "this" expression to the closest preceding expression before
      // we can correctly resolve precedence.

      Expr outerExpr = this;

      // current: <this>{<left>{A op1 B} op2 <right>{C}}
      if(_left instanceof EBaseBinaryOp) {

        EBaseBinaryOp leftOp = (EBaseBinaryOp)_left;
        
        // target: <left>{A op1 <this>{B op2 <right>{C}}}
        _left = leftOp._right;

        // give the new version of this expression an opportunity to further
        // swap (since the swapped expression may itself be a binary
        // expression)
        leftOp._right = resolveOrderOfOperations();
        outerExpr = leftOp;

        // at this point, this expression has been pushed all the way to the
        // rightmost preceding expression (we artifically gave "this" the
        // highest precedence).  now, we want to adjust precedence as
        // necessary (shift it back down if the operator precedence is
        // incorrect).  note, we only need to check precedence against "this",
        // as all other precedence has been resolved in previous parsing
        // rounds.
        if((leftOp._right == this) && isHigherPrecendence(leftOp._op, _op)) {

          // FIXME, need to move up if precedecne is the same!
          
          // doh, "this" is lower precedence, restore the original order of
          // things
          leftOp._right = _left;
          _left = leftOp;
          outerExpr = this;
        }
      }

      return outerExpr;
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      _left.toString(sb, isDebug);
      sb.append(" ").append(_op).append(" ");
      _right.toString(sb, isDebug);
    }    
  }

  private static class EBinaryOp extends EBaseBinaryOp
  {

    private EBinaryOp(String op, Expr left, Expr right) {
      super(op, left, right);
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
    private final Expr _expr;

    private EUnaryOp(String op, Expr expr) {
      _op = op;
      _expr = expr;
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append(" ").append(_op).append(" ");
      _expr.toString(sb, isDebug);
    }
  } 

  private static class ECompOp extends EBaseBinaryOp
  {
    private ECompOp(String op, Expr left, Expr right) {
      super(op, left, right);
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }
  }

  private static class ELogicalOp extends EBaseBinaryOp
  {
    private ELogicalOp(String op, Expr left, Expr right) {
      super(op, left, right);
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }
  } 

  private static abstract class ESpecOp extends Expr
  {
    private final SpecOp _op;
    private final Expr _expr;

    private ESpecOp(SpecOp op, Expr expr) {
      _op = op;
      _expr = expr;
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      // FIXME
      throw new UnsupportedOperationException("FIXME");
      // _expr.toString(sb, isDebug);
      // sb.append(" ").append(_op);
    } 
  }

  private static class ENullOp extends ESpecOp
  {
    private ENullOp(SpecOp op, Expr expr) {
      super(op, expr);
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }
  }

  private static class ELikeOp extends ESpecOp
  {
    private final String _pattern;

    private ELikeOp(SpecOp op, Expr expr, String pattern) {
      super(op, expr);
      _pattern = pattern;
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }
  }

  private static class EInOp extends ESpecOp
  {
    private final List<Expr> _exprs;

    private EInOp(SpecOp op, Expr expr, List<Expr> exprs) {
      super(op, expr);
      _exprs = exprs;
    }

    @Override
    protected Object eval(RowContext ctx) {
      // FIXME 

      return null;
    }
  }
}
