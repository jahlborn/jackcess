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

package com.healthmarketscience.jackcess.impl.expr;

import java.math.BigDecimal;
import java.text.DateFormat;
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
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.expr.Expression;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.RowContext;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.expr.ExpressionTokenizer.Token;
import com.healthmarketscience.jackcess.impl.expr.ExpressionTokenizer.TokenType;


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

  public enum Type {
    DEFAULT_VALUE, FIELD_VALIDATOR, RECORD_VALIDATOR;
  }

  public interface ParseContext {
    public SimpleDateFormat createDateFormat(String formatStr);
    public Function getExpressionFunction(String name);
  }

  public static final ParseContext DEFAULT_PARSE_CONTEXT = new ParseContext() {
    public SimpleDateFormat createDateFormat(String formatStr) {
      return DatabaseBuilder.createDateFormat(formatStr);
    }
    public Function getExpressionFunction(String name) {
      return DefaultFunctions.getFunction(name);
    }
  };

  private enum WordType {
    OP, COMP, LOG_OP, CONST, SPEC_OP_PREFIX, DELIM;
  }

  private static final String FUNC_START_DELIM = "(";
  private static final String FUNC_END_DELIM = ")";
  private static final String OPEN_PAREN = "(";
  private static final String CLOSE_PAREN = ")";
  private static final String FUNC_PARAM_SEP = ",";

  private static final Map<String,WordType> WORD_TYPES = 
    new HashMap<String,WordType>();

  static {
    setWordType(WordType.OP, "+", "-", "*", "/", "\\", "^", "&", "mod");
    setWordType(WordType.COMP, "<", "<=", ">", ">=", "=", "<>");
    setWordType(WordType.LOG_OP, "and", "or", "eqv", "xor", "imp");
    setWordType(WordType.CONST, "true", "false", "null");
    setWordType(WordType.SPEC_OP_PREFIX, "is", "like", "between", "in", "not");
    // "X is null", "X is not null", "X like P", "X between A and B",
    // "X not between A and B", "X in (A, B, C...)", "X not in (A, B, C...)",
    // "not X"
    setWordType(WordType.DELIM, ".", "!", ",", "(", ")");
  }

  private interface OpType {}

  private enum UnaryOp implements OpType {
    NEG("-", false) {
      @Override public Value eval(Value param1) {
        return BuiltinOperators.negate(param1);
      }
    },
    NOT("Not", true) {
      @Override public Value eval(Value param1) {
        return BuiltinOperators.not(param1);
      }
    };

    private final String _str;
    private final boolean _needSpace;

    private UnaryOp(String str, boolean needSpace) {
      _str = str;
      _needSpace = needSpace;
    }

    public boolean needsSpace() {
      return _needSpace;
    }

    @Override
    public String toString() {
      return _str;
    }

    public abstract Value eval(Value param1);
  }

  private enum BinaryOp implements OpType {
    PLUS("+") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.add(param1, param2);
      }
    }, 
    MINUS("-") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.subtract(param1, param2);
      }
    }, 
    MULT("*") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.multiply(param1, param2);
      }
    },
    DIV("/") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.divide(param1, param2);
      }
    }, 
    INT_DIV("\\") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.intDivide(param1, param2);
      }
    },
    EXP("^") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.exp(param1, param2);
      }
    }, 
    CONCAT("&") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.concat(param1, param2);
      }
    }, 
    MOD("Mod") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.mod(param1, param2);
      }
    };

    private final String _str;

    private BinaryOp(String str) {
      _str = str;
    }

    @Override
    public String toString() {
      return _str;
    }

    public abstract Value eval(Value param1, Value param2);
  }

  private enum CompOp implements OpType {
    LT("<") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.lessThan(param1, param2);
      }
    }, 
    LTE("<=") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.lessThanEq(param1, param2);
      }
    },
    GT(">") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.greaterThan(param1, param2);
      }
    }, 
    GTE(">=") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.greaterThanEq(param1, param2);
      }
    }, 
    EQ("=") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.equals(param1, param2);
      }
    }, 
    NE("<>") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.notEquals(param1, param2);
      }
    };

    private final String _str;

    private CompOp(String str) {
      _str = str;
    }

    @Override
    public String toString() {
      return _str;
    }

    public abstract Value eval(Value param1, Value param2);
  }

  private enum LogOp implements OpType {
    AND("And") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.and(param1, param2);
      }
    }, 
    OR("Or") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.or(param1, param2);
      }
    }, 
    EQV("Eqv") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.eqv(param1, param2);
      }
    }, 
    XOR("Xor") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.xor(param1, param2);
      }
    },
    IMP("Imp") {
      @Override public Value eval(Value param1, Value param2) {
        return BuiltinOperators.imp(param1, param2);
      }
    };

    private final String _str;

    private LogOp(String str) {
      _str = str;
    }

    @Override
    public String toString() {
      return _str;
    }

    public abstract Value eval(Value param1, Value param2);
  }

  private enum SpecOp implements OpType {
    // note, "NOT" is not actually used as a special operation, always
    // replaced with UnaryOp.NOT
    NOT("Not") {
      @Override public Value eval(Value param1, Object param2, Object param3) {
        throw new UnsupportedOperationException();
      }
    }, 
    IS_NULL("Is Null") {
      @Override public Value eval(Value param1, Object param2, Object param3) {
        return BuiltinOperators.isNull(param1);
      }
    }, 
    IS_NOT_NULL("Is Not Null") {
      @Override public Value eval(Value param1, Object param2, Object param3) {
        return BuiltinOperators.isNotNull(param1);
      }
    }, 
    LIKE("Like") {
      @Override public Value eval(Value param1, Object param2, Object param3) {
        return BuiltinOperators.like(param1, (Pattern)param2);
      }
    }, 
    BETWEEN("Between") {
      @Override public Value eval(Value param1, Object param2, Object param3) {
        return BuiltinOperators.between(param1, (Value)param2, (Value)param3);
      }
    }, 
    NOT_BETWEEN("Not Between") {
      @Override public Value eval(Value param1, Object param2, Object param3) {
        return BuiltinOperators.notBetween(param1, (Value)param2, (Value)param3);
      }
    }, 
    IN("In") {
      @Override public Value eval(Value param1, Object param2, Object param3) {
        return BuiltinOperators.in(param1, (Value[])param2);
      }
    }, 
    NOT_IN("Not In") {
      @Override public Value eval(Value param1, Object param2, Object param3) {
        return BuiltinOperators.notIn(param1, (Value[])param2);
      }
    };

    private final String _str;

    private SpecOp(String str) {
      _str = str;
    }

    @Override
    public String toString() {
      return _str;
    }

    public abstract Value eval(Value param1, Object param2, Object param3);
  }

  private static final Map<OpType, Integer> PRECENDENCE = 
    buildPrecedenceMap(
        new OpType[]{BinaryOp.EXP},
        new OpType[]{UnaryOp.NEG},
        new OpType[]{BinaryOp.MULT, BinaryOp.DIV},
        new OpType[]{BinaryOp.INT_DIV},
        new OpType[]{BinaryOp.MOD},
        new OpType[]{BinaryOp.PLUS, BinaryOp.MINUS},
        new OpType[]{BinaryOp.CONCAT},
        new OpType[]{CompOp.LT, CompOp.GT, CompOp.NE, CompOp.LTE, CompOp.GTE, 
                     CompOp.EQ, SpecOp.LIKE, SpecOp.IS_NULL, SpecOp.IS_NOT_NULL},
        new OpType[]{UnaryOp.NOT},
        new OpType[]{LogOp.AND},
        new OpType[]{LogOp.OR},
        new OpType[]{LogOp.XOR},
        new OpType[]{LogOp.EQV},
        new OpType[]{LogOp.IMP},
        new OpType[]{SpecOp.IN, SpecOp.NOT_IN, SpecOp.BETWEEN, 
                     SpecOp.NOT_BETWEEN});

  private static final Set<Character> REGEX_SPEC_CHARS = new HashSet<Character>(
      Arrays.asList('\\','.','%','=','+', '$','^','|','(',')','{','}','&'));
  

  private static final Expr THIS_COL_VALUE = new Expr() {
    @Override public boolean isPure() {
      return false;
    }
    @Override protected Value eval(RowContext ctx) {
      return ctx.getThisColumnValue();
    }
    @Override protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append("<THIS_COL>");
    }
  };

  private static final Expr NULL_VALUE = new EConstValue(
      BuiltinOperators.NULL_VAL, "Null");
  private static final Expr TRUE_VALUE = new EConstValue(
      BuiltinOperators.TRUE_VAL, "True");
  private static final Expr FALSE_VALUE = new EConstValue(
      BuiltinOperators.FALSE_VAL, "False");

  private Expressionator() 
  {
  }

  static String testTokenize(Type exprType, String exprStr, 
                             ParseContext context) {
    
    if(context == null) {
      context = DEFAULT_PARSE_CONTEXT;
    }
    List<Token> tokens = trimSpaces(
        ExpressionTokenizer.tokenize(exprType, exprStr, context));

    if(tokens == null) {
      // FIXME, NULL_EXPR?
      return null;
    }

    return tokens.toString();
  }

  public static Expression parse(Type exprType, String exprStr, 
                                 ParseContext context) {

    if(context == null) {
      context = DEFAULT_PARSE_CONTEXT;
    }

    // FIXME,restrictions:
    // - default value only accepts simple exprs, otherwise becomes literal text
    // - def val cannot refer to any columns
    // - field validation cannot refer to other columns
    // - record validation cannot refer to outside columns

    List<Token> tokens = trimSpaces(
        ExpressionTokenizer.tokenize(exprType, exprStr, context));

    if(tokens == null) {
      // FIXME, NULL_EXPR?
      return null;
    }

    return parseExpression(new TokBuf(exprType, tokens, context), false);
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
    while(buf.hasNext()) {
      Token t = buf.next();

      switch(t.getType()) {
      case OBJ_NAME:

        parseObjectRefExpression(t, buf);
        break;

      case LITERAL:
        
        buf.setPendingExpr(new ELiteralValue(t.getValueType(), t.getValue(),
                                             t.getDateFormat()));
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

          parseCompOpExpression(t, buf);
          break;

        default:
          throw new RuntimeException("Unexpected OP word type " + wordType);
        }
        
        break;

      case DELIM:

        parseDelimExpression(t, buf);
        break;
        
      case STRING:

        // see if it's a special word?
        wordType = getWordType(t);
        if(wordType == null) {

          // is it a function call?
          if(!maybeParseFuncCallExpression(t, buf)) {

            // is it an object name?
            Token next = buf.peekNext();
            if((next != null) && isObjNameSep(next)) {

              parseObjectRefExpression(t, buf);

            } else {
              
              // FIXME maybe bare obj name, maybe string literal?
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

            parseLogicalOpExpression(t, buf);
            break;

          case CONST:

            parseConstExpression(t, buf);
            break;

          case SPEC_OP_PREFIX:

            parseSpecOpExpression(t, buf);
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

  private static void parseObjectRefExpression(Token firstTok, TokBuf buf) {

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

    buf.setPendingExpr(
        new EObjValue(collectionName, objName, fieldName));
  }
  
  private static void parseDelimExpression(Token firstTok, TokBuf buf) {
    // the only "top-level" delim we expect to find is open paren, and
    // there shouldn't be any pending expression
    if(!isDelim(firstTok, OPEN_PAREN) || buf.hasPendingExpr()) {
      throw new IllegalArgumentException("Unexpected delimiter " + 
                                         firstTok.getValue() + " " + buf);
    }

    Expr subExpr = findParenExprs(buf, false).get(0);
    buf.setPendingExpr(new EParen(subExpr));
  }

  private static boolean maybeParseFuncCallExpression(
      Token firstTok, TokBuf buf) {

    int startPos = buf.curPos();
    boolean foundFunc = false;

    try {
      Token t = buf.peekNext();
      if(!isDelim(t, FUNC_START_DELIM)) {
        // not a function call
        return false;
      }
        
      buf.next();
      List<Expr> params = findParenExprs(buf, true);
      String funcName = firstTok.getValueStr();
      Function func = buf.getFunction(funcName);
      if(func == null) {
        throw new IllegalArgumentException("Could not find function '" + 
                                           funcName + "' " + buf);
      }
      buf.setPendingExpr(new EFunc(func, params));
      foundFunc = true;
      return true;

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
      parseBinaryOpExpression(t, buf);
    } else if(isOp(t, "-")) {
      parseUnaryOpExpression(t, buf);
    } else {
      throw new IllegalArgumentException(
          "Missing left expression for binary operator " + t.getValue() + 
          " " + buf);
    }
  }

  private static void parseBinaryOpExpression(Token firstTok, TokBuf buf) {
    BinaryOp op = getOpType(firstTok, BinaryOp.class);
    Expr leftExpr = buf.takePendingExpr();
    Expr rightExpr = parseExpression(buf, true);

    buf.setPendingExpr(new EBinaryOp(op, leftExpr, rightExpr));
  }

  private static void parseUnaryOpExpression(Token firstTok, TokBuf buf) {
    UnaryOp op = getOpType(firstTok, UnaryOp.class);
    Expr val = parseExpression(buf, true);

    buf.setPendingExpr(new EUnaryOp(op, val));
  }

  private static void parseCompOpExpression(Token firstTok, TokBuf buf) {

    if(!buf.hasPendingExpr()) {
      if(buf.getExprType() == Type.FIELD_VALIDATOR) {
        // comparison operators for field validators can implicitly use
        // the current field value for the left value
        buf.setPendingExpr(THIS_COL_VALUE);
      } else {
        throw new IllegalArgumentException(
            "Missing left expression for comparison operator " + 
            firstTok.getValue() + " " + buf);
      }
    }

    CompOp op = getOpType(firstTok, CompOp.class);
    Expr leftExpr = buf.takePendingExpr();
    Expr rightExpr = parseExpression(buf, true);

    buf.setPendingExpr(new ECompOp(op, leftExpr, rightExpr));
  }

  private static void parseLogicalOpExpression(Token firstTok, TokBuf buf) {

    if(!buf.hasPendingExpr()) {
      throw new IllegalArgumentException(
          "Missing left expression for logical operator " + 
          firstTok.getValue() + " " + buf);
    }

    LogOp op = getOpType(firstTok, LogOp.class);
    Expr leftExpr = buf.takePendingExpr();
    Expr rightExpr = parseExpression(buf, true);

    buf.setPendingExpr(new ELogicalOp(op, leftExpr, rightExpr));
  }

  private static void parseSpecOpExpression(Token firstTok, TokBuf buf) {
    
    SpecOp specOp = getSpecialOperator(firstTok, buf);

    if(specOp == SpecOp.NOT) {
      // this is the unary prefix operator
      parseUnaryOpExpression(firstTok, buf);
      return;
    }

    if(!buf.hasPendingExpr()) {
      if(buf.getExprType() == Type.FIELD_VALIDATOR) {
        // comparison operators for field validators can implicitly use
        // the current field value for the left value
        buf.setPendingExpr(THIS_COL_VALUE);
      } else {
        throw new IllegalArgumentException(
            "Missing left expression for comparison operator " + 
            specOp + " " + buf);
      }
    }

    Expr expr = buf.takePendingExpr();

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
      String patternStr = t.getValueStr();
      Pattern pattern = likePatternToRegex(patternStr, buf);
      specOpExpr = new ELikeOp(specOp, expr, pattern, patternStr);
      break;

    case BETWEEN:
    case NOT_BETWEEN:
      
      // the "rest" of a between expression is of the form "X And Y".  we are
      // going to speculatively parse forward until we find the "And"
      // operator.
      Expr startRangeExpr = null;
      while(true) {

        Expr tmpExpr = parseExpression(buf, true);
        Token tmpT = buf.peekNext();

        if(tmpT == null) {
          // ran out of expression?
          throw new IllegalArgumentException(
              "Missing 'And' for 'Between' expression " + buf);
        }

        if(isString(tmpT, "and")) {
          buf.next();
          startRangeExpr = tmpExpr;
          break;
        }

        // put the pending expression back and try parsing some more
        buf.restorePendingExpr(tmpExpr);
      }

      Expr endRangeExpr = parseExpression(buf, true);

      specOpExpr = new EBetweenOp(specOp, expr, startRangeExpr, endRangeExpr);
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

  private static void parseConstExpression(Token firstTok, TokBuf buf) {
    Expr constExpr = null;
    if("true".equalsIgnoreCase(firstTok.getValueStr())) {
      constExpr = TRUE_VALUE;
    } else if("false".equalsIgnoreCase(firstTok.getValueStr())) {
      constExpr = FALSE_VALUE;
    } else if("null".equalsIgnoreCase(firstTok.getValueStr())) {
      constExpr = NULL_VALUE;
    } else {
      throw new RuntimeException("Unexpected CONST word "
                                 + firstTok.getValue());
    }
    buf.setPendingExpr(constExpr);
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

  private static <T extends Enum<T>> T getOpType(Token t, Class<T> opClazz) {
    String str = t.getValueStr();
    for(T op : opClazz.getEnumConstants()) {
      if(str.equalsIgnoreCase(op.toString())) {
        return op;
      }
    }
    throw new IllegalArgumentException("Unexpected op string " + t.getValueStr());
  }

  private static final class TokBuf
  {
    private final Type _exprType;
    private final List<Token> _tokens;
    private final TokBuf _parent;
    private final int _parentOff;
    private final ParseContext _context;
    private int _pos;
    private Expr _pendingExpr;
    private final boolean _simpleExpr;

    private TokBuf(Type exprType, List<Token> tokens, ParseContext context) {
      this(exprType, false, tokens, null, 0, context);
    }

    private TokBuf(List<Token> tokens, TokBuf parent, int parentOff) {
      this(parent._exprType, parent._simpleExpr, tokens, parent, parentOff,
           parent._context);
    }

    private TokBuf(Type exprType, boolean simpleExpr, List<Token> tokens, 
                   TokBuf parent, int parentOff, ParseContext context) {
      _exprType = exprType;
      _tokens = tokens;
      _parent = parent;
      _parentOff = parentOff;
      _context = context;
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
      _pendingExpr = expr.resolveOrderOfOperations();
    } 

    public void restorePendingExpr(Expr expr) {
      // this is an expression which was previously set, so no need to re-resolve
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

    public Function getFunction(String funcName) {
      return _context.getExpressionFunction(funcName);
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

      if(_pendingExpr != null) {
        sb.append(" [pending '").append(_pendingExpr.toDebugString())
          .append("']");
      }

      return sb.toString();
    } 
  }

  private static boolean isHigherPrecendence(OpType op1, OpType op2) {
    int prec1 = PRECENDENCE.get(op1);
    int prec2 = PRECENDENCE.get(op2);

    // higher preceendence ops have lower numbers
    return (prec1 < prec2);
  }

  private static final Map<OpType, Integer> buildPrecedenceMap(
      OpType[]... opArrs) {
    Map<OpType, Integer> prec = new HashMap<OpType, Integer>();

    int level = 0;
    for(OpType[] ops : opArrs) {
      for(OpType op : ops) {
        prec.put(op, level);
      }
      ++level;
    }

    return prec;
  }

  private static void exprListToString(
      List<Expr> exprs, String sep, StringBuilder sb, boolean isDebug) {
    Iterator<Expr> iter = exprs.iterator();
    iter.next().toString(sb, isDebug);
    while(iter.hasNext()) {
      sb.append(sep);
      iter.next().toString(sb, isDebug);
    }
  }

  private static Value[] exprListToValues(
      List<Expr> exprs, RowContext ctx) {
    Value[] paramVals = new Value[exprs.size()];
    for(int i = 0; i < exprs.size(); ++i) {
      paramVals[i] = exprs.get(i).eval(ctx);
    }
    return paramVals;
  }

  private static Value[] exprListToDelayedValues(
      List<Expr> exprs, RowContext ctx) {
    Value[] paramVals = new Value[exprs.size()];
    for(int i = 0; i < exprs.size(); ++i) {
      paramVals[i] = new DelayedValue(exprs.get(i), ctx);
    }
    return paramVals;
  }

  private static boolean arePure(List<Expr> exprs) {
    for(Expr expr : exprs) {
      if(!expr.isPure()) {
        return false;
      }
    }
    return true;
  }

  private static boolean arePure(Expr... exprs) {
    for(Expr expr : exprs) {
      if(!expr.isPure()) {
        return false;
      }
    }
    return true;
  }

  private static void literalStrToString(String str, StringBuilder sb) {
    sb.append("\"")
      .append(str.replace("\"", "\"\""))
      .append("\"");
  }

  private static Pattern likePatternToRegex(String pattern, Object location) {

    StringBuilder sb = new StringBuilder(pattern.length());

    // Access LIKE pattern supports (note, matching is case-insensitive):
    // - '*' -> 0 or more chars
    // - '?' -> single character
    // - '#' -> single digit
    // - '[...]' -> character class, '[!...]' -> not in char class

    for(int i = 0; i < pattern.length(); ++i) {
      char c = pattern.charAt(i);

      if(c == '*') {
        sb.append(".*");
      } else if(c == '?') {
        sb.append('.');
      } else if(c == '#') {
        sb.append("\\d");
      } else if(c == '[') {

        // find closing brace
        int startPos = i + 1;
        int endPos = -1;
        for(int j = startPos; j < pattern.length(); ++j) {
          if(pattern.charAt(j) == ']') {
            endPos = j;
            break;
          } 
        }

        if(endPos == -1) {
          throw new IllegalArgumentException(
              "Could not find closing bracket in pattern '" + pattern + "' " +
              location);
        }

        String charClass = pattern.substring(startPos, endPos);
        
        if((charClass.length() > 0) && (charClass.charAt(0) == '!')) {
          // this is a negated char class
          charClass = '^' + charClass.substring(1);
        }
        
        sb.append('[').append(charClass).append(']');

      } else if(REGEX_SPEC_CHARS.contains(c)) {
        // this char is special in regexes, so escape it
        sb.append('\\').append(c);
      } else {
        sb.append(c);
      }
    }

    return Pattern.compile(sb.toString(),
                           Pattern.CASE_INSENSITIVE | Pattern.DOTALL | 
                           Pattern.UNICODE_CASE);
  }

  private static Value toLiteralValue(Value.Type valType, Object value, 
                                      DateFormat sdf)
  {
    switch(valType) {
    case STRING:
      return new StringValue((String)value);
    case DATE:
      return new DateValue((Date)value, sdf);
    case TIME:
      return new TimeValue((Date)value, sdf);
    case DATE_TIME:
      return new DateTimeValue((Date)value, sdf);
    case BIG_DEC:
      return new BigDecimalValue((BigDecimal)value);
    default:
      throw new RuntimeException("unexpected literal type " + valType);
    } 
  }

  private interface LeftAssocExpr {
    public OpType getOp();
    public Expr getLeft();
    public void setLeft(Expr left);
  }

  private interface RightAssocExpr {
    public OpType getOp();
    public Expr getRight();
    public void setRight(Expr right);
  }

  private static final class DelayedValue extends BaseDelayedValue
  {
    private final Expr _expr;
    private final RowContext _ctx;

    private DelayedValue(Expr expr, RowContext ctx) {
      _expr = expr;
      _ctx = ctx;
    }

    @Override
    protected Value eval() {
      return _expr.eval(_ctx);
    }
  }
  

  private static abstract class Expr implements Expression
  {
    public Object evalDefault() {
      Value val = eval(null);

      if(val.getType() == Value.Type.NULL) {
        return null;
      }

      // FIXME, booleans seem to go to -1 (true),0 (false) ...?

      return val.get();
    }

    public Boolean evalCondition(RowContext ctx) {
      Value val = eval(ctx);

      if(val.getType() == Value.Type.NULL) {
        return null;
      }

      // FIXME, is this only true for non-numeric...?
      // if(val.getType() != Value.Type.BOOLEAN) {
      //   // a single value as a conditional expression seems to act like an
      //   // implicit "="
      //   // FIXME, what about row validators?
      //   val = BuiltinOperators.equals(val, ctx.getThisColumnValue());
      // }

      return val.getAsBoolean();
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

    protected Expr resolveOrderOfOperations() {

      if(!(this instanceof LeftAssocExpr)) {
        // nothing we can do
        return this;
      }

      // in order to get the precedence right, we need to first associate this
      // expression with the "rightmost" expression preceding it, then adjust
      // this expression "down" (lower precedence) as the precedence of the
      // operations dictates.  since we parse from left to right, the initial
      // "left" value isn't the immediate left expression, instead it's based
      // on how the preceding operator precedence worked out.  we need to
      // adjust "this" expression to the closest preceding expression before
      // we can correctly resolve precedence.

      Expr outerExpr = this;
      final LeftAssocExpr thisExpr = (LeftAssocExpr)this;
      final Expr thisLeft = thisExpr.getLeft();

      // current: <this>{<left>{A op1 B} op2 <right>{C}}
      if(thisLeft instanceof RightAssocExpr) {

        RightAssocExpr leftOp = (RightAssocExpr)thisLeft;
        
        // target: <left>{A op1 <this>{B op2 <right>{C}}}
        
        thisExpr.setLeft(leftOp.getRight());

        // give the new version of this expression an opportunity to further
        // swap (since the swapped expression may itself be a binary
        // expression)
        leftOp.setRight(resolveOrderOfOperations());
        outerExpr = thisLeft;

        // at this point, this expression has been pushed all the way to the
        // rightmost preceding expression (we artifically gave "this" the
        // highest precedence).  now, we want to adjust precedence as
        // necessary (shift it back down if the operator precedence is
        // incorrect).  note, we only need to check precedence against "this",
        // as all other precedence has been resolved in previous parsing
        // rounds.
        if((leftOp.getRight() == this) && 
           !isHigherPrecendence(thisExpr.getOp(), leftOp.getOp())) {

          // doh, "this" is lower (or the same) precedence, restore the
          // original order of things
          leftOp.setRight(thisExpr.getLeft());
          thisExpr.setLeft(thisLeft);
          outerExpr = this;
        }
      }

      return outerExpr;
    }
    
    public abstract boolean isPure();

    protected abstract Value eval(RowContext ctx);

    protected abstract void toExprString(StringBuilder sb, boolean isDebug);
  }

  private static final class EConstValue extends Expr
  {
    private final Value _val;
    private final String _str;

    private EConstValue(Value val, String str) {
      _val = val;
      _str = str;
    }

    @Override
    public boolean isPure() {
      return true;
    }

    @Override 
    protected Value eval(RowContext ctx) {
      return _val;
    }

    @Override 
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append(_str);
    }
  }

  private static final class ELiteralValue extends Expr
  {
    private final Value _val;

    private ELiteralValue(Value.Type valType, Object value,
                          DateFormat sdf) {
      _val = toLiteralValue(valType, value, sdf);
    }

    @Override
    public boolean isPure() {
      return true;
    }

    @Override
    public Value eval(RowContext ctx) {
      return _val;
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      if(_val.getType() == Value.Type.STRING) {
        literalStrToString((String)_val.get(), sb);
      } else if(_val.getType().isTemporal()) {
        sb.append("#").append(_val.getAsString()).append("#");
      } else {
        sb.append(_val.get());
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
    public boolean isPure() {
      return false;
    }

    @Override
    public Value eval(RowContext ctx) {
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

  private static class EParen extends Expr
  {
    private final Expr _expr;

    private EParen(Expr expr) {
      _expr = expr;
    }

    @Override
    public boolean isPure() {
      return _expr.isPure();
    }

    @Override
    protected Value eval(RowContext ctx) {
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
    private final Function _func;
    private final List<Expr> _params;

    private EFunc(Function func, List<Expr> params) {
      _func = func;
      _params = params;
    }

    @Override
    public boolean isPure() {
      return _func.isPure() && arePure(_params);
    }

    @Override
    protected Value eval(RowContext ctx) {
      return _func.eval(exprListToValues(_params, ctx));
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append(_func.getName()).append("(");

      if(!_params.isEmpty()) {
        exprListToString(_params, ",", sb, isDebug);
      }
      
      sb.append(")");
    }
  }

  private static abstract class EBaseBinaryOp extends Expr
    implements LeftAssocExpr, RightAssocExpr
  {
    protected final OpType _op;
    protected Expr _left;
    protected Expr _right;

    private EBaseBinaryOp(OpType op, Expr left, Expr right) {
      _op = op;
      _left = left;
      _right = right;
    }

    @Override
    public boolean isPure() {
      return arePure(_left, _right);
    }
    
    public OpType getOp() {
      return _op;
    }

    public Expr getLeft() {
      return _left;
    }

    public void setLeft(Expr left) {
      _left = left;
    }

    public Expr getRight() {
      return _right;
    }

    public void setRight(Expr right) {
      _right = right;
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
    private EBinaryOp(BinaryOp op, Expr left, Expr right) {
      super(op, left, right);
    }

    @Override
    protected Value eval(RowContext ctx) {
      return ((BinaryOp)_op).eval(_left.eval(ctx), _right.eval(ctx));
    }
  }

  private static class EUnaryOp extends Expr
    implements RightAssocExpr
  {
    private final OpType _op;
    private Expr _expr;

    private EUnaryOp(UnaryOp op, Expr expr) {
      _op = op;
      _expr = expr;
    }

    @Override
    public boolean isPure() {
      return _expr.isPure();
    }

    public OpType getOp() {
      return _op;
    }

    public Expr getRight() {
      return _expr;
    }

    public void setRight(Expr right) {
      _expr = right;
    }

    @Override
    protected Value eval(RowContext ctx) {
      return ((UnaryOp)_op).eval(_expr.eval(ctx));
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      sb.append(_op);
      if(isDebug || ((UnaryOp)_op).needsSpace()) {
        sb.append(" ");
      }
      _expr.toString(sb, isDebug);
    }
  } 

  private static class ECompOp extends EBaseBinaryOp
  {
    private ECompOp(CompOp op, Expr left, Expr right) {
      super(op, left, right);
    }

    @Override
    protected Value eval(RowContext ctx) {
      return ((CompOp)_op).eval(_left.eval(ctx), _right.eval(ctx));
    }
  }

  private static class ELogicalOp extends EBaseBinaryOp
  {
    private ELogicalOp(LogOp op, Expr left, Expr right) {
      super(op, left, right);
    }

    @Override
    protected Value eval(final RowContext ctx) {
      
      // logical operations do short circuit evaluation, so we need to delay
      // computing results until necessary
      return ((LogOp)_op).eval(new DelayedValue(_left, ctx),
                               new DelayedValue(_right, ctx));
    }
  } 

  private static abstract class ESpecOp extends Expr
    implements LeftAssocExpr
  {
    protected final SpecOp _op;
    protected Expr _expr;

    private ESpecOp(SpecOp op, Expr expr) {
      _op = op;
      _expr = expr;
    }

    @Override
    public boolean isPure() {
      return _expr.isPure();
    }

    public OpType getOp() {
      return _op;
    }

    public Expr getLeft() {
      return _expr;
    }

    public void setLeft(Expr left) {
      _expr = left;
    }
  }

  private static class ENullOp extends ESpecOp
  {
    private ENullOp(SpecOp op, Expr expr) {
      super(op, expr);
    }

    @Override
    protected Value eval(RowContext ctx) {
      return _op.eval(_expr.eval(ctx), null, null);
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      _expr.toString(sb, isDebug);
      sb.append(" ").append(_op);
    }
  }

  private static class ELikeOp extends ESpecOp
  {
    // FIXME, compile Pattern on first use?
    private final Pattern _pattern;
    private final String _patternStr;

    private ELikeOp(SpecOp op, Expr expr, Pattern pattern, String patternStr) {
      super(op, expr);
      _pattern = pattern;
      _patternStr = patternStr;
    }

    @Override
    protected Value eval(RowContext ctx) {
      return _op.eval(_expr.eval(ctx), _pattern, null);
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      _expr.toString(sb, isDebug);
      sb.append(" ").append(_op).append(" ");
      literalStrToString(_patternStr, sb);
      if(isDebug) {
        sb.append("(").append(_pattern).append(")");
      }
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
    public boolean isPure() {
      return super.isPure() && arePure(_exprs);
    }

    @Override
    protected Value eval(RowContext ctx) {
      return _op.eval(_expr.eval(ctx), 
                      exprListToDelayedValues(_exprs, ctx), null);
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      _expr.toString(sb, isDebug);
      sb.append(" ").append(_op).append(" (");
      exprListToString(_exprs, ",", sb, isDebug);
      sb.append(")");
    }
  }

  private static class EBetweenOp extends ESpecOp
    implements RightAssocExpr
  {
    private final Expr _startRangeExpr;
    private Expr _endRangeExpr;

    private EBetweenOp(SpecOp op, Expr expr, Expr startRangeExpr,
                       Expr endRangeExpr) {
      super(op, expr);
      _startRangeExpr = startRangeExpr;
      _endRangeExpr = endRangeExpr;
    }

    @Override
    public boolean isPure() {
      return _expr.isPure() && arePure(_startRangeExpr, _endRangeExpr);
    }

    public Expr getRight() {
      return _endRangeExpr;
    }

    public void setRight(Expr right) {
      _endRangeExpr = right;
    }

    @Override
    protected Value eval(RowContext ctx) {
      return _op.eval(_expr.eval(ctx), 
                      new DelayedValue(_startRangeExpr, ctx),
                      new DelayedValue(_endRangeExpr, ctx));
    }

    @Override
    protected void toExprString(StringBuilder sb, boolean isDebug) {
      _expr.toString(sb, isDebug);
      sb.append(" ").append(_op).append(" ");
      _startRangeExpr.toString(sb, isDebug);
      sb.append(" And ");
      _endRangeExpr.toString(sb, isDebug);
    }
  }
}
