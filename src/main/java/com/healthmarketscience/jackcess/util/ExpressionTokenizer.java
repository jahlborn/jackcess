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

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.healthmarketscience.jackcess.util.Expressionator.*;
import com.healthmarketscience.jackcess.util.Expression.ValueType;


/**
 *
 * @author James Ahlborn
 */
class ExpressionTokenizer 
{
  private static final int EOF = -1;
  private static final char QUOTED_STR_CHAR = '"';
  private static final char OBJ_NAME_START_CHAR = '[';
  private static final char OBJ_NAME_END_CHAR = ']';
  private static final char DATE_LIT_QUOTE_CHAR = '#';
  private static final char EQUALS_CHAR = '=';

  private static final String DATE_FORMAT = "M/d/yyyy";
  private static final String TIME_FORMAT = "HH:mm:ss";
  private static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

  private static final byte IS_OP_FLAG =     0x01;
  private static final byte IS_COMP_FLAG =   0x02;
  private static final byte IS_DELIM_FLAG =  0x04;
  private static final byte IS_SPACE_FLAG =  0x08;
  private static final byte IS_QUOTE_FLAG =  0x10;

  enum TokenType {
    OBJ_NAME, LITERAL, OP, DELIM, STRING, SPACE;
  }

  private static final byte[] CHAR_FLAGS = new byte[128];
  private static final Set<String> TWO_CHAR_COMP_OPS = new HashSet<String>(
      Arrays.asList("<=", ">=", "<>"));

  static {
    setCharFlag(IS_OP_FLAG, '+', '-', '*', '/', '\\', '^', '&');
    setCharFlag(IS_COMP_FLAG, '<', '>', '=');
    setCharFlag(IS_DELIM_FLAG, '.', '!', ',', '(', ')');
    setCharFlag(IS_SPACE_FLAG, ' ', '\n', '\r', '\t');
    setCharFlag(IS_QUOTE_FLAG, '"', '#', '[', ']');
  }

  /**
   * Tokenizes an expression string of the given type and (optionally) in the
   * context of the relevant database.
   */
  static List<Token> tokenize(Type exprType, String exprStr,
                              ParseContext context) {

    if(exprStr != null) {
      exprStr = exprStr.trim();
    }

    if((exprStr == null) || (exprStr.length() == 0)) {
      return null;
    }

    List<Token> tokens = new ArrayList<Token>();

    ExprBuf buf = new ExprBuf(exprStr);

    while(buf.hasNext()) {
      char c = buf.next();
      
      byte charFlag = getCharFlag(c);
      if(charFlag != 0) {
        
        // what could it be?
        switch(charFlag) {
        case IS_OP_FLAG:

          // special case '-' for negative number
          Token numLit = maybeParseNumberLiteral(c, buf);
          if(numLit != null) {
            tokens.add(numLit);
            continue;
          }
          
          // all simple operator chars are single character operators
          tokens.add(new Token(TokenType.OP, String.valueOf(c)));
          break;

        case IS_COMP_FLAG:

          switch(exprType) {
          case DEFAULT_VALUE:

            // special case
            if((c == EQUALS_CHAR) && (buf.prevPos() == 0)) {
              // a leading equals sign indicates how a default value should be
              // evaluated
              tokens.add(new Token(TokenType.OP, String.valueOf(c)));
              continue;
            }
            // def values can't have cond at top level
            throw new IllegalArgumentException(
                exprType + " cannot have top-level conditional " + buf);

          case FIELD_VALIDATOR:
          case RECORD_VALIDATOR:

            tokens.add(new Token(TokenType.OP, parseCompOp(c, buf)));
            break;
          }

          break;

        case IS_DELIM_FLAG:

          // all delimiter chars are single character symbols
          tokens.add(new Token(TokenType.DELIM, String.valueOf(c)));
          break;

        case IS_SPACE_FLAG:

          // normalize whitespace into single space
          consumeWhitespace(buf);
          tokens.add(new Token(TokenType.SPACE, " "));
          break;

        case IS_QUOTE_FLAG:

          switch(c) {
          case QUOTED_STR_CHAR:
            tokens.add(new Token(TokenType.LITERAL, null, parseQuotedString(buf),
                                 ValueType.STRING));
            break;
          case DATE_LIT_QUOTE_CHAR:
            tokens.add(parseDateLiteralString(buf, context));
            break;
          case OBJ_NAME_START_CHAR:
            tokens.add(new Token(TokenType.OBJ_NAME, parseObjNameString(buf)));
            break;
          default:
            throw new IllegalArgumentException(
                "Invalid leading quote character " + c + " " + buf);
          }

          break;

        default:
          throw new RuntimeException("unknown char flag " + charFlag);
        }

      } else {

        if(isDigit(c)) {
          Token numLit = maybeParseNumberLiteral(c, buf);
          if(numLit != null) {
            tokens.add(numLit);
            continue;
          }
        }

        // standalone word of some sort
        String str = parseBareString(c, buf, exprType);
        tokens.add(new Token(TokenType.STRING, str));
      }

    }

    return tokens;
  }

  private static byte getCharFlag(char c) {
    return ((c < 128) ? CHAR_FLAGS[c] : 0);
  }

  private static boolean isSpecialChar(char c) {
    return (getCharFlag(c) != 0);
  }

  private static String parseCompOp(char firstChar, ExprBuf buf) {
    String opStr = String.valueOf(firstChar);

    int c = buf.peekNext();
    if((c != EOF) && hasFlag(getCharFlag((char)c), IS_COMP_FLAG)) {

      // is the combo a valid comparison operator?
      String tmpStr = opStr + (char)c;
      if(TWO_CHAR_COMP_OPS.contains(tmpStr)) {
        opStr = tmpStr;
        buf.next();
      }
    }

    return opStr;
  }

  private static void consumeWhitespace(ExprBuf buf) {
    int c = EOF;
    while(((c = buf.peekNext()) != EOF) && 
          hasFlag(getCharFlag((char)c), IS_SPACE_FLAG)) {
        buf.next();
    }
  }
  
  private static String parseBareString(char firstChar, ExprBuf buf,
                                        Type exprType) {
    StringBuilder sb = buf.getScratchBuffer().append(firstChar);

    byte stopFlags = (IS_OP_FLAG | IS_DELIM_FLAG | IS_SPACE_FLAG);
    if(exprType == Type.FIELD_VALIDATOR) {
      stopFlags |= IS_COMP_FLAG;
    }

    while(buf.hasNext()) {
      char c = buf.next();
      byte charFlag = getCharFlag(c);
      if(hasFlag(charFlag, stopFlags)) {
        buf.popPrev();
        break;
      }
      sb.append(c);
    }
    
    return sb.toString();
  }

  private static String parseQuotedString(ExprBuf buf) {
    StringBuilder sb = buf.getScratchBuffer();

    boolean complete = false;
    while(buf.hasNext()) {
      char c = buf.next();
      if(c == QUOTED_STR_CHAR) {
        int nc = buf.peekNext();
        if(nc == QUOTED_STR_CHAR) {
          sb.append(QUOTED_STR_CHAR);
          buf.next();
        } else {
          complete = true;
          break;
        }
      }

      sb.append(c);
    }

    if(!complete) {
      throw new IllegalArgumentException("Missing closing '" + QUOTED_STR_CHAR + 
                                         "' for quoted string " + buf);
    }

    return sb.toString();
  }

  private static String parseObjNameString(ExprBuf buf) {
    return parseStringUntil(buf, OBJ_NAME_END_CHAR, OBJ_NAME_START_CHAR);
  }

  private static String parseStringUntil(ExprBuf buf, char endChar, 
                                         Character startChar) 
  {
    StringBuilder sb = buf.getScratchBuffer();

    boolean complete = false;
    while(buf.hasNext()) {
      char c = buf.next();
      if(c == endChar) {
        complete = true;
        break;
      } else if((startChar != null) &&
                (startChar == c)) {
        throw new IllegalArgumentException("Missing closing '" + endChar +
                                           "' for quoted string " + buf);
      }

      sb.append(c);
    }

    if(!complete) {
      throw new IllegalArgumentException("Missing closing '" + endChar +
                                         "' for quoted string " + buf);
    }

    return sb.toString();
  }

  private static Token parseDateLiteralString(
      ExprBuf buf, ParseContext context) 
  {
    String dateStr = parseStringUntil(buf, DATE_LIT_QUOTE_CHAR, null);
    
    boolean hasDate = (dateStr.indexOf('/') >= 0);
    boolean hasTime = (dateStr.indexOf(':') >= 0);

    SimpleDateFormat sdf = null;
    ValueType valType = null;
    if(hasDate && hasTime) {
      sdf = buf.getDateTimeFormat(context);
      valType = ValueType.DATE_TIME;
    } else if(hasDate) {
      sdf = buf.getDateFormat(context);
      valType = ValueType.DATE;
    } else if(hasTime) {
      sdf = buf.getTimeFormat(context);
      valType = ValueType.TIME;
    } else {
      throw new IllegalArgumentException("Invalid date time literal " + dateStr +
                                         " " + buf);
    }

    try {
      return new Token(TokenType.LITERAL, sdf.parse(dateStr), dateStr, valType);
    } catch(ParseException pe) {
      throw new IllegalArgumentException(       
          "Invalid date time literal " + dateStr + " " + buf, pe);
    }
  }

  private static Token maybeParseNumberLiteral(char firstChar, ExprBuf buf) {
    StringBuilder sb = buf.getScratchBuffer().append(firstChar);
    boolean hasDigit = isDigit(firstChar);

    int startPos = buf.curPos();
    boolean foundNum = false;

    try {

      int c = EOF;
      while((c = buf.peekNext()) != EOF) {
        if(isDigit(c)) {
          hasDigit = true;
          sb.append((char)c);
          buf.next();
        } else if(c == '.') {
          sb.append((char)c);
          buf.next();
        } else if(isSpecialChar((char)c)) {
          break;
        } else {
          // found a non-number, non-special string
          return null;
        }
      }

      if(!hasDigit) {
        // no digits, no number
        return null;
      }

      String numStr = sb.toString();
      try {
        // what number type to use here?
        BigDecimal num = new BigDecimal(numStr);
        foundNum = true;
        return new Token(TokenType.LITERAL, num, numStr, ValueType.BIG_DEC);
      } catch(NumberFormatException ne) {
        throw new IllegalArgumentException(
            "Invalid number literal " + numStr + " " + buf, ne);
      }
      
    } finally {
      if(!foundNum) {
        buf.reset(startPos);
      }
    }
  }

  private static boolean hasFlag(byte charFlag, byte flag) {
    return ((charFlag & flag) != 0);
  }

  private static void setCharFlag(byte flag, char... chars) {
    for(char c : chars) {
      CHAR_FLAGS[c] |= flag;
    }
  }

  private static boolean isDigit(int c) {
    return ((c >= '0') && (c <= '9'));
  }

  static <K,V> Map.Entry<K,V> newEntry(K a, V b) {
    return new AbstractMap.SimpleImmutableEntry<K,V>(a, b);
  }

  private static final class ExprBuf
  {
    private final String _str;
    private int _pos;
    private SimpleDateFormat _dateFmt;
    private SimpleDateFormat _timeFmt;
    private SimpleDateFormat _dateTimeFmt;
    private final StringBuilder _scratch = new StringBuilder();
    
    private ExprBuf(String str) {
      _str = str;
    }

    private int len() {
      return _str.length();
    }

    public int curPos() {
      return _pos;
    }

    public int prevPos() {
      return _pos - 1;
    }

    public boolean hasNext() {
      return _pos < len();
    }

    public char next() {
      return _str.charAt(_pos++);
    }

    public void popPrev() {
      --_pos;
    }

    public int peekNext() {
      if(!hasNext()) {
        return EOF;
      }
      return _str.charAt(_pos);
    }

    public void reset(int pos) {
      _pos = pos;
    }

    public StringBuilder getScratchBuffer() {
      _scratch.setLength(0);
      return _scratch;
    }

    public SimpleDateFormat getDateFormat(ParseContext context) {
      if(_dateFmt == null) {
        _dateFmt = context.createDateFormat(DATE_FORMAT);
      }
      return _dateFmt;
    }

    public SimpleDateFormat getTimeFormat(ParseContext context) {
      if(_timeFmt == null) {
        _timeFmt = context.createDateFormat(TIME_FORMAT);
      }
      return _timeFmt;
    }

    public SimpleDateFormat getDateTimeFormat(ParseContext context) {
      if(_dateTimeFmt == null) {
        _dateTimeFmt = context.createDateFormat(DATE_TIME_FORMAT);
      }
      return _dateTimeFmt;
    }

    @Override
    public String toString() {
      return "[char " + _pos + "] '" + _str + "'";
    }
  }


  static final class Token
  {
    private final TokenType _type;
    private final Object _val;
    private final String _valStr;
    private final ValueType _valType;

    private Token(TokenType type, String val) {
      this(type, val, val);
    }

    private Token(TokenType type, Object val, String valStr) {
      this(type, val, valStr, null);
    }

    private Token(TokenType type, Object val, String valStr, ValueType valType) {
      _type = type;
      _val = ((val != null) ? val : valStr);
      _valStr = valStr;
      _valType = valType;
    }

    public TokenType getType() {
      return _type;
    }

    public Object getValue() {
      return _val;
    }

    public String getValueStr() {
      return _valStr;
    }

    public ValueType getValueType() {
      return _valType;
    }

    @Override
    public String toString() {
      if(_type == TokenType.SPACE) {
        return "' '";
      }
      String str = "[" + _type + "] '" + _val + "'";
      if(_valType != null) {
        str += " (" + _valType + ")";
      }
      return str;
    }
  } 

}
