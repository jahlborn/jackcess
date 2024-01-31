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
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.healthmarketscience.jackcess.impl.expr.Expressionator.*;
import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.ParseException;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.util.StringUtil;


/**
 *
 * @author James Ahlborn
 */
class ExpressionTokenizer
{
  private static final int EOF = -1;
  static final char QUOTED_STR_CHAR = '"';
  private static final char SINGLE_QUOTED_STR_CHAR = '\'';
  private static final char OBJ_NAME_START_CHAR = '[';
  private static final char OBJ_NAME_END_CHAR = ']';
  private static final char DATE_LIT_QUOTE_CHAR = '#';
  private static final char EQUALS_CHAR = '=';

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
    setCharFlag(IS_QUOTE_FLAG, '"', '#', '[', ']', '\'');
  }

  private ExpressionTokenizer() {}

  /**
   * Tokenizes an expression string of the given type and (optionally) in the
   * context of the relevant database.
   */
  static List<Token> tokenize(Type exprType, String exprStr,
                              ParseContext context) {

    if(exprStr != null) {
      exprStr = exprStr.trim();
    }

    if(StringUtil.isEmpty(exprStr)) {
      return null;
    }

    List<Token> tokens = new ArrayList<Token>();

    ExprBuf buf = new ExprBuf(exprStr, context);

    while(buf.hasNext()) {
      char c = buf.next();

      byte charFlag = getCharFlag(c);
      if(charFlag != 0) {

        // what could it be?
        switch(charFlag) {
        case IS_OP_FLAG:

          // all simple operator chars are single character operators
          tokens.add(new Token(TokenType.OP, String.valueOf(c)));
          break;

        case IS_COMP_FLAG:

          // special case for default values
          if((exprType == Type.DEFAULT_VALUE) && (c == EQUALS_CHAR) &&
             (buf.prevPos() == 0)) {
            // a leading equals sign indicates how a default value should be
            // evaluated
            tokens.add(new Token(TokenType.OP, String.valueOf(c)));
            continue;
          }

          tokens.add(new Token(TokenType.OP, parseCompOp(c, buf)));
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
          case SINGLE_QUOTED_STR_CHAR:
            tokens.add(new Token(TokenType.LITERAL, null,
                                 parseQuotedString(buf, c), Value.Type.STRING));
            break;
          case DATE_LIT_QUOTE_CHAR:
            tokens.add(parseDateLiteral(buf));
            break;
          case OBJ_NAME_START_CHAR:
            tokens.add(new Token(TokenType.OBJ_NAME, parseObjNameString(buf)));
            break;
          default:
            throw new ParseException(
                "Invalid leading quote character " + c + " " + buf);
          }

          break;

        default:
          throw new RuntimeException("unknown char flag " + charFlag);
        }

      } else {

        if(isDigit(c)) {
          Token numLit = maybeParseNumberLiteral(c, buf, context);
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

  private static String parseQuotedString(ExprBuf buf, char quoteChar) {
    return parseStringUntil(buf, null, quoteChar, true);
  }

  private static String parseObjNameString(ExprBuf buf) {
    return parseStringUntil(buf, OBJ_NAME_START_CHAR, OBJ_NAME_END_CHAR, false);
  }

  private static String parseDateLiteralString(ExprBuf buf) {
    return parseStringUntil(buf, null, DATE_LIT_QUOTE_CHAR, false);
  }

  static String parseStringUntil(ExprBuf buf, Character startChar,
                                 char endChar, boolean allowDoubledEscape)
  {
    return parseStringUntil(buf, startChar, endChar, allowDoubledEscape,
                            buf.getScratchBuffer())
      .toString();
  }

  static StringBuilder parseStringUntil(
      ExprBuf buf, Character startChar, char endChar, boolean allowDoubledEscape,
      StringBuilder sb)
  {
    boolean complete = false;
    while(buf.hasNext()) {
      char c = buf.next();
      if(c == endChar) {
        if(allowDoubledEscape && (buf.peekNext() == endChar)) {
          buf.next();
        } else {
          complete = true;
          break;
        }
      } else if((startChar != null) &&
                (startChar == c)) {
        throw new ParseException("Missing closing '" + endChar +
                                 "' for quoted string " + buf);
      }

      sb.append(c);
    }

    if(!complete) {
      throw new ParseException("Missing closing '" + endChar +
                               "' for quoted string " + buf);
    }

    return sb;
  }

  private static Token parseDateLiteral(ExprBuf buf)
  {
    String dateStr = parseDateLiteralString(buf);

    TemporalConfig.Type type = determineDateType(
        dateStr, buf.getContext());
    if(type == null) {
      throw new ParseException("Invalid date/time literal " + dateStr +
                               " " + buf);
    }

    // note that although we may parse in the time "24" format, we will
    // display as the default time format
    DateTimeFormatter parseDf = buf.getParseDateTimeFormat(type);

    try {
      TemporalAccessor parsedInfo = parseDf.parse(dateStr);

      LocalDate ld = ColumnImpl.BASE_LD;
      if(type.includesDate()) {
        ld = LocalDate.from(parsedInfo);
      }

      LocalTime lt = ColumnImpl.BASE_LT;
      if(type.includesTime()) {
        lt = LocalTime.from(parsedInfo);
      }

      return new Token(TokenType.LITERAL, LocalDateTime.of(ld, lt),
                       dateStr, type.getValueType());
    } catch(DateTimeException de) {
      throw new ParseException(
          "Invalid date/time literal " + dateStr + " " + buf, de);
    }
  }

  static TemporalConfig.Type determineDateType(
      String dateStr, LocaleContext ctx)
  {
    TemporalConfig cfg = ctx.getTemporalConfig();
    boolean hasDate = (dateStr.indexOf(cfg.getDateSeparator()) >= 0);
    boolean hasTime = (dateStr.indexOf(cfg.getTimeSeparator()) >= 0);
    boolean hasAmPm = false;

    if(hasTime) {
      String[] amPmStrs = cfg.getAmPmStrings();
      String amStr = " " + amPmStrs[0];
      String pmStr = " " + amPmStrs[1];
      hasAmPm = (hasSuffix(dateStr, amStr) || hasSuffix(dateStr, pmStr));
    }

    if(hasDate) {
      if(hasTime) {
        return (hasAmPm ? TemporalConfig.Type.DATE_TIME_12 :
                TemporalConfig.Type.DATE_TIME_24);
      }
      return TemporalConfig.Type.DATE;
    } else if(hasTime) {
      return (hasAmPm ? TemporalConfig.Type.TIME_12 :
              TemporalConfig.Type.TIME_24);
    }
    return null;
  }

  private static boolean hasSuffix(String str, String suffStr) {
    int strLen = str.length();
    int suffStrLen = suffStr.length();
    return ((strLen >= suffStrLen) &&
            str.regionMatches(true, strLen - suffStrLen,
                              suffStr, 0, suffStrLen));
  }

  private static Token maybeParseNumberLiteral(
      char firstChar, ExprBuf buf, ParseContext context) {
    StringBuilder sb = buf.getScratchBuffer().append(firstChar);
    boolean hasDigit = isDigit(firstChar);

    int startPos = buf.curPos();
    boolean foundNum = false;
    boolean isFp = false;
    int expPos = -1;
    char decimalSep = context.getNumericConfig().getDecimalFormatSymbols()
      .getDecimalSeparator();

    try {

      int c = EOF;
      while((c = buf.peekNext()) != EOF) {
        if(isDigit(c)) {
          hasDigit = true;
          sb.append((char)c);
          buf.next();
        } else if(c == decimalSep) {
          isFp = true;
          // we handle a localized decimal separator for input, but the code
          // below will parse using non-locale symbols
          sb.append('.');
          buf.next();
        } else if(hasDigit && (expPos < 0) && ((c == 'e') || (c == 'E'))) {
          isFp = true;
          sb.append((char)c);
          expPos = sb.length();
          buf.next();
        } else if((expPos == sb.length()) && ((c == '-') || (c == '+'))) {
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
        Number num = null;
        Value.Type numType = null;

        if(!isFp) {
          try {
            // try to parse as int.  if that fails, fall back to BigDecimal
            // (this will handle the case of int overflow)
            num = Integer.valueOf(numStr);
            numType = Value.Type.LONG;
          } catch(NumberFormatException ne) {
            // fallback to decimal
          }
        }

        if(num == null) {
          num = new BigDecimal(numStr);
          numType = Value.Type.BIG_DEC;
        }

        foundNum = true;
        return new Token(TokenType.LITERAL, num, numStr, numType);
      } catch(NumberFormatException ne) {
        throw new ParseException(
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

  static final class ExprBuf
  {
    private final String _str;
    private final ParseContext _ctx;
    private int _pos;
    private final Map<TemporalConfig.Type,DateTimeFormatter> _dateTimeFmts =
      new EnumMap<TemporalConfig.Type,DateTimeFormatter>(
          TemporalConfig.Type.class);
    private final StringBuilder _scratch = new StringBuilder();

    ExprBuf(String str, ParseContext ctx) {
      _str = str;
      _ctx = ctx;
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

    public ParseContext getContext() {
      return _ctx;
    }

    public DateTimeFormatter getParseDateTimeFormat(TemporalConfig.Type type) {
      DateTimeFormatter df = _dateTimeFmts.get(type);
      if(df == null) {
        df = _ctx.createDateFormatter(
            _ctx.getTemporalConfig().getDateTimeFormat(type));
        _dateTimeFmts.put(type, df);
      }
      return df;
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
    private final Value.Type _valType;

    private Token(TokenType type, String val) {
      this(type, val, val);
    }

    private Token(TokenType type, Object val, String valStr) {
      this(type, val, valStr, null);
    }

    private Token(TokenType type, Object val, String valStr, Value.Type valType) {
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

    public Value.Type getValueType() {
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
