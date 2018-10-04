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
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static com.healthmarketscience.jackcess.impl.expr.Expressionator.*;
import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.ParseException;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;


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

  private static final int AMPM_SUFFIX_LEN = 3;
  private static final String AM_SUFFIX = " am";
  private static final String PM_SUFFIX = " pm";
  // access times are based on this date (not the UTC base)
  private static final String BASE_DATE = "12/30/1899";
  private static final String BASE_DATE_FMT = "M/d/yyyy";

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

    if((exprStr == null) || (exprStr.length() == 0)) {
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

  private static String parseQuotedString(ExprBuf buf, char quoteChar) {
    return parseStringUntil(buf, quoteChar, null, true);
  }

  private static String parseObjNameString(ExprBuf buf) {
    return parseStringUntil(buf, OBJ_NAME_END_CHAR, OBJ_NAME_START_CHAR, false);
  }

  private static String parseDateLiteralString(ExprBuf buf) {
    return parseStringUntil(buf, DATE_LIT_QUOTE_CHAR, null, false);
  }

  private static String parseStringUntil(ExprBuf buf, char endChar,
                                         Character startChar,
                                         boolean allowDoubledEscape)
  {
    StringBuilder sb = buf.getScratchBuffer();

    boolean complete = false;
    while(buf.hasNext()) {
      char c = buf.next();
      if(c == endChar) {
        if(allowDoubledEscape && (buf.peekNext() == endChar)) {
          sb.append(endChar);
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

    return sb.toString();
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
    DateFormat parseDf = buf.getParseDateTimeFormat(type);

    try {
      return new Token(TokenType.LITERAL, parseComplete(parseDf, dateStr),
                       dateStr, type.getValueType());
    } catch(java.text.ParseException pe) {
      throw new ParseException(
          "Invalid date/time literal " + dateStr + " " + buf, pe);
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
      int strLen = dateStr.length();
      hasAmPm = ((strLen >= AMPM_SUFFIX_LEN) &&
                 (dateStr.regionMatches(true, strLen - AMPM_SUFFIX_LEN,
                                        AM_SUFFIX, 0, AMPM_SUFFIX_LEN) ||
                  dateStr.regionMatches(true, strLen - AMPM_SUFFIX_LEN,
                                        PM_SUFFIX, 0, AMPM_SUFFIX_LEN)));
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

  static DateFormat createParseDateTimeFormat(TemporalConfig.Type type,
                                              LocaleContext ctx)
  {
    switch(type) {
    case TIME:
      return createParseTimeFormat(TemporalConfig.Type.DATE_TIME, ctx);
    case TIME_12:
      return createParseTimeFormat(TemporalConfig.Type.DATE_TIME_12, ctx);
    case TIME_24:
      return createParseTimeFormat(TemporalConfig.Type.DATE_TIME_24, ctx);
    default:
      // use normal formatter
    }

    TemporalConfig cfg = ctx.getTemporalConfig();
    return ctx.createDateFormat(cfg.getDateTimeFormat(type));
  }

  private static DateFormat createParseTimeFormat(TemporalConfig.Type parseType,
                                                  LocaleContext ctx)
  {
    TemporalConfig cfg = ctx.getTemporalConfig();
    // we need to use a special DateFormat impl which manipulates the parsed
    // time-only value so it becomes the right Date value
    String baseDate = getBaseDatePrefix(ctx);
    DateFormat parseDf = ctx.createDateFormat(
        cfg.getDateTimeFormat(parseType));
    return new ParseTimeFormat(parseDf, baseDate);
  }

  private static String getBaseDatePrefix(LocaleContext ctx) {
    String dateFmt = ctx.getTemporalConfig().getDateFormat();
    String baseDate = BASE_DATE;
    if(!BASE_DATE_FMT.equals(dateFmt)) {
      try {
        // need to reformat the base date to the relevant date format
        DateFormat parseDf = ctx.createDateFormat(BASE_DATE_FMT);
        DateFormat df = ctx.createDateFormat(dateFmt);
        baseDate = df.format(parseComplete(parseDf, baseDate));
      } catch(Exception e) {
        throw new ParseException("Could not parse base date", e);
      }
    }
    return baseDate + " ";
  }

  private static Token maybeParseNumberLiteral(char firstChar, ExprBuf buf) {
    StringBuilder sb = buf.getScratchBuffer().append(firstChar);
    boolean hasDigit = isDigit(firstChar);

    int startPos = buf.curPos();
    boolean foundNum = false;
    boolean isFp = false;
    int expPos = -1;

    try {

      int c = EOF;
      while((c = buf.peekNext()) != EOF) {
        if(isDigit(c)) {
          hasDigit = true;
          sb.append((char)c);
          buf.next();
        } else if(c == '.') {
          isFp = true;
          sb.append((char)c);
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

  static Date parseComplete(DateFormat df, String str)
    throws java.text.ParseException
  {
    // the java parsers will parse "successfully" even if there is leftover
    // information.  we only want to consider a parse operation successful if
    // it parses the entire string (ignoring surrounding whitespace)
    str = str.trim();
    ParsePosition pp = new ParsePosition(0);
    Object d = df.parse(str, pp);
    if(pp.getIndex() < str.length()) {
      throw new java.text.ParseException("Failed parsing '" + str + "'",
                                         pp.getIndex());
    }
    return (Date)d;
  }

  private static final class ExprBuf
  {
    private final String _str;
    private final ParseContext _ctx;
    private int _pos;
    private final Map<TemporalConfig.Type,DateFormat> _dateTimeFmts =
      new EnumMap<TemporalConfig.Type,DateFormat>(TemporalConfig.Type.class);
    private final StringBuilder _scratch = new StringBuilder();

    private ExprBuf(String str, ParseContext ctx) {
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

    public DateFormat getParseDateTimeFormat(TemporalConfig.Type type) {
      DateFormat df = _dateTimeFmts.get(type);
      if(df == null) {
        df = createParseDateTimeFormat(type, _ctx);
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

  /**
   * Special date/time format which will parse time-only strings "correctly"
   * according to how access handles time-only values.
   */
  private static final class ParseTimeFormat extends DateFormat
  {
    private static final long serialVersionUID = 0L;

    private final DateFormat _parseDelegate;
    private final String _baseDate;

    private ParseTimeFormat(DateFormat parseDelegate, String baseDate)
    {
      _parseDelegate = parseDelegate;
      _baseDate = baseDate;
    }

    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date parse(String source, ParsePosition pos) {
      // we parse as a full date/time in order to get the correct "base date"
      // used by access
      return _parseDelegate.parse(_baseDate + source, pos);
    }

    @Override
    public Calendar getCalendar() {
      return _parseDelegate.getCalendar();
    }

    @Override
    public TimeZone getTimeZone() {
      return _parseDelegate.getTimeZone();
    }
  }

}
