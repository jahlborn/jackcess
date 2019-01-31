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

package com.healthmarketscience.jackcess.impl.expr;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.NumericConfig;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;
import static com.healthmarketscience.jackcess.impl.expr.ExpressionTokenizer.ExprBuf;

/**
 *
 * @author James Ahlborn
 */
public class FormatUtil
{
  public enum NumPatternType {
    GENERAL,
    CURRENCY {
      @Override
      protected void appendPrefix(StringBuilder fmt) {
        fmt.append('\u00A4');
      }
      @Override
      protected boolean useParensForNegatives(NumericConfig cfg) {
        return cfg.useParensForCurrencyNegatives();
      }
    },
    EURO {
      @Override
      protected void appendPrefix(StringBuilder fmt) {
        fmt.append('\u20AC');
      }
      @Override
      protected boolean useParensForNegatives(NumericConfig cfg) {
        return cfg.useParensForCurrencyNegatives();
      }
    },
    PERCENT {
      @Override
      protected void appendSuffix(StringBuilder fmt) {
        fmt.append('%');
      }
    },
    SCIENTIFIC {
      @Override
      protected void appendSuffix(StringBuilder fmt) {
        fmt.append("E0");
      }
    };

    protected void appendPrefix(StringBuilder fmt) {}

    protected void appendSuffix(StringBuilder fmt) {}

    protected boolean useParensForNegatives(NumericConfig cfg) {
      return cfg.useParensForNegatives();
    }
  }

  private enum TextCase {
    NONE,
    UPPER {
      @Override public char apply(char c) {
        return Character.toUpperCase(c);
      }
    },
    LOWER {
      @Override public char apply(char c) {
        return Character.toLowerCase(c);
      }
    };

    public char apply(char c) {
      return c;
    }
  }

  private static final Map<String,Fmt> PREDEF_FMTS = new HashMap<String,Fmt>();

  static {
    PREDEF_FMTS.put("General Date", args -> ValueSupport.toValue(
                        args.coerceToDateTimeValue().getAsString()));
    PREDEF_FMTS.put("Long Date",
                    new PredefDateFmt(TemporalConfig.Type.LONG_DATE));
    PREDEF_FMTS.put("Medium Date",
                    new PredefDateFmt(TemporalConfig.Type.MEDIUM_DATE));
    PREDEF_FMTS.put("Short Date",
                    new PredefDateFmt(TemporalConfig.Type.SHORT_DATE));
    PREDEF_FMTS.put("Long Time",
                    new PredefDateFmt(TemporalConfig.Type.LONG_TIME));
    PREDEF_FMTS.put("Medium Time",
                    new PredefDateFmt(TemporalConfig.Type.MEDIUM_TIME));
    PREDEF_FMTS.put("Short Time",
                    new PredefDateFmt(TemporalConfig.Type.SHORT_TIME));

    PREDEF_FMTS.put("General Number", args -> ValueSupport.toValue(
                        args.coerceToNumberValue().getAsString()));
    PREDEF_FMTS.put("Currency",
                    new PredefNumberFmt(NumericConfig.Type.CURRENCY));
    PREDEF_FMTS.put("Euro", new PredefNumberFmt(NumericConfig.Type.EURO));
    PREDEF_FMTS.put("Fixed",
                    new PredefNumberFmt(NumericConfig.Type.FIXED));
    PREDEF_FMTS.put("Standard",
                    new PredefNumberFmt(NumericConfig.Type.STANDARD));
    PREDEF_FMTS.put("Percent",
                    new PredefNumberFmt(NumericConfig.Type.PERCENT));
    PREDEF_FMTS.put("Scientific", new ScientificPredefNumberFmt());

    PREDEF_FMTS.put("True/False", new PredefBoolFmt("True", "False"));
    PREDEF_FMTS.put("Yes/No", new PredefBoolFmt("Yes", "No"));
    PREDEF_FMTS.put("On/Off", new PredefBoolFmt("On", "Off"));
  }

  private static final Fmt NULL_FMT = args -> ValueSupport.EMPTY_STR_VAL;

  private static final char QUOTE_CHAR = '"';
  private static final char ESCAPE_CHAR = '\\';
  private static final char LEFT_ALIGN_CHAR = '!';
  private static final char START_COLOR_CHAR = '[';
  private static final char END_COLOR_CHAR = ']';
  private static final char CHOICE_SEP_CHAR = ';';
  // this only seems to be useful if you have fixed length string fields which
  // isn't a normal thing in ms access
  private static final char FILL_ESCAPE_CHAR = '*';
  private static final char REQ_PLACEHOLDER_CHAR = '@';
  private static final char OPT_PLACEHOLDER_CHAR = '&';
  private static final char TO_UPPER_CHAR = '>';
  private static final char TO_LOWER_CHAR = '<';
  private static final char DT_LIT_COLON_CHAR = ':';
  private static final char DT_LIT_SLASH_CHAR = '/';
  private static final char SINGLE_QUOTE_CHAR = '\'';
  private static final char EXP_E_CHAR = 'E';
  private static final char EXP_e_CHAR = 'e';
  private static final char PLUS_CHAR = '+';
  private static final char MINUS_CHAR = '-';
  private static final int NO_CHAR = -1;

  private static final byte FCT_UNKNOWN = 0;
  private static final byte FCT_LITERAL = 1;
  private static final byte FCT_GENERAL = 2;
  private static final byte FCT_DATE = 3;
  private static final byte FCT_NUMBER = 4;
  private static final byte FCT_TEXT = 5;

  private static final byte[] FORMAT_CODE_TYPES = new byte[127];
  static {
    setFormatCodeTypes(" $+-()", FCT_LITERAL);
    setFormatCodeTypes("\"!*\\[];", FCT_GENERAL);
    setFormatCodeTypes(":/cdwmqyhnstampmAMPM", FCT_DATE);
    setFormatCodeTypes(".,0#%Ee", FCT_NUMBER);
    setFormatCodeTypes("@&<>", FCT_TEXT);
  }

  @FunctionalInterface
  interface Fmt {
    public Value format(Args args);
  }

  @FunctionalInterface
  interface DateFormatBuilder {
    public void build(DateTimeFormatterBuilder dtfb, Args args,
                      boolean hasAmPm, Value.Type dtType);
  }

  private static final DateFormatBuilder PARTIAL_PREFIX =
    (dtfb, args, hasAmPm, dtType) -> {
      throw new UnsupportedOperationException();
    };

  private static final Map<String,DateFormatBuilder> DATE_FMT_BUILDERS =
    new HashMap<>();
  static {
    DATE_FMT_BUILDERS.put("c",
      (dtfb, args, hasAmPm, dtType) ->
        dtfb.append(ValueSupport.getDateFormatForType(
                        args._ctx, dtType)));
    DATE_FMT_BUILDERS.put("d", new SimpleDFB("d"));
    DATE_FMT_BUILDERS.put("dd", new SimpleDFB("dd"));
    DATE_FMT_BUILDERS.put("ddd", new SimpleDFB("eee"));
    DATE_FMT_BUILDERS.put("dddd", new SimpleDFB("eeee"));
    DATE_FMT_BUILDERS.put("ddddd", new PredefDFB(TemporalConfig.Type.SHORT_DATE));
    DATE_FMT_BUILDERS.put("dddddd", new PredefDFB(TemporalConfig.Type.LONG_DATE));
    DATE_FMT_BUILDERS.put("w", new WeekBasedDFB() {
      @Override
      protected TemporalField getField(WeekFields weekFields) {
        return weekFields.dayOfWeek();
      }
    });
    DATE_FMT_BUILDERS.put("ww", new WeekBasedDFB() {
      @Override
      protected TemporalField getField(WeekFields weekFields) {
        return weekFields.weekOfWeekBasedYear();
      }
    });
    DATE_FMT_BUILDERS.put("m", new SimpleDFB("L"));
    DATE_FMT_BUILDERS.put("mm", new SimpleDFB("LL"));
    DATE_FMT_BUILDERS.put("mmm", new SimpleDFB("MMM"));
    DATE_FMT_BUILDERS.put("mmmm", new SimpleDFB("MMMM"));
    DATE_FMT_BUILDERS.put("q", new SimpleDFB("Q"));
    DATE_FMT_BUILDERS.put("y", new SimpleDFB("D"));
    DATE_FMT_BUILDERS.put("yy", new SimpleDFB("yy"));
    DATE_FMT_BUILDERS.put("yyyy", new SimpleDFB("yyyy"));
    DATE_FMT_BUILDERS.put("h", new HourlyDFB("h", "H"));
    DATE_FMT_BUILDERS.put("hh", new HourlyDFB("hh", "HH"));
    DATE_FMT_BUILDERS.put("n", new SimpleDFB("m"));
    DATE_FMT_BUILDERS.put("nn", new SimpleDFB("mm"));
    DATE_FMT_BUILDERS.put("s", new SimpleDFB("s"));
    DATE_FMT_BUILDERS.put("ss", new SimpleDFB("ss"));
    DATE_FMT_BUILDERS.put("ttttt", new PredefDFB(TemporalConfig.Type.LONG_TIME));
    DATE_FMT_BUILDERS.put("AM/PM", new AmPmDFB("AM", "PM"));
    DATE_FMT_BUILDERS.put("am/pm", new AmPmDFB("am", "pm"));
    DATE_FMT_BUILDERS.put("A/P", new AmPmDFB("A", "P"));
    DATE_FMT_BUILDERS.put("a/p", new AmPmDFB("a", "p"));
    DATE_FMT_BUILDERS.put("AMPM",
      (dtfb, args, hasAmPm, dtType) -> {
        String[] amPmStrs = args._ctx.getTemporalConfig().getAmPmStrings();
        new AmPmDFB(amPmStrs[0], amPmStrs[1]).build(dtfb, args, hasAmPm, dtType);
      }
    );
    fillInPartialPrefixes();
  }

  private static final int NF_POS_IDX = 0;
  private static final int NF_NEG_IDX = 1;
  private static final int NF_ZERO_IDX = 2;
  private static final int NF_NULL_IDX = 3;
  private static final int NUM_NF_FMTS = 4;

  private static final NumberFormatter.NotationType[] NO_EXP_TYPES =
    new NumberFormatter.NotationType[NUM_NF_FMTS];
  private static final boolean[] NO_FMT_TYPES = new boolean[NUM_NF_FMTS];


  private static final class Args
  {
    private final EvalContext _ctx;
    private Value _expr;
    private final int _firstDay;
    private final int _firstWeekType;

    private Args(EvalContext ctx, Value expr, int firstDay, int firstWeekType) {
      _ctx = ctx;
      _expr = expr;
      _firstDay = firstDay;
      _firstWeekType = firstWeekType;
    }

    public boolean isNullOrEmptyString() {
      return(_expr.isNull() ||
             // only a string value could ever be an empty string
             (_expr.getType().isString() && getAsString().isEmpty()));
    }

    public boolean maybeCoerceToEmptyString() {
      if(isNullOrEmptyString()) {
        // ensure that we have a non-null value when formatting (null acts
        // like empty string)
        _expr = ValueSupport.EMPTY_STR_VAL;
        return true;
      }
      return false;
    }

    public Args coerceToDateTimeValue() {
      if(!_expr.getType().isTemporal()) {

        // format coerces boolean strings to numbers
        Value boolExpr = null;
        if(_expr.getType().isString() &&
           ((boolExpr = maybeGetStringAsBooleanValue()) != null)) {
          _expr = boolExpr;
        }

        // StringValue already handles most String -> Number -> Date/Time, so
        // most other convertions work here (and failures are thrown so that
        // default handling kicks in)
        _expr = _expr.getAsDateTimeValue(_ctx);
      }
      return this;
    }

    public Args coerceToNumberValue() {
      if(!_expr.getType().isNumeric()) {
        if(_expr.getType().isString()) {

          // format coerces "true"/"false" to boolean values
          Value boolExpr = maybeGetStringAsBooleanValue();
          if(boolExpr != null) {
            _expr = boolExpr;
          } else {
            BigDecimal bd = DefaultFunctions.maybeGetAsBigDecimal(_ctx, _expr);
            if(bd != null) {
              _expr = ValueSupport.toValue(bd);
            } else {
              // convert to date to number.  this doesn't happen as part of the
              // default value coercion behavior, but the format method tries
              // harder
              Value maybe = DefaultFunctions.maybeGetAsDateTimeValue(
                  _ctx, _expr);
              if(maybe != null) {
                _expr = ValueSupport.toValue(maybe.getAsDouble(_ctx));
              } else {
                // string which can't be converted to number force failure
                // here so default formatting will kick in
                throw new EvalException("invalid number value");
              }
            }
          }
        } else {
          // convert date to number
          _expr = ValueSupport.toValue(_expr.getAsDouble(_ctx));
        }
      }
      return this;
    }

    private Value maybeGetStringAsBooleanValue() {
      // format coerces "true"/"false" to boolean values
      String val = getAsString();
      if("true".equalsIgnoreCase(val)) {
        return ValueSupport.TRUE_VAL;
      }
      if("false".equalsIgnoreCase(val)) {
        return ValueSupport.FALSE_VAL;
      }
      return null;
    }

    public BigDecimal getAsBigDecimal() {
      coerceToNumberValue();
      return _expr.getAsBigDecimal(_ctx);
    }

    public LocalDateTime getAsLocalDateTime() {
      coerceToDateTimeValue();
      return _expr.getAsLocalDateTime(_ctx);
    }

    public boolean getAsBoolean() {
      // even though string values have a "boolean" value, for formatting,
      // strings which don't convert to valid boolean/number/date are just
      // returned as is.  so we use coerceToNumberValue to force the exception
      // to be thrown which results in the "default" formatting behavior.
      coerceToNumberValue();
      return _expr.getAsBoolean(_ctx);
    }

    public String getAsString() {
      return _expr.getAsString(_ctx);
    }
  }

  private FormatUtil() {}


  public static Value format(EvalContext ctx, Value expr, String fmtStr,
                             int firstDay, int firstWeekType) {

    try {
      Args args = new Args(ctx, expr, firstDay, firstWeekType);

      Fmt predefFmt = PREDEF_FMTS.get(fmtStr);
      if(predefFmt != null) {
        if(args.isNullOrEmptyString()) {
          // predefined formats return empty string for null
          return ValueSupport.EMPTY_STR_VAL;
        }
        return predefFmt.format(args);
      }

      // TODO implement caching for custom formats?  put into Bindings.  use
      // special "cache" prefix to know which caches to clear when evalconfig
      // is altered (could also cache other Format* functions)

      return parseCustomFormat(fmtStr, args).format(args);

    } catch(EvalException ee) {
      // values which cannot be formatted as the target type are just
      // returned "as is"
      return expr;
    }
  }

  private static Fmt parseCustomFormat(String fmtStr, Args args) {

    ExprBuf buf = new ExprBuf(fmtStr, null);

    // do partial pass to determine what type of format this is
    byte curFormatType = determineFormatType(buf);

    // reset buffer for real parse
    buf.reset(0);

    switch(curFormatType) {
    case FCT_GENERAL:
      return parseCustomGeneralFormat(buf, args);
    case FCT_DATE:
      return parseCustomDateFormat(buf, args);
    case FCT_NUMBER:
      return parseCustomNumberFormat(buf, args);
    case FCT_TEXT:
      return parseCustomTextFormat(buf, args);
    default:
      throw new EvalException("Invalid format type " + curFormatType);
    }
  }

  private static byte determineFormatType(ExprBuf buf) {

    while(buf.hasNext()) {
      char c = buf.next();
      byte fmtType = getFormatCodeType(c);
      switch(fmtType) {
      case FCT_UNKNOWN:
      case FCT_LITERAL:
        // meaningless, ignore for now
        break;
      case FCT_GENERAL:
        switch(c) {
        case QUOTE_CHAR:
          parseQuotedString(buf);
          break;
        case START_COLOR_CHAR:
          parseColorString(buf);
          break;
        case ESCAPE_CHAR:
        case FILL_ESCAPE_CHAR:
          if(buf.hasNext()) {
            buf.next();
          }
          break;
        default:
          // meaningless, ignore for now
        }
        break;
      case FCT_DATE:
      case FCT_NUMBER:
      case FCT_TEXT:
        // found specific type
        return fmtType;
      default:
        throw new EvalException("Invalid format type " + fmtType);
      }
    }

    // no specific type
    return FCT_GENERAL;
  }

  private static Fmt parseCustomGeneralFormat(ExprBuf buf, Args args) {

    // a "general" format is actually a "yes/no" format which functions almost
    // exactly like a number format (without any number format specific chars)

    StringBuilder sb = new StringBuilder();
    String[] fmtStrs = new String[NUM_NF_FMTS];
    int fmtIdx = 0;

    BUF_LOOP:
    while(buf.hasNext()) {
      char c = buf.next();
      int fmtType = getFormatCodeType(c);
      switch(fmtType) {
      case FCT_GENERAL:
        switch(c) {
        case LEFT_ALIGN_CHAR:
          // no effect
          break;
        case QUOTE_CHAR:
          parseQuotedString(buf, sb);
          break;
        case START_COLOR_CHAR:
          // color strings seem to be ignored
          parseColorString(buf);
          break;
        case ESCAPE_CHAR:
          if(buf.hasNext()) {
            sb.append(buf.next());
          }
          break;
        case FILL_ESCAPE_CHAR:
          // unclear what this actually does.  online examples don't seem to
          // match with experimental results.  for now, ignore
          if(buf.hasNext()) {
            buf.next();
          }
          break;
        case CHOICE_SEP_CHAR:
          // yes/no (number) format supports up to 4 formats: pos, neg, zero,
          // null.  after that, ignore the rest
          if(fmtIdx == (NUM_NF_FMTS - 1)) {
            // last possible format, ignore remaining
            break BUF_LOOP;
          }
          addCustomGeneralFormat(fmtStrs, fmtIdx++, sb);
          break;
        default:
          sb.append(c);
        }
        break;
      default:
        sb.append(c);
      }
    }

    // fill in remaining formats
    while(fmtIdx < NUM_NF_FMTS) {
      addCustomGeneralFormat(fmtStrs, fmtIdx++, sb);
    }

    return new CustomGeneralFmt(
        ValueSupport.toValue(fmtStrs[NF_POS_IDX]),
        ValueSupport.toValue(fmtStrs[NF_NEG_IDX]),
        ValueSupport.toValue(fmtStrs[NF_ZERO_IDX]),
        ValueSupport.toValue(fmtStrs[NF_NULL_IDX]));
  }

  private static void addCustomGeneralFormat(String[] fmtStrs, int fmtIdx,
                                             StringBuilder sb)
  {
    addCustomNumberFormat(fmtStrs, NO_EXP_TYPES, NO_FMT_TYPES, fmtIdx, sb);
  }

  private static Fmt parseCustomDateFormat(ExprBuf buf, Args args) {

    // keep track of some extra state while parsing the format, whether or not
    // there was an am/pm pattern and whether or not there was a general
    // date/time pattern
    boolean[] fmtState = new boolean[]{false, false};
    List<DateFormatBuilder> dfbs = new ArrayList<>();

    BUF_LOOP:
    while(buf.hasNext()) {
      char c = buf.next();
      int fmtType = getFormatCodeType(c);
      switch(fmtType) {
      case FCT_GENERAL:
        switch(c) {
        case QUOTE_CHAR:
          String str = parseQuotedString(buf);
          dfbs.add((dtfb, argsParam, hasAmPmParam, dtType) ->
                   dtfb.appendLiteral(str));
          break;
        case START_COLOR_CHAR:
          // color strings seem to be ignored
          parseColorString(buf);
          break;
        case ESCAPE_CHAR:
          if(buf.hasNext()) {
            dfbs.add(buildLiteralCharDFB(buf.next()));
          }
          break;
        case FILL_ESCAPE_CHAR:
          // unclear what this actually does.  online examples don't seem to
          // match with experimental results.  for now, ignore
          if(buf.hasNext()) {
            buf.next();
          }
          break;
        case CHOICE_SEP_CHAR:
          // date/time doesn't use multiple pattern choices, but it does
          // respect the char.  ignore everything after the first choice
          break BUF_LOOP;
        default:
          dfbs.add(buildLiteralCharDFB(c));
        }
        break;
      case FCT_DATE:
        parseCustomDateFormatPattern(c, buf, dfbs, fmtState, args);
        break;
      default:
        dfbs.add(buildLiteralCharDFB(c));
      }
    }

    boolean hasAmPm = fmtState[0];
    boolean hasGeneralFormat = fmtState[1];
    if(!hasGeneralFormat) {
      // simple situation, one format for every value
      DateTimeFormatter dtf = createDateTimeFormatter(dfbs, args, hasAmPm, null);
      return new CustomFmt(argsParam -> ValueSupport.toValue(
                               dtf.format(argsParam.getAsLocalDateTime())));
    }

    // we need separate formatters for date, time, and date/time values
    DateTimeFormatter dateFmt = createDateTimeFormatter(dfbs, args, hasAmPm,
                                                        Value.Type.DATE);
    DateTimeFormatter timeFmt = createDateTimeFormatter(dfbs, args, hasAmPm,
                                                        Value.Type.TIME);
    DateTimeFormatter dtFmt = createDateTimeFormatter(dfbs, args, hasAmPm,
                                                      Value.Type.DATE_TIME);

    return new CustomFmt(argsParam -> formatDateTime(
                             argsParam, dateFmt, timeFmt, dtFmt));
  }

  private static void parseCustomDateFormatPattern(
      char c, ExprBuf buf, List<DateFormatBuilder> dfbs,
      boolean[] fmtState, Args args) {

    if((c == DT_LIT_COLON_CHAR) || (c == DT_LIT_SLASH_CHAR)) {
      // date/time literal char, nothing more to do
      dfbs.add(buildLiteralCharDFB(c));
      return;
    }

    StringBuilder sb = buf.getScratchBuffer();
    sb.append(c);

    char firstChar = c;
    int firstPos = buf.curPos();
    String bestMatchPat = sb.toString();

    DateFormatBuilder bestMatch = DATE_FMT_BUILDERS.get(bestMatchPat);
    int bestPos = firstPos;
    while(buf.hasNext()) {
      sb.append(buf.next());
      String tmpPat = sb.toString();
      DateFormatBuilder dfb = DATE_FMT_BUILDERS.get(tmpPat);
      if(dfb == null) {
        // no more possible matches
        break;
      }
      if(dfb != PARTIAL_PREFIX) {
        // this is the longest, valid pattern we have seen so far
        bestMatch = dfb;
        bestPos = buf.curPos();
        bestMatchPat = tmpPat;
      }
    }

    if(bestMatch != PARTIAL_PREFIX) {

      // apply valid pattern
      buf.reset(bestPos);
      dfbs.add(bestMatch);

      switch(firstChar) {
      case 'a':
      case 'A':
        // this was an am/pm pattern
        fmtState[0] = true;
        break;
      case 'c':
        // this was a general date/time format
        fmtState[1] = true;
        break;
      default:
        // don't care
      }

    } else {

      // just consume the first char
      buf.reset(firstPos);
      dfbs.add(buildLiteralCharDFB(firstChar));
    }
  }

  private static DateFormatBuilder buildLiteralCharDFB(char c) {
    return (dtfb, args, hasAmPm, dtType) -> dtfb.appendLiteral(c);
  }

  private static DateTimeFormatter createDateTimeFormatter(
      List<DateFormatBuilder> dfbs, Args args, boolean hasAmPm,
      Value.Type dtType)
  {
    DateTimeFormatterBuilder dtfb = new DateTimeFormatterBuilder();
    dfbs.forEach(d -> d.build(dtfb, args, hasAmPm, dtType));
    return dtfb.toFormatter(args._ctx.getTemporalConfig().getLocale());
  }

  private static Value formatDateTime(
      Args args, DateTimeFormatter dateFmt,
      DateTimeFormatter timeFmt, DateTimeFormatter dtFmt)
  {
    LocalDateTime ldt = args.getAsLocalDateTime();
    DateTimeFormatter fmt = null;
    switch(args._expr.getType()) {
    case DATE:
      fmt = dateFmt;
      break;
    case TIME:
      fmt = timeFmt;
      break;
    default:
      fmt = dtFmt;
    }

    return ValueSupport.toValue(fmt.format(ldt));
  }

  private static Fmt parseCustomNumberFormat(ExprBuf buf, Args args) {

    StringBuilder sb = new StringBuilder();
    String[] fmtStrs = new String[NUM_NF_FMTS];
    int fmtIdx = 0;
    StringBuilder pendingLiteral = new StringBuilder();
    NumberFormatter.NotationType[] expTypes =
      new NumberFormatter.NotationType[NUM_NF_FMTS];
    boolean[] hasFmts = new boolean[NUM_NF_FMTS];

    BUF_LOOP:
    while(buf.hasNext()) {
      char c = buf.next();
      int fmtType = getFormatCodeType(c);
      switch(fmtType) {
      case FCT_GENERAL:
        switch(c) {
        case LEFT_ALIGN_CHAR:
          // no effect
          break;
        case QUOTE_CHAR:
          parseQuotedString(buf, pendingLiteral);
          break;
        case START_COLOR_CHAR:
          // color strings seem to be ignored
          parseColorString(buf);
          break;
        case ESCAPE_CHAR:
          if(buf.hasNext()) {
            pendingLiteral.append(buf.next());
          }
          break;
        case FILL_ESCAPE_CHAR:
          // unclear what this actually does.  online examples don't seem to
          // match with experimental results.  for now, ignore
          if(buf.hasNext()) {
            buf.next();
          }
          break;
        case CHOICE_SEP_CHAR:
          // number format supports up to 4 formats: pos, neg, zero, null.
          // after that, ignore the rest
          if(fmtIdx == (NUM_NF_FMTS - 1)) {
            // last possible format, ignore remaining
            break BUF_LOOP;
          }
          flushPendingNumberLiteral(pendingLiteral, sb);
          addCustomNumberFormat(fmtStrs, expTypes, hasFmts, fmtIdx++, sb);
          break;
        default:
          pendingLiteral.append(c);
        }
        break;
      case FCT_NUMBER:
        hasFmts[fmtIdx] = true;
        switch(c) {
        case EXP_E_CHAR:
          int signChar = buf.peekNext();
          if((signChar == PLUS_CHAR) || (signChar == MINUS_CHAR)) {
            buf.next();
            expTypes[fmtIdx] = ((signChar == PLUS_CHAR) ?
                                NumberFormatter.NotationType.EXP_E_PLUS :
                                NumberFormatter.NotationType.EXP_E_MINUS);
            flushPendingNumberLiteral(pendingLiteral, sb);
            sb.append(EXP_E_CHAR);
          } else {
            pendingLiteral.append(c);
          }
          break;
        case EXP_e_CHAR:
          signChar = buf.peekNext();
          if((signChar == PLUS_CHAR) || (signChar == MINUS_CHAR)) {
            buf.next();
            expTypes[fmtIdx] = ((signChar == PLUS_CHAR) ?
                                NumberFormatter.NotationType.EXP_e_PLUS :
                                NumberFormatter.NotationType.EXP_e_MINUS);
            flushPendingNumberLiteral(pendingLiteral, sb);
            sb.append(EXP_E_CHAR);
          } else {
            pendingLiteral.append(c);
          }
          break;
        default:
          // most number format chars pass straight through
          flushPendingNumberLiteral(pendingLiteral, sb);
          sb.append(c);
        }
        break;
      default:
        pendingLiteral.append(c);
      }
    }

    // fill in remaining formats
    while(fmtIdx < NUM_NF_FMTS) {
      flushPendingNumberLiteral(pendingLiteral, sb);
      addCustomNumberFormat(fmtStrs, expTypes, hasFmts, fmtIdx++, sb);
    }

    return new CustomNumberFmt(
        createCustomNumberFormat(fmtStrs, expTypes, hasFmts, NF_POS_IDX, args),
        createCustomNumberFormat(fmtStrs, expTypes, hasFmts, NF_NEG_IDX, args),
        createCustomNumberFormat(fmtStrs, expTypes, hasFmts, NF_ZERO_IDX, args),
        createCustomNumberFormat(fmtStrs, expTypes, hasFmts, NF_NULL_IDX, args));
  }

  private static void addCustomNumberFormat(
      String[] fmtStrs, NumberFormatter.NotationType[] expTypes,
      boolean[] hasFmts, int fmtIdx, StringBuilder sb)
  {
    if(sb.length() == 0) {
      // do special empty format handling on a per-format-type basis
      switch(fmtIdx) {
      case NF_NEG_IDX:
        // re-use "pos" format
        sb.append('-').append(fmtStrs[NF_POS_IDX]);
        expTypes[NF_NEG_IDX] = expTypes[NF_POS_IDX];
        hasFmts[NF_NEG_IDX] = hasFmts[NF_POS_IDX];
        break;
      case NF_ZERO_IDX:
        // re-use "pos" format
        sb.append(fmtStrs[NF_POS_IDX]);
        expTypes[NF_ZERO_IDX] = expTypes[NF_POS_IDX];
        hasFmts[NF_ZERO_IDX] = hasFmts[NF_POS_IDX];
        break;
      default:
        // use empty string result
      }
    }

    fmtStrs[fmtIdx] = sb.toString();
    sb.setLength(0);
  }

  private static void flushPendingNumberLiteral(
      StringBuilder pendingLiteral, StringBuilder sb) {
    if(pendingLiteral.length() == 0) {
      return;
    }

    if((pendingLiteral.length() == 1) &&
       pendingLiteral.charAt(0) == SINGLE_QUOTE_CHAR) {
      // handle standalone single quote
      sb.append(SINGLE_QUOTE_CHAR).append(SINGLE_QUOTE_CHAR);
      pendingLiteral.setLength(0);
      return;
    }

    sb.append(SINGLE_QUOTE_CHAR);
    int startPos = sb.length();
    sb.append(pendingLiteral);

    // we need to quote any single quotes in the literal string
    for(int i = startPos; i < sb.length(); ++i) {
      char c = sb.charAt(i);
      if(c == SINGLE_QUOTE_CHAR) {
        sb.insert(++i, SINGLE_QUOTE_CHAR);
      }
    }

    sb.append(SINGLE_QUOTE_CHAR);
    pendingLiteral.setLength(0);
  }

  private static BDFormat createCustomNumberFormat(
      String[] fmtStrs, NumberFormatter.NotationType[] expTypes,
      boolean[] hasFmts, int fmtIdx, Args args) {

    String fmtStr = fmtStrs[fmtIdx];
    if(!hasFmts[fmtIdx]) {
      // convert the literal string to a dummy number format
      if(fmtStr.length() > 0) {
        // strip quoting
        StringBuilder sb = new StringBuilder(fmtStr)
          .deleteCharAt(fmtStr.length() - 1)
          .deleteCharAt(0);
        if(sb.length() > 0) {
          for(int i = 0; i < sb.length(); ++i) {
            if(sb.charAt(i) == SINGLE_QUOTE_CHAR) {
              // delete next single quote char
              sb.deleteCharAt(++i);
            }
          }
        } else {
          // this was a single, literal single quote
          sb.append(SINGLE_QUOTE_CHAR);
        }
        fmtStr = sb.toString();
      }
      return new LiteralBDFormat(fmtStr);
    }

    NumberFormatter.NotationType expType = expTypes[fmtIdx];
    DecimalFormat df = args._ctx.createDecimalFormat(fmtStr);

    if(df.getMaximumFractionDigits() > 0) {
      // if the decimal is included in the format, access always shows it
      df.setDecimalSeparatorAlwaysShown(true);
    }

    if(expType != null) {
      return new BaseBDFormat(
          new NumberFormatter.ScientificFormat(df, expType));
    }

    return new DecimalBDFormat(df);
  }

  private static Fmt parseCustomTextFormat(ExprBuf buf, Args args) {

    Fmt fmt = null;

    List<BiConsumer<StringBuilder,CharSource>> subFmts = new ArrayList<>();
    int numPlaceholders = 0;
    boolean rightAligned = true;
    TextCase textCase = TextCase.NONE;
    StringBuilder pendingLiteral = new StringBuilder();
    boolean hasFmtChars = false;

    BUF_LOOP:
    while(buf.hasNext()) {
      char c = buf.next();
      hasFmtChars = true;
      int fmtType = getFormatCodeType(c);
      switch(fmtType) {
      case FCT_GENERAL:
        switch(c) {
        case LEFT_ALIGN_CHAR:
          rightAligned = false;
          break;
        case QUOTE_CHAR:
          parseQuotedString(buf, pendingLiteral);
          break;
        case START_COLOR_CHAR:
          // color strings seem to be ignored
          parseColorString(buf);
          break;
        case ESCAPE_CHAR:
          if(buf.hasNext()) {
            pendingLiteral.append(buf.next());
          }
          break;
        case FILL_ESCAPE_CHAR:
          // unclear what this actually does.  online examples don't seem to
          // match with experimental results.  for now, ignore
          if(buf.hasNext()) {
            buf.next();
          }
          break;
        case CHOICE_SEP_CHAR:
          // text format supports up to 2 formats: normal and empty/null.
          // after that, ignore the rest
          if(fmt != null) {
            // ignore remaining format
            break BUF_LOOP;
          }
          flushPendingTextLiteral(pendingLiteral, subFmts);
          fmt = new CharSourceFmt(subFmts, numPlaceholders, rightAligned,
                                  textCase);
          // reset for next format
          subFmts = new ArrayList<>();
          numPlaceholders = 0;
          rightAligned = true;
          textCase = TextCase.NONE;
          hasFmtChars = false;
          break;
        default:
          pendingLiteral.append(c);
        }
        break;
      case FCT_TEXT:
        switch(c) {
        case REQ_PLACEHOLDER_CHAR:
          flushPendingTextLiteral(pendingLiteral, subFmts);
          ++numPlaceholders;
          subFmts.add((sb,cs) -> {
              int tmp = cs.next();
              sb.append((tmp != NO_CHAR) ? (char)tmp : ' ');
            });
          break;
        case OPT_PLACEHOLDER_CHAR:
          flushPendingTextLiteral(pendingLiteral, subFmts);
          ++numPlaceholders;
          subFmts.add((sb,cs) -> {
              int tmp = cs.next();
              if(tmp != NO_CHAR) {
                sb.append((char)tmp);
              }
            });
          break;
        case TO_UPPER_CHAR:
          // an uppper and lower symbol cancel each other out
          textCase = ((textCase == TextCase.LOWER) ?
                      TextCase.NONE : TextCase.UPPER);
          break;
        case TO_LOWER_CHAR:
          // an uppper and lower symbol cancel each other out
          textCase = ((textCase == TextCase.UPPER) ?
                      TextCase.NONE : TextCase.LOWER);
          break;
        default:
          pendingLiteral.append(c);
        }
        break;
      default:
        pendingLiteral.append(c);
      }
    }

    flushPendingTextLiteral(pendingLiteral, subFmts);

    Fmt emptyFmt = null;
    if(fmt == null) {
      fmt = new CharSourceFmt(subFmts, numPlaceholders, rightAligned,
                              textCase);
      emptyFmt = NULL_FMT;
    } else {
      emptyFmt = (hasFmtChars ?
                  new CharSourceFmt(subFmts, numPlaceholders, rightAligned,
                                    textCase) :
                  NULL_FMT);
    }

    return new CustomFmt(fmt, emptyFmt);
  }

  private static void flushPendingTextLiteral(
      StringBuilder pendingLiteral,
      List<BiConsumer<StringBuilder,CharSource>> subFmts) {
    if(pendingLiteral.length() == 0) {
      return;
    }

    String literal = pendingLiteral.toString();
    pendingLiteral.setLength(0);
    subFmts.add((sb, cs) -> sb.append(literal));
  }

  public static String createNumberFormatPattern(
      NumPatternType numPatType, int numDecDigits, boolean incLeadDigit,
      boolean negParens, int numGroupDigits) {

    StringBuilder fmt = new StringBuilder();

    numPatType.appendPrefix(fmt);

    if(numGroupDigits > 0) {
      fmt.append("#,");
      DefaultTextFunctions.nchars(fmt, numGroupDigits - 1, '#');
    }

    fmt.append(incLeadDigit ? "0" : "#");
    if(numDecDigits > 0) {
      fmt.append(".");
      DefaultTextFunctions.nchars(fmt, numDecDigits, '0');
    }

    numPatType.appendSuffix(fmt);

    if(negParens) {
      // the javadocs claim the second pattern does not need to be fully
      // defined, but it doesn't seem to work that way
      String mainPat = fmt.toString();
      fmt.append(";(").append(mainPat).append(")");
    }

    return fmt.toString();
  }

  private static byte getFormatCodeType(char c) {
    if((c >= 0) && (c < 127)) {
      return FORMAT_CODE_TYPES[c];
    }
    return FCT_UNKNOWN;
  }

  private static void setFormatCodeTypes(String chars, byte type) {
    for(char c : chars.toCharArray()) {
      FORMAT_CODE_TYPES[c] = type;
    }
  }

  private static String parseQuotedString(ExprBuf buf) {
    return ExpressionTokenizer.parseStringUntil(buf, null, QUOTE_CHAR, true);
  }

  private static void parseQuotedString(ExprBuf buf, StringBuilder sb) {
    ExpressionTokenizer.parseStringUntil(buf, null, QUOTE_CHAR, true, sb);
  }

  private static String parseColorString(ExprBuf buf) {
    return ExpressionTokenizer.parseStringUntil(
        buf, START_COLOR_CHAR, END_COLOR_CHAR, false);
  }

  private static void fillInPartialPrefixes() {
    List<String> validPrefixes = new ArrayList<>(DATE_FMT_BUILDERS.keySet());
    for(String validPrefix : validPrefixes) {
      int len = validPrefix.length();
      while(len > 1) {
        --len;
        validPrefix = validPrefix.substring(0, len);
        DATE_FMT_BUILDERS.putIfAbsent(validPrefix, PARTIAL_PREFIX);
      }
    }
  }

  private static final class PredefDateFmt implements Fmt
  {
    private final TemporalConfig.Type _type;

    private PredefDateFmt(TemporalConfig.Type type) {
      _type = type;
    }

    @Override
    public Value format(Args args) {
      DateTimeFormatter dtf = args._ctx.createDateFormatter(
          args._ctx.getTemporalConfig().getDateTimeFormat(_type));
      return ValueSupport.toValue(dtf.format(args.getAsLocalDateTime()));
    }
  }

  private static final class PredefBoolFmt implements Fmt
  {
    private final Value _trueVal;
    private final Value _falseVal;

    private PredefBoolFmt(String trueStr, String falseStr) {
      _trueVal = ValueSupport.toValue(trueStr);
      _falseVal = ValueSupport.toValue(falseStr);
    }

    @Override
    public Value format(Args args) {
      return(args.getAsBoolean() ? _trueVal : _falseVal);
    }
  }

  private static abstract class BaseNumberFmt implements Fmt
  {
    @Override
    public Value format(Args args) {
      NumberFormat df = getNumberFormat(args);
      return ValueSupport.toValue(df.format(args.getAsBigDecimal()));
    }

    protected abstract NumberFormat getNumberFormat(Args args);
  }

  private static final class PredefNumberFmt extends BaseNumberFmt
  {
    private final NumericConfig.Type _type;

    private PredefNumberFmt(NumericConfig.Type type) {
      _type = type;
    }

    @Override
    protected NumberFormat getNumberFormat(Args args) {
      return args._ctx.createDecimalFormat(
          args._ctx.getNumericConfig().getNumberFormat(_type));
    }
  }

  private static final class ScientificPredefNumberFmt extends BaseNumberFmt
  {
    @Override
    protected NumberFormat getNumberFormat(Args args) {
      NumberFormat df = args._ctx.createDecimalFormat(
          args._ctx.getNumericConfig().getNumberFormat(
              NumericConfig.Type.SCIENTIFIC));
      df = new NumberFormatter.ScientificFormat(df);
      return df;
    }
  }

  private static final class SimpleDFB implements DateFormatBuilder
  {
    private final String _pat;

    private SimpleDFB(String pat) {
      _pat = pat;
    }
    @Override
    public void build(DateTimeFormatterBuilder dtfb, Args args,
                      boolean hasAmPm, Value.Type dtType) {
      dtfb.appendPattern(_pat);
    }
  }

  private static final class HourlyDFB implements DateFormatBuilder
  {
    private final String _pat12;
    private final String _pat24;

    private HourlyDFB(String pat12, String pat24) {
      _pat12 = pat12;
      _pat24 = pat24;
    }
    @Override
    public void build(DateTimeFormatterBuilder dtfb, Args args,
                      boolean hasAmPm, Value.Type dtTypePm) {
      // annoyingly the "hour" patterns are the same and depend on the
      // existence of the am/pm pattern to determine how they function (12 vs
      // 24 hour).
      dtfb.appendPattern(hasAmPm ? _pat12 : _pat24);
    }
  }

  private static final class PredefDFB implements DateFormatBuilder
  {
    private final TemporalConfig.Type _type;

    private PredefDFB(TemporalConfig.Type type) {
      _type = type;
    }
    @Override
    public void build(DateTimeFormatterBuilder dtfb, Args args,
                      boolean hasAmPm, Value.Type dtType) {
      dtfb.appendPattern(args._ctx.getTemporalConfig().getDateTimeFormat(_type));
    }
  }

  private static abstract class WeekBasedDFB implements DateFormatBuilder
  {
    @Override
    public void build(DateTimeFormatterBuilder dtfb, Args args,
                      boolean hasAmPm, Value.Type dtType) {
      dtfb.appendValue(getField(DefaultDateFunctions.weekFields(
                                    args._firstDay, args._firstWeekType)));
    }

    protected abstract TemporalField getField(WeekFields weekFields);
  }

  private static final class AmPmDFB extends AbstractMap<Long,String>
    implements DateFormatBuilder
  {
    private static final Long ZERO_KEY = 0L;
    private final String _am;
    private final String _pm;

    private AmPmDFB(String am, String pm) {
      _am = am;
      _pm = pm;
    }
    @Override
    public void build(DateTimeFormatterBuilder dtfb, Args args,
                      boolean hasAmPm, Value.Type dtType) {
      dtfb.appendText(ChronoField.AMPM_OF_DAY, this);
    }
    @Override
    public int size() {
      return 2;
    }
    @Override
    public String get(Object key) {
      return(ZERO_KEY.equals(key) ? _am : _pm);
    }
    @Override
    public Set<Map.Entry<Long,String>> entrySet() {
      return new AbstractSet<Map.Entry<Long,String>>() {
        @Override
        public int size() {
          return 2;
        }
        @Override
        public Iterator<Map.Entry<Long,String>> iterator() {
          return Arrays.<Map.Entry<Long,String>>asList(
              new AbstractMap.SimpleImmutableEntry<Long,String>(0L, _am),
              new AbstractMap.SimpleImmutableEntry<Long,String>(1L, _pm))
            .iterator();
        }
      };
    }
  }

  private static final class CustomFmt implements Fmt
  {
    private final Fmt _fmt;
    private final Fmt _emptyFmt;

    private CustomFmt(Fmt fmt) {
      this(fmt, NULL_FMT);
    }

    private CustomFmt(Fmt fmt, Fmt emptyFmt) {
      _fmt = fmt;
      _emptyFmt = emptyFmt;
    }

    @Override
    public Value format(Args args) {
      Fmt fmt = _fmt;
      if(args.maybeCoerceToEmptyString()) {
        fmt = _emptyFmt;
      }
      return fmt.format(args);
    }
  }

  private static final class CharSourceFmt implements Fmt
  {
    private final List<BiConsumer<StringBuilder,CharSource>> _subFmts;
    private final int _numPlaceholders;
    private final boolean _rightAligned;
    private final TextCase _textCase;

    private CharSourceFmt(List<BiConsumer<StringBuilder,CharSource>> subFmts,
                          int numPlaceholders, boolean rightAligned,
                          TextCase textCase) {
      _subFmts = subFmts;
      _numPlaceholders = numPlaceholders;
      _rightAligned = rightAligned;
      _textCase = textCase;
    }

    @Override
    public Value format(Args args) {
      CharSource cs = new CharSource(args.getAsString(), _numPlaceholders,
                                     _rightAligned, _textCase);
      StringBuilder sb = new StringBuilder();
      _subFmts.stream().forEach(fmt -> fmt.accept(sb, cs));
      cs.appendRemaining(sb);
      return ValueSupport.toValue(sb.toString());
    }
  }

  private static final class CharSource
  {
    private int _prefLen;
    private final String _str;
    private int _strPos;
    private final TextCase _textCase;

    private CharSource(String str, int len, boolean rightAligned,
                       TextCase textCase) {
      _str = str;
      _textCase = textCase;
      int strLen = str.length();
      if(len > strLen) {
        if(rightAligned) {
          _prefLen = len - strLen;
        }
      } else if(len < strLen) {
        // it doesn't make sense to me, but the meaning of "right aligned"
        // seems to flip when the string is longer than the format length
        if(!rightAligned) {
          _strPos = strLen - len;
        }
      }
    }

    public int next() {
      if(_prefLen > 0) {
        --_prefLen;
        return NO_CHAR;
      }
      if(_strPos < _str.length()) {
        return _textCase.apply(_str.charAt(_strPos++));
      }
      return NO_CHAR;
    }

    public void appendRemaining(StringBuilder sb) {
      int strLen = _str.length();
      while(_strPos < strLen) {
        sb.append(_textCase.apply(_str.charAt(_strPos++)));
      }
    }
  }

  private static abstract class BaseCustomNumberFmt implements Fmt
  {
    @Override
    public Value format(Args args) {
      if(args._expr.isNull()) {
        return formatNull(args);
      }

      BigDecimal bd = args.getAsBigDecimal();
      int cmp = BigDecimal.ZERO.compareTo(bd);

      return ((cmp < 0) ? formatPos(bd, args) :
              ((cmp > 0) ? formatNeg(bd, args) :
               formatZero(bd, args)));
    }

    protected abstract Value formatNull(Args args);
    protected abstract Value formatPos(BigDecimal bd, Args args);
    protected abstract Value formatNeg(BigDecimal bd, Args args);
    protected abstract Value formatZero(BigDecimal bd, Args args);
  }

  private static final class CustomGeneralFmt extends BaseCustomNumberFmt
  {
    private final Value _posVal;
    private final Value _negVal;
    private final Value _zeroVal;
    private final Value _nullVal;

    private CustomGeneralFmt(Value posVal, Value negVal,
                             Value zeroVal, Value nullVal) {
      _posVal = posVal;
      _negVal = negVal;
      _zeroVal = zeroVal;
      _nullVal = nullVal;
    }

    @Override
    protected Value formatNull(Args args) {
      return _nullVal;
    }
    @Override
    protected Value formatPos(BigDecimal bd, Args args) {
      return _posVal;
    }
    @Override
    protected Value formatNeg(BigDecimal bd, Args args) {
      return _negVal;
    }
    @Override
    protected Value formatZero(BigDecimal bd, Args args) {
      return _zeroVal;
    }
  }

  private static final class CustomNumberFmt extends BaseCustomNumberFmt
  {
    private final BDFormat _posFmt;
    private final BDFormat _negFmt;
    private final BDFormat _zeroFmt;
    private final BDFormat _nullFmt;

    private CustomNumberFmt(BDFormat posFmt, BDFormat negFmt,
                            BDFormat zeroFmt, BDFormat nullFmt) {
      _posFmt = posFmt;
      _negFmt = negFmt;
      _zeroFmt = zeroFmt;
      _nullFmt = nullFmt;
    }

    private Value formatMaybeZero(BigDecimal bd, BDFormat fmt) {
      // in theory we want to use the given format.  however, if, due to
      // rounding, we end up with a number equivalent to zero, then we fall
      // back to the zero format.  if we are using scientific notation,
      // however, then don't worry about this
      int maxDecDigits = fmt.getMaxDecimalDigits();
      if(maxDecDigits < bd.scale()) {
          bd = bd.setScale(maxDecDigits, NumberFormatter.ROUND_MODE);
          if(BigDecimal.ZERO.compareTo(bd) == 0) {
            // fall back to zero format
            fmt = _zeroFmt;
          }
      }

      return ValueSupport.toValue(fmt.format(bd));
    }

    @Override
    protected Value formatNull(Args args) {
        return ValueSupport.toValue(_nullFmt.format(BigDecimal.ZERO));
    }
    @Override
    protected Value formatPos(BigDecimal bd, Args args) {
      return formatMaybeZero(bd, _posFmt);
    }
    @Override
    protected Value formatNeg(BigDecimal bd, Args args) {
      return formatMaybeZero(bd.negate(), _negFmt);
    }
    @Override
    protected Value formatZero(BigDecimal bd, Args args) {
      return ValueSupport.toValue(_zeroFmt.format(bd));
    }
  }

  private static abstract class BDFormat
  {
    public int getMaxDecimalDigits() {
      return Integer.MAX_VALUE;
    }

    public abstract String format(BigDecimal bd);
  }

  private static final class LiteralBDFormat extends BDFormat
  {
    private final String _str;

    private LiteralBDFormat(String str) {
      _str = str;
    }

    @Override
    public String format(BigDecimal bd) {
      return _str;
    }
  }

  private static class BaseBDFormat extends BDFormat
  {
    private final NumberFormat _nf;

    private BaseBDFormat(NumberFormat nf) {
      _nf = nf;
    }

    @Override
    public String format(BigDecimal bd) {
      return _nf.format(bd);
    }
  }

  private static final class DecimalBDFormat extends BaseBDFormat
  {
    private final int _maxDecDigits;

    private DecimalBDFormat(DecimalFormat df) {
      super(df);

      int maxDecDigits = df.getMaximumFractionDigits();
      int mult = df.getMultiplier();
      while(mult > 1) {
        ++maxDecDigits;
        mult /= 10;
      }
      _maxDecDigits = maxDecDigits;
    }

    @Override
    public int getMaxDecimalDigits() {
      return _maxDecDigits;
    }
  }
}
