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
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.NumericConfig;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public class FormatUtil
{
  public enum NumPatternType {
    GENERAL, CURRENCY {
      @Override
      protected void appendPrefix(StringBuilder fmt) {
        fmt.append("\u00A4");
      }
      @Override
      protected boolean useParensForNegatives(NumericConfig cfg) {
        return cfg.useParensForCurrencyNegatives();
      }
    },
    PERCENT {
      @Override
      protected void appendSuffix(StringBuilder fmt) {
        fmt.append("%");
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

  private static final Map<String,Fmt> PREDEF_FMTS = new HashMap<String,Fmt>();

  static {
    PREDEF_FMTS.put("General Date", new GenPredefDateFmt());
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

    PREDEF_FMTS.put("General Number", new GenPredefNumberFmt());
    PREDEF_FMTS.put("Currency",
                    new PredefNumberFmt(NumericConfig.Type.CURRENCY));
    // FIXME ?
    // PREDEF_FMTS.put("Euro",
    //                 new PredefNumberFmt(???));
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

  private FormatUtil() {}


  public static Value format(EvalContext ctx, Value expr, String fmtStr,
                             int firstDay, int firstWeekType) {

    Fmt predefFmt = PREDEF_FMTS.get(fmtStr);
    if(predefFmt != null) {
      if(expr.isNull()) {
        // predefined formats return null for null
        return ValueSupport.NULL_VAL;
      }
      return predefFmt.format(ctx, expr, null, firstDay, firstWeekType);
    }

    // FIXME,
    throw new UnsupportedOperationException();
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


  private static abstract class Fmt
  {
    // FIXME, no null
    // FIXME, need fmtStr?
    public abstract Value format(EvalContext ctx, Value expr, String fmtStr,
                                 int firstDay, int firstWeekType);
  }

  private static class PredefDateFmt extends Fmt
  {
    private final TemporalConfig.Type _type;

    private PredefDateFmt(TemporalConfig.Type type) {
      _type = type;
    }

    @Override
    public Value format(EvalContext ctx, Value expr, String fmtStr,
                        int firstDay, int firstWeekType) {
      DateTimeFormatter dtf = ctx.createDateFormatter(
          ctx.getTemporalConfig().getDateTimeFormat(_type));
      return ValueSupport.toValue(dtf.format(expr.getAsLocalDateTime(ctx)));
    }
  }

  private static class GenPredefDateFmt extends Fmt
  {
    @Override
    public Value format(EvalContext ctx, Value expr, String fmtStr,
                         int firstDay, int firstWeekType) {
      Value tempExpr = expr;
      if(!expr.getType().isTemporal()) {
        Value maybe = DefaultFunctions.maybeGetAsDateTimeValue(ctx, expr);
        if(maybe != null) {
          tempExpr = maybe;
        }
      }
      return ValueSupport.toValue(tempExpr.getAsString(ctx));
    }
  }

  private static class PredefBoolFmt extends Fmt
  {
    private final Value _trueVal;
    private final Value _falseVal;

    private PredefBoolFmt(String trueStr, String falseStr) {
      _trueVal = ValueSupport.toValue(trueStr);
      _falseVal = ValueSupport.toValue(falseStr);
    }

    @Override
    public Value format(EvalContext ctx, Value expr, String fmtStr,
                        int firstDay, int firstWeekType) {
      return(expr.getAsBoolean(ctx) ? _trueVal : _falseVal);
    }
  }

  private static class PredefNumberFmt extends Fmt
  {
    private final NumericConfig.Type _type;

    private PredefNumberFmt(NumericConfig.Type type) {
      _type = type;
    }

    @Override
    public Value format(EvalContext ctx, Value expr, String fmtStr,
                         int firstDay, int firstWeekType) {
      DecimalFormat df = ctx.createDecimalFormat(
          ctx.getNumericConfig().getNumberFormat(_type));
      return ValueSupport.toValue(df.format(expr.getAsBigDecimal(ctx)));
    }
  }

  private static class GenPredefNumberFmt extends Fmt
  {
    @Override
    public Value format(EvalContext ctx, Value expr, String fmtStr,
                        int firstDay, int firstWeekType) {
      Value numExpr = expr;
      if(!expr.getType().isNumeric()) {
        if(expr.getType().isString()) {
          BigDecimal bd = DefaultFunctions.maybeGetAsBigDecimal(ctx, expr);
          if(bd != null) {
            numExpr = ValueSupport.toValue(bd);
          } else {
            // convert to date to number
            Value maybe = DefaultFunctions.maybeGetAsDateTimeValue(ctx, expr);
            if(maybe != null) {
              numExpr = ValueSupport.toValue(maybe.getAsDouble(ctx));
            }
          }
        } else {
          // convert date to number
          numExpr = ValueSupport.toValue(expr.getAsDouble(ctx));
        }
      }
      return ValueSupport.toValue(numExpr.getAsString(ctx));
    }
  }

  private static class ScientificPredefNumberFmt extends Fmt
  {
    @Override
    public Value format(EvalContext ctx, Value expr, String fmtStr,
                         int firstDay, int firstWeekType) {
      NumberFormat df = ctx.createDecimalFormat(
          ctx.getNumericConfig().getNumberFormat(
              NumericConfig.Type.SCIENTIFIC));
      df = new NumberFormatter.ScientificFormat(df);
      return ValueSupport.toValue(df.format(expr.getAsBigDecimal(ctx)));
    }
  }
}
