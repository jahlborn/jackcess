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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;

/**
 *
 * @author James Ahlborn
 */
public class NumberFormatter
{
  public static final RoundingMode ROUND_MODE = RoundingMode.HALF_EVEN;

  private static final int FLT_SIG_DIGITS = 7;
  private static final int DBL_SIG_DIGITS = 15;
  private static final int DEC_SIG_DIGITS = 28;

  public static final MathContext FLT_MATH_CONTEXT =
    new MathContext(FLT_SIG_DIGITS, ROUND_MODE);
  public static final MathContext DBL_MATH_CONTEXT =
    new MathContext(DBL_SIG_DIGITS, ROUND_MODE);
  public static final MathContext DEC_MATH_CONTEXT =
    new MathContext(DEC_SIG_DIGITS, ROUND_MODE);

  // note, java doesn't distinguish between pos/neg NaN
  private static final String NAN_STR = "1.#QNAN";
  private static final String POS_INF_STR = "1.#INF";
  private static final String NEG_INf_STR = "-1.#INF";

  private static final ThreadLocal<NumberFormatter> INSTANCE =
    new ThreadLocal<NumberFormatter>() {
    @Override
    protected NumberFormatter initialValue() {
      return new NumberFormatter();
    }
  };

  private final TypeFormatter _fltFmt = new TypeFormatter(FLT_SIG_DIGITS);
  private final TypeFormatter _dblFmt = new TypeFormatter(DBL_SIG_DIGITS);
  private final TypeFormatter _decFmt = new TypeFormatter(DEC_SIG_DIGITS);

  private NumberFormatter() {}

  public static String format(float f) {
    return INSTANCE.get().formatImpl(f);
  }

  public static String format(double d) {
    return INSTANCE.get().formatImpl(d);
  }

  public static String format(BigDecimal bd) {
    return INSTANCE.get().formatImpl(bd);
  }

  private String formatImpl(float f) {

    if(Float.isNaN(f)) {
      return NAN_STR;
    }
    if(Float.isInfinite(f)) {
      return ((f < 0f) ? NEG_INf_STR : POS_INF_STR);
    }

    return _fltFmt.format(new BigDecimal(f, FLT_MATH_CONTEXT));
  }

  private String formatImpl(double d) {

    if(Double.isNaN(d)) {
      return NAN_STR;
    }
    if(Double.isInfinite(d)) {
      return ((d < 0d) ? NEG_INf_STR : POS_INF_STR);
    }

    return _dblFmt.format(new BigDecimal(d, DBL_MATH_CONTEXT));
  }

  private String formatImpl(BigDecimal bd) {
    return _decFmt.format(bd.round(DEC_MATH_CONTEXT));
  }

  private static final class TypeFormatter
  {
    private final DecimalFormat _df = new DecimalFormat("0.#");
    private final BetterDecimalFormat _dfS;
    private final int _prec;

    private TypeFormatter(int prec) {
      _prec = prec;
      _df.setMaximumIntegerDigits(prec);
      _df.setMaximumFractionDigits(prec);
      _df.setRoundingMode(ROUND_MODE);
      _dfS = new BetterDecimalFormat("0.#E00", prec);
    }

    public String format(BigDecimal bd) {
      bd = bd.stripTrailingZeros();
      int prec = bd.precision();
      int scale = bd.scale();

      int sigDigits = prec;
      if(scale < 0) {
        sigDigits -= scale;
      } else if(scale > prec) {
        sigDigits += (scale - prec);
      }

      return ((sigDigits > _prec) ? _dfS.format(bd) : _df.format(bd));
    }
  }

  private static final class BetterDecimalFormat extends NumberFormat
  {
    private static final long serialVersionUID = 0L;

    private final DecimalFormat _df;

    private BetterDecimalFormat(String pat, int prec) {
      super();
      _df = new DecimalFormat(pat);
      _df.setMaximumIntegerDigits(1);
      _df.setMaximumFractionDigits(prec);
      _df.setRoundingMode(ROUND_MODE);
    }

    @Override
    public StringBuffer format(Object number, StringBuffer toAppendTo,
                               FieldPosition pos)
    {
      StringBuffer sb = _df.format(number, toAppendTo, pos);
      int idx = sb.lastIndexOf("E");
      if(sb.charAt(idx + 1) != '-') {
        sb.insert(idx + 1, '+');
      }
      return sb;
    }

    @Override
    public StringBuffer format(double number, StringBuffer toAppendTo,
                               FieldPosition pos) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Number parse(String source, ParsePosition parsePosition) {
      throw new UnsupportedOperationException();
    }

    @Override
    public StringBuffer format(long number, StringBuffer toAppendTo,
                               FieldPosition pos) {
      throw new UnsupportedOperationException();
    }
  }
}
