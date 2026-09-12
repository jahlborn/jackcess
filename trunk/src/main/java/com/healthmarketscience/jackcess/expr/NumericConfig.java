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

package com.healthmarketscience.jackcess.expr;

import java.math.BigDecimal;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

import com.healthmarketscience.jackcess.impl.expr.FormatUtil;
import com.healthmarketscience.jackcess.impl.expr.NumberFormatter;

/**
 * A NumericConfig encapsulates number formatting options for expression
 * evaluation.  The default {@link #US_NUMERIC_CONFIG} instance provides US
 * specific locale configuration.  Databases which have been built for other
 * locales can utilize custom implementations of NumericConfig in order to
 * evaluate expressions correctly.
 *
 * @author James Ahlborn
 */
public class NumericConfig
{
  public static final NumericConfig US_NUMERIC_CONFIG = new NumericConfig(
      2, true, false, true, 3, Locale.US);

  public enum Type {
    CURRENCY, FIXED, STANDARD, PERCENT, SCIENTIFIC, EURO;
  }

  private final int _numDecDigits;
  private final boolean _incLeadingDigit;
  private final boolean _useNegParens;
  private final boolean _useNegCurrencyParens;
  private final int _numGroupDigits;
  private final DecimalFormatSymbols _symbols;
  private final NumberFormatter _numFmt;
  private final String _currencyFormat;
  private final String _fixedFormat;
  private final String _standardFormat;
  private final String _percentFormat;
  private final String _scientificFormat;
  private final String _euroFormat;

  public NumericConfig(int numDecDigits, boolean incLeadingDigit,
                       boolean useNegParens, boolean useNegCurrencyParens,
                       int numGroupDigits, Locale locale) {
    _numDecDigits = numDecDigits;
    _incLeadingDigit = incLeadingDigit;
    _useNegParens = useNegParens;
    _useNegCurrencyParens = useNegCurrencyParens;
    _numGroupDigits = numGroupDigits;
    _symbols = DecimalFormatSymbols.getInstance(locale);
    _numFmt = new NumberFormatter(_symbols);

    _currencyFormat = FormatUtil.createNumberFormatPattern(
        FormatUtil.NumPatternType.CURRENCY, _numDecDigits, _incLeadingDigit,
        _useNegCurrencyParens, _numGroupDigits);
    _fixedFormat = FormatUtil.createNumberFormatPattern(
        FormatUtil.NumPatternType.GENERAL, _numDecDigits, true,
        _useNegParens, 0);
    _standardFormat = FormatUtil.createNumberFormatPattern(
        FormatUtil.NumPatternType.GENERAL, _numDecDigits, _incLeadingDigit,
        _useNegParens, _numGroupDigits);
    _percentFormat = FormatUtil.createNumberFormatPattern(
        FormatUtil.NumPatternType.PERCENT, _numDecDigits, _incLeadingDigit,
        _useNegParens, 0);
    _scientificFormat = FormatUtil.createNumberFormatPattern(
        FormatUtil.NumPatternType.SCIENTIFIC, _numDecDigits, true,
        false, 0);
    _euroFormat = FormatUtil.createNumberFormatPattern(
        FormatUtil.NumPatternType.EURO, _numDecDigits, _incLeadingDigit,
        _useNegCurrencyParens, _numGroupDigits);
  }

  public int getNumDecimalDigits() {
    return _numDecDigits;
  }

  public boolean includeLeadingDigit() {
    return _incLeadingDigit;
  }

  public boolean useParensForNegatives() {
    return _useNegParens;
  }

  public boolean useParensForCurrencyNegatives() {
    return _useNegCurrencyParens;
  }

  public int getNumGroupingDigits() {
    return _numGroupDigits;
  }

  public String getNumberFormat(Type type) {
    switch(type) {
    case CURRENCY:
      return _currencyFormat;
    case FIXED:
      return _fixedFormat;
    case STANDARD:
      return _standardFormat;
    case PERCENT:
      return _percentFormat;
    case SCIENTIFIC:
      return _scientificFormat;
    case EURO:
      return _euroFormat;
    default:
      throw new IllegalArgumentException("unknown number type " + type);
    }
  }

  public DecimalFormatSymbols getDecimalFormatSymbols() {
    return _symbols;
  }

  /**
   * @return the given float formatted according to the current locale config
   */
  public String format(float f) {
    return _numFmt.format(f);
  }

  /**
   * @return the given double formatted according to the current locale config
   */
  public String format(double d) {
    return _numFmt.format(d);
  }

  /**
   * @return the given BigDecimal formatted according to the current locale config
   */
  public String format(BigDecimal bd) {
    return _numFmt.format(bd);
  }
}
