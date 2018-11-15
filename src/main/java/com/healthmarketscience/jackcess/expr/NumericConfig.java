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

import java.text.DecimalFormatSymbols;
import java.util.Locale;

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

  private final int _numDecDigits;
  private final boolean _incLeadingDigit;
  private final boolean _useNegParens;
  private final boolean _useNegCurrencyParens;
  private final int _numGroupDigits;
  private final DecimalFormatSymbols _symbols;

  public NumericConfig(int numDecDigits, boolean incLeadingDigit,
                       boolean useNegParens, boolean useNegCurrencyParens,
                       int numGroupDigits, Locale locale) {
    _numDecDigits = numDecDigits;
    _incLeadingDigit = incLeadingDigit;
    _useNegParens = useNegParens;
    _useNegCurrencyParens = useNegCurrencyParens;
    _numGroupDigits = numGroupDigits;
    _symbols = DecimalFormatSymbols.getInstance(locale);
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

  public DecimalFormatSymbols getDecimalFormatSymbols() {
    return _symbols;
  }
}
