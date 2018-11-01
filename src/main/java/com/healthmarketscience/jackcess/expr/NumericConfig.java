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
      Locale.US);

  private final DecimalFormatSymbols _symbols;

  public NumericConfig(Locale locale) {
    _symbols = DecimalFormatSymbols.getInstance(locale);
  }

  public DecimalFormatSymbols getDecimalFormatSymbols() {
    return _symbols;
  }
}
