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
import java.math.BigInteger;
import java.text.DecimalFormatSymbols;

import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.Value;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author James Ahlborn
 */
public class StringValue extends BaseValue
{
  private static final Object NOT_A_NUMBER = new Object();

  private final String _val;
  private Object _num;

  public StringValue(String val)
  {
    _val = val;
  }

  public Type getType() {
    return Type.STRING;
  }

  public Object get() {
    return _val;
  }

  @Override
  public boolean getAsBoolean(LocaleContext ctx) {
    // ms access seems to treat strings as "true"
    return true;
  }

  @Override
  public String getAsString(LocaleContext ctx) {
    return _val;
  }

  @Override
  public Integer getAsLongInt(LocaleContext ctx) {
    return roundToLongInt(ctx);
  }

  @Override
  public Double getAsDouble(LocaleContext ctx) {
    return getNumber(ctx).doubleValue();
  }

  @Override
  public BigDecimal getAsBigDecimal(LocaleContext ctx) {
    return getNumber(ctx);
  }

  @Override
  public Value getAsDateTimeValue(LocaleContext ctx) {
    Value dateValue = DefaultDateFunctions.stringToDateValue(ctx, _val);

    if(dateValue == null) {
      // see if string can be coerced to number and then to value date (note,
      // numberToDateValue may return null for out of range numbers)
      try {
        dateValue = DefaultDateFunctions.numberToDateValue(
            ctx, getNumber(ctx).doubleValue());
      } catch(EvalException ignored) {
        // not a number, not a date/time
      }

      if(dateValue == null) {
        throw invalidConversion(Type.DATE_TIME);
      }
    }

    // TODO, for now, we can't cache the date value becuase it could be an
    // "implicit" date which would need to be re-calculated on each call
    return dateValue;
  }

  protected BigDecimal getNumber(LocaleContext ctx) {
    if(_num instanceof BigDecimal) {
      return (BigDecimal)_num;
    }
    if(_num == null) {
      // see if it is parseable as a number
      try {
        // ignore extraneous whitespace whitespace and handle "&[hH]" or
        // "&[oO]" prefix (only supports integers)
        String tmpVal = _val.trim();
        if(tmpVal.length() > 0) {

          if(tmpVal.charAt(0) != ValueSupport.NUMBER_BASE_PREFIX) {
            // convert to standard numeric support for parsing
            tmpVal = toCanonicalNumberFormat(ctx, tmpVal);
            _num = ValueSupport.normalize(new BigDecimal(tmpVal));
            return (BigDecimal)_num;
          }

          // parse as hex/octal symbolic value
          if(ValueSupport.HEX_PAT.matcher(tmpVal).matches()) {
            return parseIntegerString(tmpVal, 16);
          } else if(ValueSupport.OCTAL_PAT.matcher(tmpVal).matches()) {
            return parseIntegerString(tmpVal, 8);
          }

          // fall through to NaN
        }
      } catch(NumberFormatException nfe) {
        // fall through to NaN...
      }
      _num = NOT_A_NUMBER;
    }
    throw invalidConversion(Type.DOUBLE);
  }

  private BigDecimal parseIntegerString(String tmpVal, int radix) {
    _num = new BigDecimal(ValueSupport.parseIntegerString(tmpVal, radix));
    return (BigDecimal)_num;
  }

  private static String toCanonicalNumberFormat(LocaleContext ctx, String tmpVal)
  {
    // convert to standard numeric format:
    // - discard any grouping separators
    // - convert decimal separator to '.'
    DecimalFormatSymbols syms = ctx.getNumericConfig().getDecimalFormatSymbols();
    char groupSepChar = syms.getGroupingSeparator();
    tmpVal = StringUtils.remove(tmpVal, groupSepChar);

    char decSepChar = syms.getDecimalSeparator();
    if((decSepChar != ValueSupport.CANON_DEC_SEP) && (tmpVal.indexOf(decSepChar) >= 0)) {
      tmpVal = tmpVal.replace(decSepChar, ValueSupport.CANON_DEC_SEP);
    }

    return tmpVal;
  }
}
