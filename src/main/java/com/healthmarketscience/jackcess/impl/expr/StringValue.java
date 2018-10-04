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
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.expr.LocaleContext;
import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author James Ahlborn
 */
public class StringValue extends BaseValue
{
  private static final Object NOT_A_NUMBER = new Object();

  private static final char NUMBER_BASE_PREFIX = '&';
  private static final Pattern OCTAL_PAT =
    Pattern.compile(NUMBER_BASE_PREFIX + "[oO][0-7]+");
  private static final Pattern HEX_PAT =
    Pattern.compile(NUMBER_BASE_PREFIX + "[hH]\\p{XDigit}+");

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

          if(tmpVal.charAt(0) != NUMBER_BASE_PREFIX) {
            // parse using standard numeric support, after discarding any
            // grouping separators
            char groupSepChar = ctx.getNumericConfig().getDecimalFormatSymbols()
              .getGroupingSeparator();
            tmpVal = StringUtils.remove(tmpVal, groupSepChar);
            _num = ValueSupport.normalize(new BigDecimal(tmpVal));
            return (BigDecimal)_num;
          }

          // parse as hex/octal symbolic value
          if(HEX_PAT.matcher(tmpVal).matches()) {
            return parseIntegerString(tmpVal, 16);
          } else if(OCTAL_PAT.matcher(tmpVal).matches()) {
            return parseIntegerString(tmpVal, 8);
          }

          // fall through to NaN
        }
      } catch(NumberFormatException nfe) {
        // fall through to NaN...
      }
      _num = NOT_A_NUMBER;
    }
    throw new NumberFormatException("Invalid number '" + _val + "'");
  }

  private BigDecimal parseIntegerString(String tmpVal, int radix) {
    _num = new BigDecimal(new BigInteger(tmpVal.substring(2), radix));
    return (BigDecimal)_num;
  }
}
