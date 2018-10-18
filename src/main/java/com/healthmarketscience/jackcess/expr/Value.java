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

package com.healthmarketscience.jackcess.expr;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Wrapper for a typed primitive value used within the expression evaluation
 * engine.  Note that the "Null" value is represented by an actual Value
 * instance with the type of {@link Type#NULL}.  Also note that all the
 * conversion methods will throw an {@link EvalException} if the conversion is
 * not supported for the current value.
 *
 * @author James Ahlborn
 */
public interface Value
{
  /** the types supported within the expression evaluation engine */
  public enum Type
  {
    NULL, STRING, DATE, TIME, DATE_TIME, LONG, DOUBLE, BIG_DEC;

    public boolean isNumeric() {
      return inRange(LONG, BIG_DEC);
    }

    public boolean isIntegral() {
      return (this == LONG);
    }

    public boolean isTemporal() {
      return inRange(DATE, DATE_TIME);
    }

    public Type getPreferredFPType() {
      return((ordinal() <= DOUBLE.ordinal()) ? DOUBLE : BIG_DEC);
    }

    public Type getPreferredNumericType() {
      if(isNumeric()) {
        return this;
      }
      if(isTemporal()) {
        return ((this == DATE) ? LONG : DOUBLE);
      }
      return null;
    }

    private boolean inRange(Type start, Type end) {
      return ((start.ordinal() <= ordinal()) && (ordinal() <= end.ordinal()));
    }
  }

  /**
   * @return the type of this value
   */
  public Type getType();

  /**
   * @return the raw primitive value
   */
  public Object get();

  /**
   * @return {@code true} if this value represents a "Null" value,
   *         {@code false} otherwise.
   */
  public boolean isNull();

  /**
   * @return this primitive value converted to a boolean
   */
  public boolean getAsBoolean(LocaleContext ctx);

  /**
   * @return this primitive value converted to a String
   */
  public String getAsString(LocaleContext ctx);

  /**
   * @return this primitive value converted to a Date
   */
  public Date getAsDateTime(LocaleContext ctx);

  /**
   * Since date/time values have different types, it may be more convenient to
   * get the date/time primitive value with the appropriate type information.
   *
   * @return this value converted to a date/time value
   */
  public Value getAsDateTimeValue(LocaleContext ctx);

  /**
   * @return this primitive value converted (rounded) to an int
   */
  public Integer getAsLongInt(LocaleContext ctx);

  /**
   * @return this primitive value converted (rounded) to a double
   */
  public Double getAsDouble(LocaleContext ctx);

  /**
   * @return this primitive value converted to a BigDecimal
   */
  public BigDecimal getAsBigDecimal(LocaleContext ctx);
}
