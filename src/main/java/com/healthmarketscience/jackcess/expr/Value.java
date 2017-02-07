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
 *
 * @author James Ahlborn
 */
public interface Value 
{
  public enum Type
  {
    NULL, STRING, DATE, TIME, DATE_TIME, LONG, DOUBLE, BIG_DEC;

    public boolean isNumeric() {
      return inRange(LONG, BIG_DEC);
    }

    public boolean isIntegral() {
      // note when BOOLEAN is converted to number, it is integral
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


  public Type getType();

  public Object get();

  public boolean isNull();

  public boolean getAsBoolean();

  public String getAsString();

  public Date getAsDateTime(EvalContext ctx);

  public Long getAsLong();

  public Double getAsDouble();

  public BigDecimal getAsBigDecimal();
}
