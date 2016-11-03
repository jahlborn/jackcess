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

package com.healthmarketscience.jackcess.util;

/**
 *
 * @author James Ahlborn
 */
public interface Expression 
{
  public interface RowContext
  {
    public Value getThisColumnValue();

    public Value getRowValue(String collectionName, String objName,
                             String colName);
  }

  public enum ValueType
  {
    NULL, BOOLEAN, STRING, DATE, TIME, DATE_TIME, LONG, DOUBLE, BIG_INT, BIG_DEC;

    public boolean isNumeric() {
      return inRange(LONG, BIG_DEC);
    }

    public boolean isTemporal() {
      return inRange(DATE, DATE_TIME);
    }

    private boolean inRange(ValueType start, ValueType end) {
      return ((start.ordinal() <= ordinal()) && (ordinal() <= end.ordinal()));
    }
  }

  public interface Value
  {
    public ValueType getType();
    public Object get();
  }

  public interface Function
  {
    public String getName();
    public Value eval(Value... params);
  }


  public Object evalDefault();

  public Boolean evalCondition(RowContext ctx);

  public String toDebugString();
}
