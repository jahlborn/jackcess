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

import java.text.SimpleDateFormat;
import java.util.Random;

/**
 *
 * @author James Ahlborn
 */
public interface EvalContext 
{
  public Value.Type getResultType();

  public TemporalConfig getTemporalConfig();

  public SimpleDateFormat createDateFormat(String formatStr);

  public Value getThisColumnValue();

  public Value getRowValue(String collectionName, String objName,
                           String colName);

  public float getRandom(Integer seed);
}
