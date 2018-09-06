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
import javax.script.Bindings;

/**
 * EvalContext encapsulates all shared state for expression parsing and
 * evaluation.  It provides a bridge between the expression execution engine
 * and the current Database.
 *
 * @author James Ahlborn
 */
public interface EvalContext
{
  /**
   * @return the currently configured TemporalConfig (from the
   *         {@link EvalConfig})
   */
  public TemporalConfig getTemporalConfig();

  /**
   * @return an appropriately configured (i.e. TimeZone and other date/time
   *         flags) SimpleDateFormat for the given format.
   */
  public SimpleDateFormat createDateFormat(String formatStr);

  /**
   * @param seed the seed for the random value, following the rules for the
   *             "Rnd" function
   * @return a random value for the given seed following the statefulness
   *         rules for the "Rnd" function
   */
  public float getRandom(Integer seed);

  /**
   * @return the expected type of the result value for the current expression
   *         evaluation (for "default value" and "calculated" expressions)
   */
  public Value.Type getResultType();

  /**
   * @return the value of the "current" column (for "field validator"
   *         expressions)
   */
  public Value getThisColumnValue();

  /**
   * @return the value of the entity identified by the given identifier (for
   *         "calculated" and "row validator" expressions)
   */
  public Value getIdentifierValue(Identifier identifier);

  /**
   * @return the currently configured Bindings (from the {@link EvalConfig})
   */
  public Bindings getBindings();

  /**
   * @return the value of the current key from the currently configured
   *         {@link Bindings}
   */
  public Object get(String key);

  /**
   * Sets the value of the given key to the given value in the currently
   * configured {@link Bindings}.
   */
  public void put(String key, Object value);
}
