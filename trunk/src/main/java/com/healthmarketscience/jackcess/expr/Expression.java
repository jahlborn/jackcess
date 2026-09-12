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

import java.util.Collection;

/**
 * An Expression is an executable handle to an Access expression.  While the
 * expression framework is implemented separately from the core database
 * functionality, most usage of Expressions will happen indirectly within the
 * context of normal database operations.  Thus, most users will not ever
 * directly interact with an Expression instance.  That said, Expressions may
 * be executed independently of a Database instance if desired.
 *
 * @author James Ahlborn
 */
public interface Expression
{

  /**
   * Evaluates the expression and returns the result.
   *
   * @param ctx the context within which to evaluate the expression
   *
   * @return the result of the expression evaluation
   */
  public Object eval(EvalContext ctx);

  /**
   * @return a detailed string which indicates how the expression was
   *         interpreted by the expression evaluation engine.
   */
  public String toDebugString(LocaleContext ctx);

  /**
   * @return a parsed and re-formated version of the expression.  This may
   *         look slightly different than the original, raw string, although
   *         it should be an equivalent expression.
   */
  public String toCleanString(LocaleContext ctx);

  /**
   * @return the original, unparsed expression string.  This is the same as
   *         the value which will be returned by {@link Object#toString}.
   */
  public String toRawString();

  /**
   * @return {@code true} if this is a constant expression.  A constant
   *         expression will always return the same result when invoked and
   *         has no side effect.
   */
  public boolean isConstant();

  /**
   * Adds any Identifiers from this expression to the given collection.
   */
  public void collectIdentifiers(Collection<Identifier> identifiers);
}
