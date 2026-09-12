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

/**
 * A Function provides an invokable handle to external functionality to an
 * expression.
 *
 * @author James Ahlborn
 */
public interface Function
{

  /**
   * @return the name of this function
   */
  public String getName();

  /**
   * Evaluates this function within the given context with the given
   * parameters.
   *
   * @return the result of the function evaluation
   */
  public Value eval(EvalContext ctx, Value... params);

  /**
   * @return {@code true} if this function is a "pure" function, {@code false}
   *         otherwise.  A pure function will always return the same result
   *         for a given set of parameters and has no side effects.
   */
  public boolean isPure();
}
