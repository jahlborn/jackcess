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

/**
 * A FunctionLookup provides a source for {@link Function} instances used
 * during expression evaluation.
 *
 * @author James Ahlborn
 */
public interface FunctionLookup
{
  /**
   * @return the function for the given function name, or {@code null} if none
   *         exists.  Note that Access function names are treated in a case
   *         insensitive manner.
   */
  public Function getFunction(String name);
}
