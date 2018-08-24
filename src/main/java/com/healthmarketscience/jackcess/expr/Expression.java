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
 * FIXME, doc me and my friend
 *
 * An Expression is an executable handle to an Access expression.  While the
 * expression framework is implemented as separate from the core database
 * functionality, most usage of Expressions will happen indirectly within the
 * context of normal database operations.  Thus, most users will not ever
 * directly interact with an Expression instance.  That said, Expressions may
 * be executed independently of a Database instance if desired.
 *
 * @author James Ahlborn
 */
public interface Expression
{
  public Object eval(EvalContext ctx);

  public String toDebugString();

  public String toRawString();

  public boolean isConstant();

  public void collectIdentifiers(Collection<Identifier> identifiers);
}
