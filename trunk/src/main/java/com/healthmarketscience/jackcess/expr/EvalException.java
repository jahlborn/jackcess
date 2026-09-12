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
 * Base class for exceptions thrown during expression evaluation.
 *
 * @author James Ahlborn
 */
public class EvalException extends IllegalStateException
{
  private static final long serialVersionUID = 20180330L;

  public EvalException(String message) {
    super(message);
  }

  public EvalException(Throwable cause) {
    super(cause);
  }

  public EvalException(String message, Throwable cause) {
    super(message, cause);
  }
}
