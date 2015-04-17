/*
Copyright (c) 2008 Health Market Science, Inc.

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

import java.io.IOException;
import com.healthmarketscience.jackcess.Column;

/**
 * Simple implementation of an ErrorHandler which always returns the
 * configured object.
 * 
 * @author James Ahlborn
 * @usage _intermediate_class_
 */
public class ReplacementErrorHandler implements ErrorHandler
{

  private final Object _replacement;

  /**
   * Constructs a ReplacementErrorHandler which replaces all errored values
   * with {@code null}.
   */
  public ReplacementErrorHandler() {
    this(null);
  }

  /**
   * Constructs a ReplacementErrorHandler which replaces all errored values
   * with the given Object.
   */
  public ReplacementErrorHandler(Object replacement) {
    _replacement = replacement;
  }

  public Object handleRowError(Column column, byte[] columnData,
                               Location location, Exception error)
    throws IOException
  {
    return _replacement;
  }

}
