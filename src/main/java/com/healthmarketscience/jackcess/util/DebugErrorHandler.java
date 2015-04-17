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
import com.healthmarketscience.jackcess.impl.ByteUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of ErrorHandler which is useful for generating debug
 * information about bad row data (great for bug reports!).  After logging a
 * debug entry for the failed column, it will return some sort of replacement
 * value, see {@link ReplacementErrorHandler}.
 * 
 * @author James Ahlborn
 * @usage _general_class_
 */
public class DebugErrorHandler extends ReplacementErrorHandler
{
  private static final Log LOG = LogFactory.getLog(DebugErrorHandler.class); 

  /**
   * Constructs a DebugErrorHandler which replaces all errored values with
   * {@code null}.
   */
  public DebugErrorHandler() {
  }

  /**
   * Constructs a DebugErrorHandler which replaces all errored values with the
   * given Object.
   */
  public DebugErrorHandler(Object replacement) {
    super(replacement);
  }

  @Override
  public Object handleRowError(Column column, byte[] columnData, 
                               Location location, Exception error)
    throws IOException
  {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Failed reading column " + column + ", row " +
                location + ", bytes " +
                ((columnData != null) ?
                 ByteUtil.toHexString(columnData) : "null"),
                error);
    }

    return super.handleRowError(column, columnData, location, error);
  }

}
