/*
Copyright (c) 2013 James Ahlborn

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

package com.healthmarketscience.jackcess;

/**
 * JackcessException which is thrown from multi-add-row {@link Table} methods
 * which indicates how many rows were successfully written before the
 * underlying failure was encountered.
 *
 * @author James Ahlborn
 */
public class BatchUpdateException extends JackcessException 
{
  private static final long serialVersionUID = 20131123L;

  private final int _updateCount;

  public BatchUpdateException(int updateCount, String msg, Throwable cause) {
    super(msg + ": " + cause, cause);
    _updateCount = updateCount;
  }

  public int getUpdateCount() {
    return _updateCount;
  }
}
