/*
Copyright (c) 2014 James Ahlborn

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
 * Interface which allows for data manipulation/validation as values are being
 * inserted into a database.
 *
 * @author James Ahlborn
 */
public interface ColumnValidator
{
  /**
   * Validates and/or manipulates the given potential new value for the given
   * column.  This method may return an entirely different value or throw an
   * exception if the input value is not valid.
   */
  public Object validate(Column col, Object val) throws IOException;
}
