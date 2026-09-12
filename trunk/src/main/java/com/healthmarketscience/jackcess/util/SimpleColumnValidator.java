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
 * Simple concrete implementation of ColumnValidator which simply returns the
 * given value.
 *
 * @author James Ahlborn
 */
public class SimpleColumnValidator implements ColumnValidator
{
  public static final SimpleColumnValidator INSTANCE = new SimpleColumnValidator();
  
  @Override
  public Object validate(Column col, Object val) throws IOException {
    return val;
  }
}
