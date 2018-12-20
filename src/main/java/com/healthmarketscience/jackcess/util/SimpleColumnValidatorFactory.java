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

import com.healthmarketscience.jackcess.Column;

/**
 * Simple concrete implementation of ColumnValidatorFactory which returns
 * {@link SimpleColumnValidator#INSTANCE} for all columns.
 *
 * @author James Ahlborn
 */
public class SimpleColumnValidatorFactory implements ColumnValidatorFactory
{
  public static final SimpleColumnValidatorFactory INSTANCE =
    new SimpleColumnValidatorFactory();
  
  @Override
  public ColumnValidator createValidator(Column col) {
    return SimpleColumnValidator.INSTANCE;
  }
}
