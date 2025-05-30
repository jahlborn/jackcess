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

package com.healthmarketscience.jackcess.impl;


/**
 * ColumnImpl subclass which is used for numeric data types.
 *
 * @author James Ahlborn
 * @usage _advanced_class_
 */
class NumericColumnImpl extends ColumnImpl
{
  /** Numeric precision */
  private final byte _precision;
  /** Numeric scale */
  private final byte _scale;

  NumericColumnImpl(InitArgs args)
  {
    super(args);

    _precision = args.buffer.get(
        args.offset + getFormat().OFFSET_COLUMN_PRECISION);
    _scale = args.buffer.get(args.offset + getFormat().OFFSET_COLUMN_SCALE);
  }

  @Override
  public byte getPrecision() {
    return _precision;
  }

  @Override
  public byte getScale() {
    return _scale;
  }
}
