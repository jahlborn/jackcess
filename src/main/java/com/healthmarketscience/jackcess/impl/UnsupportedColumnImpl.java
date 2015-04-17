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

import java.io.IOException;
import java.nio.ByteOrder;


/**
 * ColumnImpl subclass which is used for unknown/unsupported data types.
 * 
 * @author James Ahlborn
 * @usage _advanced_class_
 */
class UnsupportedColumnImpl extends ColumnImpl 
{
  private final byte _originalType;
  
  UnsupportedColumnImpl(InitArgs args) throws IOException
  {
    super(args);
    _originalType = args.colType;
  }

  @Override
  byte getOriginalDataType() {
    return _originalType;
  }

  @Override
  public Object read(byte[] data, ByteOrder order) throws IOException {
    return rawDataWrapper(data);
  }
}
