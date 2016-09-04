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

import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.complex.ComplexColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.impl.complex.ComplexColumnInfoImpl;
import com.healthmarketscience.jackcess.impl.complex.MultiValueColumnInfoImpl;
import com.healthmarketscience.jackcess.impl.complex.MultiValueColumnPropertyMap;

/**
 * ColumnImpl subclass which is used for complex data types.
 * 
 * @author James Ahlborn
 * @usage _advanced_class_
 */
class ComplexColumnImpl extends ColumnImpl 
{
  /** additional information specific to complex columns */
  private final ComplexColumnInfo<? extends ComplexValue> _complexInfo;
  /** properties for multi-value column */
  private PropertyMap _mvProps;

  ComplexColumnImpl(InitArgs args) throws IOException
  {
    super(args);
    _complexInfo = ComplexColumnSupport.create(this, args.buffer, args.offset);
  }

  @Override
  void postTableLoadInit() throws IOException {
    if(_complexInfo != null) {
      ((ComplexColumnInfoImpl<? extends ComplexValue>)_complexInfo)
        .postTableLoadInit();
    }
    super.postTableLoadInit();
  }

  @Override
  public PropertyMap getProperties() throws IOException {
    if(_complexInfo.getType() == ComplexDataType.MULTI_VALUE) {
      if(_mvProps == null) {
        PropertyMap primaryProps = super.getProperties();
        PropertyMap complexProps = ((MultiValueColumnInfoImpl)_complexInfo)
          .getValueColumn().getProperties();
        _mvProps = new MultiValueColumnPropertyMap(primaryProps, complexProps);
      }
      return _mvProps;
    }
    return super.getProperties();
  }

  @Override
  public ComplexColumnInfo<? extends ComplexValue> getComplexInfo() {
    return _complexInfo;
  }
}
