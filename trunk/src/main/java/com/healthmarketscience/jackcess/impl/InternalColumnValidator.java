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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.util.ColumnValidator;
import com.healthmarketscience.jackcess.util.SimpleColumnValidator;

/**
 * Base class for ColumnValidator instances handling "internal" validation
 * functionality, which are wrappers around any "external" behavior.
 *
 * @author James Ahlborn
 */
abstract class InternalColumnValidator implements ColumnValidator
{
  private ColumnValidator _delegate;

  protected InternalColumnValidator(ColumnValidator delegate)
  {
    _delegate = delegate;
  }

  ColumnValidator getExternal() {
    ColumnValidator extValidator = _delegate;
    while(extValidator instanceof InternalColumnValidator) {
      extValidator = ((InternalColumnValidator)extValidator)._delegate;
    }
    return extValidator;
  }

  void setExternal(ColumnValidator extValidator) {
    InternalColumnValidator intValidator = this;
    while(intValidator._delegate instanceof InternalColumnValidator) {
      intValidator = (InternalColumnValidator)intValidator._delegate;
    }
    intValidator._delegate = extValidator;
  }

  @Override
  public final Object validate(Column col, Object val) throws IOException {
    val = _delegate.validate(col, val);
    return internalValidate(col, val);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("{");
    if(_delegate instanceof InternalColumnValidator) {
      ((InternalColumnValidator)_delegate).appendToString(sb);
    } else if(_delegate != SimpleColumnValidator.INSTANCE) {
      sb.append("custom=").append(_delegate);
    }
    if(sb.length() > 1) {
      sb.append(";");
    }
    appendToString(sb);
    sb.append("}");
    return sb.toString();
  }

  protected abstract void appendToString(StringBuilder sb);

  protected abstract Object internalValidate(Column col, Object val)
    throws IOException;
}
