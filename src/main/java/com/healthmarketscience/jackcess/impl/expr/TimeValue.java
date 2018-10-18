/*
Copyright (c) 2016 James Ahlborn

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

package com.healthmarketscience.jackcess.impl.expr;

import java.text.DateFormat;
import java.util.Date;

/**
 *
 * @author James Ahlborn
 */
public class TimeValue extends BaseDateValue
{

  public TimeValue(Date val, DateFormat fmt) 
  {
    super(val, fmt);
  }

  public Type getType() {
    return Type.TIME;
  }
}
