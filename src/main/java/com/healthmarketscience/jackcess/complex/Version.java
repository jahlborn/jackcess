/*
Copyright (c) 2011 James Ahlborn

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

package com.healthmarketscience.jackcess.complex;

import java.util.Date;

/**
 * Complex value corresponding to a version of a memo column.
 *
 * @author James Ahlborn
 */
public interface Version extends ComplexValue, Comparable<Version>
{
  public String getValue();

  public Date getModifiedDate();
}
