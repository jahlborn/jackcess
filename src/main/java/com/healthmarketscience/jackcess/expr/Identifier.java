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

package com.healthmarketscience.jackcess.expr;

import org.apache.commons.lang3.ObjectUtils;

/**
 * identifies a database entity (e.g. the name of a database field).  An
 * Identify must have an object name, but the collection name and property
 * name are optional.
 *
 * @author James Ahlborn
 */
public class Identifier
{
  private final String _collectionName;
  private final String _objectName;
  private final String _propertyName;

  public Identifier(String objectName)
  {
    this(null, objectName, null);
  }

  public Identifier(String collectionName, String objectName, String propertyName)
  {
    _collectionName = collectionName;
    _objectName = objectName;
    _propertyName = propertyName;
  }

  public String getCollectionName()
  {
    return _collectionName;
  }

  public String getObjectName()
  {
    return _objectName;
  }

  public String getPropertyName()
  {
    return _propertyName;
  }

  @Override
  public int hashCode() {
    return _objectName.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if(!(o instanceof Identifier)) {
      return false;
    }

    Identifier oi = (Identifier)o;

    return (ObjectUtils.equals(_objectName, oi._objectName) &&
            ObjectUtils.equals(_collectionName, oi._collectionName) &&
            ObjectUtils.equals(_propertyName, oi._propertyName));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if(_collectionName != null) {
      sb.append("[").append(_collectionName).append("].");
    }
    sb.append("[").append(_objectName).append("]");
    if(_propertyName != null) {
      sb.append(".[").append(_propertyName).append("]");
    }
    return sb.toString();
  }

}
