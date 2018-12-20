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

package com.healthmarketscience.jackcess.impl.complex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.impl.PropertyMapImpl;

/**
 * PropertyMap implementation for multi-value, complex properties.  The
 * properties for these columns seem to be dispersed between both the primary
 * column and the complex value column.  The primary column only seems to have
 * the simple "multi-value" property and the rest seem to be on the complex
 * value column.  This PropertyMap implementation combines them into one
 * synthetic map.
 *
 * @author James Ahlborn
 */
public class MultiValueColumnPropertyMap implements PropertyMap
{
  /** properties from the primary column */
  private final PropertyMap _primary;
  /** properties from the complex column */
  private final PropertyMap _complex;

  public MultiValueColumnPropertyMap(PropertyMap primary, PropertyMap complex) 
  {
    _primary = primary;
    _complex = complex;
  }

  @Override
  public String getName() {
    return _primary.getName();
  }

  @Override
  public int getSize() {
    return _primary.getSize() + _complex.getSize();
  }

  @Override
  public boolean isEmpty() {
    return _primary.isEmpty() && _complex.isEmpty();
  }

  @Override
  public Property get(String name) {
    Property prop = _primary.get(name);
    if(prop != null) {
      return prop;
    }
    return _complex.get(name);
  }

  @Override
  public Object getValue(String name) {
    return getValue(name, null);
  }

  @Override
  public Object getValue(String name, Object defaultValue) {
    Property prop = get(name);
    return ((prop != null) ? prop.getValue() : defaultValue);
  }

  @Override
  public Property put(String name, Object value) {
    return put(name, null, value, false);
  }

  @Override
  public Property put(String name, DataType type, Object value) {
    return put(name, type, value, false);
  }

  @Override
  public Property put(String name, DataType type, Object value, boolean isDdl) {
    // the only property which seems to go in the "primary" is the "multi
    // value" property
    if(isPrimaryKey(name)) {
      return _primary.put(name, DataType.BOOLEAN, value, true);
    }
    return _complex.put(name, type, value, isDdl);
  }

  @Override
  public void putAll(Iterable<? extends Property> props) {
    if(props == null) {
      return;
    }

    for(Property prop : props) {
      if(isPrimaryKey(prop.getName())) {
        ((PropertyMapImpl)_primary).put(prop);
      } else {
        ((PropertyMapImpl)_complex).put(prop);
      }
    }
  }  

  @Override
  public Property remove(String name) {
    if(isPrimaryKey(name)) {
      return _primary.remove(name);
    }
    return _complex.remove(name);
  }

  @Override
  public void save() throws IOException {
    _primary.save();
    _complex.save();
  }

  @Override
  public Iterator<Property> iterator() {
    final List<Iterator<Property>> iters = new ArrayList<Iterator<Property>>(2);
    iters.add(_primary.iterator());
    iters.add(_complex.iterator());

    return new Iterator<Property>() {
      private Iterator<Property> _cur;
      private Property _next = findNext();

      private Property findNext() {
        while(!iters.isEmpty()) {
          _cur = iters.get(0);
          if(_cur.hasNext()) {
            return _cur.next();
          }
          iters.remove(0);
          _cur = null;
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return (_next != null);
      }

      @Override
      public Property next() {
        if(!hasNext()) {
          throw new NoSuchElementException();
        }
        Property prop = _next;
        _next = findNext();
        return prop;
      }

      @Override
      public void remove() {
        if(_cur != null) {
          _cur.remove();
          _cur = null;
        }
      }
    };
  }

  @Override
  public String toString() {
    return PropertyMapImpl.toString(this);
  }

  private static boolean isPrimaryKey(String name) {
    // the multi-value key seems to be the only one on the primary column
    return ALLOW_MULTI_VALUE_PROP.equals(name);
  } 
}
