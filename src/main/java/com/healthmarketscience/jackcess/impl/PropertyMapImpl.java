/*
Copyright (c) 2011 James Ahlborn

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA
*/

package com.healthmarketscience.jackcess.impl;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.PropertyMap;

/**
 * Map of properties for a database object.
 *
 * @author James Ahlborn
 */
public class PropertyMapImpl implements PropertyMap
{
  private final String _mapName;
  private final short _mapType;
  private final Map<String,Property> _props = 
    new LinkedHashMap<String,Property>();
  private final PropertyMaps _owner;

  public PropertyMapImpl(String name, short type, PropertyMaps owner) {
    _mapName = name;
    _mapType = type;
    _owner = owner;
  }

  public String getName() {
    return _mapName;
  }

  public short getType() {
    return _mapType;
  }

  public PropertyMaps getOwner() {
    return _owner;
  }

  public int getSize() {
    return _props.size();
  }

  public boolean isEmpty() {
    return _props.isEmpty();
  }

  public Property get(String name) {
    return _props.get(DatabaseImpl.toLookupName(name));
  }

  public Object getValue(String name) {
    return getValue(name, null);
  }

  public Object getValue(String name, Object defaultValue) {
    Property prop = get(name);
    Object value = defaultValue;
    if((prop != null) && (prop.getValue() != null)) {
      value = prop.getValue();
    }
    return value;
  }

  /**
   * Puts a property into this map with the given information.
   */
  public void put(String name, DataType type, byte flag, Object value) {
    _props.put(DatabaseImpl.toLookupName(name), 
               new PropertyImpl(name, type, flag, value));
  }

  public Iterator<Property> iterator() {
    return _props.values().iterator();
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(PropertyMaps.DEFAULT_NAME.equals(getName()) ?
              "<DEFAULT>" : getName())
      .append(" {");
    for(Iterator<Property> iter = iterator(); iter.hasNext(); ) {
      sb.append(iter.next());
      if(iter.hasNext()) {
        sb.append(",");
      }
    }
    sb.append("}");
    return sb.toString();
  }      

  /**
   * Info about a property defined in a PropertyMap.
   */ 
  static final class PropertyImpl implements PropertyMap.Property
  {
    private final String _name;
    private final DataType _type;
    private final byte _flag;
    private final Object _value;

    private PropertyImpl(String name, DataType type, byte flag, Object value) {
      _name = name;
      _type = type;
      _flag = flag;
      _value = value;
    }

    public String getName() {
      return _name;
    }

    public DataType getType() {
      return _type;
    }

    public Object getValue() {
      return _value;
    }

    public byte getFlag() {
      return _flag;
    }

    @Override
    public String toString() {
      Object val = getValue();
      if(val instanceof byte[]) {
        val = ByteUtil.toHexString((byte[])val);
      }
      return getName() + "[" + getType() + ":" + _flag + "]=" + val;
    }
  }

}
