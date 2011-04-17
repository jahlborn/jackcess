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

package com.healthmarketscience.jackcess;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Map of properties for a given database object.
 *
 * @author James Ahlborn
 */
public class PropertyMap implements Iterable<PropertyMap.Property>
{
  public static final String ACCESS_VERSION_PROP = "AccessVersion";
  public static final String TITLE_PROP = "Title";
  public static final String AUTHOR_PROP = "Author";
  public static final String COMPANY_PROP = "Company";

  public static final String DEFAULT_VALUE_PROP = "DefaultValue";
  public static final String REQUIRED_PROP = "Required";
  public static final String ALLOW_ZERO_LEN_PROP = "AllowZeroLength";
  public static final String DECIMAL_PLACES_PROP = "DecimalPlaces";
  public static final String FORMAT_PROP = "Format";
  public static final String INPUT_MASK_PROP = "InputMask";
  public static final String CAPTION_PROP = "Caption";
  public static final String VALIDATION_RULE_PROP = "ValidationRule";
  public static final String VALIDATION_TEXT_PROP = "ValidationText";
  public static final String GUID_PROP = "GUID";
  public static final String DESCRIPTION_PROP = "Description";

  private final String _mapName;
  private final short _mapType;
  private final Map<String,Property> _props = 
    new LinkedHashMap<String,Property>();

  PropertyMap(String name, short type) {
    _mapName = name;
    _mapType = type;
  }

  public String getName() {
    return _mapName;
  }

  public short getType() {
    return _mapType;
  }

  public int getSize() {
    return _props.size();
  }

  public boolean isEmpty() {
    return _props.isEmpty();
  }

  /**
   * @return the property with the given name, if any
   */
  public Property get(String name) {
    return _props.get(Database.toLookupName(name));
  }

  /**
   * @return the value of the property with the given name, if any
   */
  public Object getValue(String name) {
    return getValue(name, null);
  }

  /**
   * @return the value of the property with the given name, if any, otherwise
   *         the given defaultValue
   */
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
    _props.put(Database.toLookupName(name), 
               new Property(name, type, flag, value));
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
  public static final class Property
  {
    private final String _name;
    private final DataType _type;
    private final byte _flag;
    private final Object _value;

    private Property(String name, DataType type, byte flag, Object value) {
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
