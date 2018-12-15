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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
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
  private static final Map<String,PropDef> DEFAULT_TYPES =
    new HashMap<String,PropDef>();

  static {
    DEFAULT_TYPES.put(ACCESS_VERSION_PROP, new PropDef(DataType.TEXT, false));
    DEFAULT_TYPES.put(TITLE_PROP, new PropDef(DataType.TEXT, false));
    DEFAULT_TYPES.put(AUTHOR_PROP, new PropDef(DataType.TEXT, false));
    DEFAULT_TYPES.put(COMPANY_PROP, new PropDef(DataType.TEXT, false));

    DEFAULT_TYPES.put(DEFAULT_VALUE_PROP, new PropDef(DataType.MEMO, true));
    DEFAULT_TYPES.put(REQUIRED_PROP, new PropDef(DataType.BOOLEAN, true));
    DEFAULT_TYPES.put(ALLOW_ZERO_LEN_PROP, new PropDef(DataType.BOOLEAN, true));
    DEFAULT_TYPES.put(DECIMAL_PLACES_PROP, new PropDef(DataType.BYTE, true));
    DEFAULT_TYPES.put(FORMAT_PROP, new PropDef(DataType.TEXT, true));
    DEFAULT_TYPES.put(INPUT_MASK_PROP, new PropDef(DataType.TEXT, true));
    DEFAULT_TYPES.put(CAPTION_PROP, new PropDef(DataType.MEMO, false));
    DEFAULT_TYPES.put(VALIDATION_RULE_PROP, new PropDef(DataType.TEXT, true));
    DEFAULT_TYPES.put(VALIDATION_TEXT_PROP, new PropDef(DataType.TEXT, true));
    DEFAULT_TYPES.put(GUID_PROP, new PropDef(DataType.BINARY, true));
    DEFAULT_TYPES.put(DESCRIPTION_PROP, new PropDef(DataType.MEMO, false));
    DEFAULT_TYPES.put(RESULT_TYPE_PROP, new PropDef(DataType.BYTE, true));
    DEFAULT_TYPES.put(EXPRESSION_PROP, new PropDef(DataType.MEMO, true));
    DEFAULT_TYPES.put(DISPLAY_CONTROL_PROP, new PropDef(DataType.INT, false));
    DEFAULT_TYPES.put(TEXT_FORMAT_PROP, new PropDef(DataType.BYTE, false));
    DEFAULT_TYPES.put(IME_MODE_PROP, new PropDef(DataType.BYTE, false));
    DEFAULT_TYPES.put(IME_SENTENCE_MODE_PROP, new PropDef(DataType.BYTE, false));
  }

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

  public PropertyImpl put(String name, Object value) {
    return put(name, null, value, false);
  }

  public PropertyImpl put(String name, DataType type, Object value) {
    return put(name, type, value, false);
  }

  public void putAll(Iterable<? extends Property> props) {
    if(props == null) {
      return;
    }

    for(Property prop : props) {
      put(prop);
    }
  }

  public PropertyImpl put(Property prop) {
    return put(prop.getName(), prop.getType(), prop.getValue(), prop.isDdl());
  }

  /**
   * Puts a property into this map with the given information.
   */
  public PropertyImpl put(String name, DataType type, Object value,
                          boolean isDdl) {
    PropertyImpl prop = (PropertyImpl)createProperty(name, type, value, isDdl);
    _props.put(DatabaseImpl.toLookupName(name), prop);
    return prop;
  }

  public PropertyImpl remove(String name) {
    return (PropertyImpl)_props.remove(DatabaseImpl.toLookupName(name));
  }

  public Iterator<Property> iterator() {
    return _props.values().iterator();
  }

  public void save() throws IOException {
    getOwner().save();
  }

  @Override
  public String toString() {
    return toString(this);
  }

  public static String toString(PropertyMap map) {
    StringBuilder sb = new StringBuilder();
    sb.append(PropertyMaps.DEFAULT_NAME.equals(map.getName()) ?
              "<DEFAULT>" : map.getName())
      .append(" {");
    for(Iterator<Property> iter = map.iterator(); iter.hasNext(); ) {
      sb.append(iter.next());
      if(iter.hasNext()) {
        sb.append(",");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  public static Property createProperty(String name, DataType type, Object value) {
    return createProperty(name, type, value, false);
  }

  public static Property createProperty(String name, DataType type,
                                        Object value, boolean isDdl) {
    // see if this is a builtin property that we already understand
    PropDef pd = DEFAULT_TYPES.get(name);

    if(value instanceof PropertyMap.EnumValue) {
      // convert custom enum to stored value
      value = ((PropertyMap.EnumValue)value).getValue();
    }

    if(pd != null) {
      // update according to the default info
      type = ((type == null) ? pd._type : type);
      isDdl |= pd._isDdl;
    } else if(type == null) {
      // choose the type based on the value
      if(value instanceof String) {
        type = DataType.TEXT;
      } else if(value instanceof Boolean) {
        type = DataType.BOOLEAN;
      } else if(value instanceof Byte) {
        type = DataType.BYTE;
      } else if(value instanceof Short) {
        type = DataType.INT;
      } else if(value instanceof Integer) {
        type = DataType.LONG;
      } else if(value instanceof Float) {
        type = DataType.FLOAT;
      } else if(value instanceof Double) {
        type = DataType.DOUBLE;
      } else if((value instanceof Date) || (value instanceof LocalDateTime)) {
        type = DataType.SHORT_DATE_TIME;
      } else if(value instanceof byte[]) {
        type = DataType.OLE;
      } else if(value instanceof Long) {
        type = DataType.BIG_INT;
      } else {
        throw new IllegalArgumentException(
            "Could not determine type for property " + name +
            " with value " + value);
      }
    }

    return new PropertyImpl(name, type, value, isDdl);
  }

  /**
   * Info about a property defined in a PropertyMap.
   */
  static final class PropertyImpl implements PropertyMap.Property
  {
    private final String _name;
    private final DataType _type;
    private final boolean _ddl;
    private Object _value;

    private PropertyImpl(String name, DataType type, Object value,
                         boolean ddl) {
      _name = name;
      _type = type;
      _ddl = ddl;
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

    public void setValue(Object newValue) {
      _value = newValue;
    }

    public boolean isDdl() {
      return _ddl;
    }

    @Override
    public String toString() {
      Object val = getValue();
      if(val instanceof byte[]) {
        val = ByteUtil.toHexString((byte[])val);
      }
      return getName() + "[" + getType() + (_ddl ? ":ddl" : "") + "]=" + val;
    }
  }

  /**
   * Helper for holding info about default properties
   */
  private static final class PropDef
  {
    private final DataType _type;
    private final boolean _isDdl;

    private PropDef(DataType type, boolean isDdl) {
      _type = type;
      _isDdl = isDdl;
    }
  }
}
