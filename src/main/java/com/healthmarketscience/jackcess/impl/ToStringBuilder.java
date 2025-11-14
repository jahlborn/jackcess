/*
Copyright (c) 2025 Markus Spann

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

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.function.Function;

/**
 * <p>Builder for {@link Object#toString()} methods.</p>
 *
 * Heavily borrowed/adapted/simplified from commons lang ToStringBuilder.
 *
 * @author Markus Spann
 */
public final class ToStringBuilder
{
  /** Object registry for avoidance of cycles. */
  private static final ThreadLocal<Map<Object, Object>> REGISTRY = new ThreadLocal<>();

  private static final int MAX_BYTE_DETAIL_LEN = 20;
  private static final String NULL_TEXT = "<null>";
  private static final String IMPL_SUFF = "Impl";
  private static final String LINE_SEP = System.lineSeparator();

  private final StringBuilder _buffer;
  private final Object _object;
  private final String _fieldSeparator;
  private final boolean _fieldSeparatorSurround;
  private final String _fieldNameValueSeparator;
  private final String _arraySeparator;
  private final String _contentStart;
  private final String _contentEnd;
  private final boolean _useIdentityHashCode;
  private final Function<Object,String> _valueFormatter;

  private ToStringBuilder(
      Object object, String fieldSeparator, boolean fieldSeparatorSurround,
      String fieldNameValueSeparator, String arraySeparator,
      String contentStart, String contentEnd, boolean useIdentityHashCode) {
    _buffer = new StringBuilder(512);
    _object = object;
    _fieldSeparator = Optional.ofNullable(fieldSeparator).orElse(",");
    _fieldSeparatorSurround = fieldSeparatorSurround;
    _fieldNameValueSeparator = Optional.ofNullable(fieldNameValueSeparator).orElse("=");
    _arraySeparator = Optional.ofNullable(arraySeparator).orElse(",");
    _contentStart = Optional.ofNullable(contentStart).orElse("[");
    _contentEnd = Optional.ofNullable(contentEnd).orElse("]");
    _useIdentityHashCode = useIdentityHashCode;

    _valueFormatter = (_fieldSeparatorSurround &&
                       (_fieldSeparator.length() > LINE_SEP.length()) &&
                       _fieldSeparator.startsWith(LINE_SEP)) ?
      this::indentValue : Object::toString;

    if (_object != null) {
      register(_object);
      _buffer.append(getShortClassName(_object));
      if (_useIdentityHashCode) {
        appendIdentityHashCode(_object, _buffer);
      }
      _buffer.append(_contentStart);
    }
  }

  public static ToStringBuilder valueBuilder(Object obj) {
    return new ToStringBuilder(obj, null, false, null, null, null, null, false);
  }

  public static ToStringBuilder builder(Object obj) {
    String fieldSep = LINE_SEP + "  ";
    return new ToStringBuilder(obj, fieldSep, true, ": ", "," + fieldSep,
                               "[" + fieldSep, LINE_SEP + "]", true);
  }

  public ToStringBuilder append(String fieldName, Object value) {
    if (fieldName != null) {
      _buffer.append(fieldName).append(_fieldNameValueSeparator);
    }

    appendValue(value, _buffer);

    _buffer.append(_fieldSeparator);
    return this;
  }

  public ToStringBuilder appendIfNotNull(String fieldName, Object value) {
    if(value == null) {
      return this;
    }
    return append(fieldName, value);
  }

  public String reflectionToString() {

    Class<?> clazz = _object.getClass();
    while(clazz != null) {
      appendFieldsIn(clazz);
      clazz = clazz.getSuperclass();
    }

    return toString();
  }

  @Override
  public String toString() {
    if (_object == null) {
      _buffer.append(NULL_TEXT);
    } else {
      removeLastFieldSeparator();
      _buffer.append(_contentEnd);
      unregister(_object);
    }
    return _buffer.toString();
  }

  private void appendInternal(Object value, StringBuilder buffer) {
    boolean primitiveWrapper = (value instanceof Number) || (value instanceof Boolean)
      || (value instanceof Character);

    if (isRegistered(value) && !primitiveWrapper) {
      buffer.append(value.getClass().getName());
      appendIdentityHashCode(value, buffer);
      return;
    }

    register(value);

    try {

      if (value instanceof byte[]) {

        appendByteArrayInternal((byte[]) value, buffer);

      } else if (value.getClass().isArray()) {

        appendArrayInternal(value, buffer);

      } else if (value instanceof Collection<?>) {

        appendCollectionInternal((Collection<?>)value, buffer);

      } else if (value instanceof Map<?, ?>) {

        appendMapInternal((Map<?,?>)value, buffer);

      } else {

        buffer.append(_valueFormatter.apply(value));
      }

    } finally {
      unregister(value);
    }
  }

  private static void appendByteArrayInternal(byte[] bar, StringBuilder buffer) {
    ByteBuffer bb = PageChannel.wrap(bar);
    int len = bb.remaining();
    buffer.append("(").append(len).append(") ").append(
        ByteUtil.toHexString(bb, bb.position(), Math.min(len, MAX_BYTE_DETAIL_LEN)));
    if (len > MAX_BYTE_DETAIL_LEN) {
      buffer.append("...");
    }
  }

  private void appendArrayInternal(Object arr, StringBuilder buffer) {

    buffer.append('[');

    int len = Array.getLength(arr);
    if(len > 0) {
      StringBuilder valueBuffer = new StringBuilder(512);
      if(_fieldSeparatorSurround) {
        valueBuffer.append(_fieldSeparator);
      }
      for (int i = 0; i < len; i++) {
        if (i > 0) {
          valueBuffer.append(_arraySeparator);
        }
        appendValue(Array.get(arr, i), valueBuffer);
      }
      buffer.append(_valueFormatter.apply(valueBuffer));
      if(_fieldSeparatorSurround){
        buffer.append(_fieldSeparator);
      }
    }

    buffer.append(']');
  }

  private void appendCollectionInternal(Collection<?> col, StringBuilder buffer) {

    buffer.append('[');

    if(!col.isEmpty()) {
      StringBuilder valueBuffer = new StringBuilder(512);
      if(_fieldSeparatorSurround) {
        valueBuffer.append(_fieldSeparator);
      }
      boolean isFirst = true;
      for(Object v : col) {
        if(!isFirst) {
          valueBuffer.append(_arraySeparator);
        }
        appendValue(v, valueBuffer);
        isFirst = false;
      }
      buffer.append(_valueFormatter.apply(valueBuffer));
      if(_fieldSeparatorSurround) {
        buffer.append(_fieldSeparator);
      }
    }

    buffer.append(']');
  }

  private void appendMapInternal(Map<?,?> map, StringBuilder buffer) {

    buffer.append('{');

    if(!map.isEmpty()) {
      StringBuilder valueBuffer = new StringBuilder(512);
      if(_fieldSeparatorSurround) {
        valueBuffer.append(_fieldSeparator);
      }
      boolean isFirst = true;
      for(Map.Entry<?,?> e : map.entrySet()) {
        if(!isFirst) {
          valueBuffer.append(_arraySeparator);
        }
        valueBuffer.append(e.getKey()).append("=");
        appendValue(e.getValue(), valueBuffer);
        isFirst = false;
      }
      buffer.append(_valueFormatter.apply(valueBuffer));
      if(_fieldSeparatorSurround) {
        buffer.append(_fieldSeparator);
      }
    }

    buffer.append('}');
  }

  private void appendValue(Object value, StringBuilder buffer) {
    if (value == null) {
      buffer.append(NULL_TEXT);
    } else {
      appendInternal(value, buffer);
    }
  }

  private String indentValue(Object value) {
    String valueStr = value.toString();
    if(valueStr != null) {
      valueStr = valueStr.replace(LINE_SEP, _fieldSeparator);
    }
    return valueStr;
  }

  private void appendFieldsIn(final Class<?> clazz) {
    Field[] fields = clazz.getDeclaredFields();
    Arrays.sort(fields, Comparator.comparing(Field::getName));
    AccessibleObject.setAccessible(fields, true);
    for (final Field field : fields) {
      String fieldName = field.getName();
      if (acceptReflectionField(field)) {
        try {
          Object value = field.get(_object);
          if(value != null) {
            if(fieldName.startsWith("_")) {
              fieldName = fieldName.substring(1);
            }
            append(fieldName, value);
          }
        } catch (final IllegalAccessException ex) {
          // this shouldn't happen. Would get a Security exception instead
          throw new InternalError("Unexpected IllegalAccessException: " + ex.getMessage());
        }
      }
    }
  }

  private static void appendIdentityHashCode(Object value, StringBuilder buffer) {
    buffer.append('@').append(Integer.toHexString(System.identityHashCode(value)));
  }

  private static boolean acceptReflectionField(final Field field) {
    // Reject field from inner class.
    return ((field.getName().indexOf('$') < 0) &&
            // Reject static fields.
            !Modifier.isStatic(field.getModifiers()));
  }

  private static String getShortClassName(Object value) {
    if(value instanceof String) {
      // caller passed in explicit "class" name
      return (String)value;
    }
    String nm = value.getClass().getSimpleName();
    if (nm.endsWith(IMPL_SUFF)) {
      nm = nm.substring(0, nm.length() - IMPL_SUFF.length());
    }
    int idx = nm.lastIndexOf('.');
    return idx >= 0 ? nm.substring(idx + 1) : nm;
  }

  private void removeLastFieldSeparator() {
    int len = _buffer.length();
    int sepLen = _fieldSeparator.length();
    if (len > 0 && sepLen > 0 && len >= sepLen) {
      for (int i = 0; i < sepLen; i++) {
        if (_buffer.charAt(len - 1 - i) != _fieldSeparator.charAt(sepLen - 1 - i)) {
          return;
        }
      }
      _buffer.setLength(len - sepLen);
    }
  }

  private static boolean isRegistered(Object value) {
    final Map<Object, Object> m = getRegistry();
    return m != null && m.containsKey(value);
  }

  private static void register(Object value) {
    if (value != null) {
      Map<Object, Object> m = getRegistry();
      if (m == null) {
        REGISTRY.set(new WeakHashMap<>());
      }
      getRegistry().put(value, null);
    }
  }

  private static void unregister(Object value) {
    if (value != null) {
      Map<Object, Object> m = getRegistry();
      if (m != null) {
        m.remove(value);
        if (m.isEmpty()) {
          REGISTRY.remove();
        }
      }
    }
  }

  private static Map<Object, Object> getRegistry() {
    return REGISTRY.get();
  }
}
