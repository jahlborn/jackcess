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
import java.util.stream.Collectors;

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
  private final boolean _fieldSeparatorAtStart;
  private final String _fieldNameValueSeparator;
  private final String _contentStart;
  private final String _contentEnd;
  private final boolean _useIdentityHashCode;

  private ToStringBuilder(
      Object object, String fieldSeparator, boolean fieldSeparatorAtStart,
      String fieldNameValueSeparator, String contentEnd, boolean useIdentityHashCode) {
    _buffer = new StringBuilder(512);
    _object = object;
    _fieldSeparator = Optional.ofNullable(fieldSeparator).orElse(",");
    _fieldSeparatorAtStart = fieldSeparatorAtStart;
    _fieldNameValueSeparator = Optional.ofNullable(fieldNameValueSeparator).orElse("=");
    _contentStart = "[";
    _contentEnd = Optional.ofNullable(contentEnd).orElse("]");
    _useIdentityHashCode = useIdentityHashCode;

    if (_object != null) {
      _buffer.append((_object instanceof String) ?
                     _object :
                     getShortClassName(_object.getClass()));
      if (_useIdentityHashCode) {
        _buffer.append('@').append(Integer.toHexString(System.identityHashCode(_object)));
      }
      _buffer.append(_contentStart);
      if (_fieldSeparatorAtStart) {
        _buffer.append(_fieldSeparator);
      }
    }
  }

  public static ToStringBuilder valueBuilder(Object obj) {
    return new ToStringBuilder(obj, null, false, null, null, false);
  }

  public static ToStringBuilder builder(Object obj) {
    return new ToStringBuilder(obj, LINE_SEP + "  ", true, ": ",
                               LINE_SEP + "]", true);
  }

  public ToStringBuilder append(String fieldName, Object value) {
    if (fieldName != null) {
      _buffer.append(fieldName).append(_fieldNameValueSeparator);
    }

    if (value == null) {
      _buffer.append(NULL_TEXT);
    } else {
      appendInternal(fieldName, value);
    }

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
    }
    return _buffer.toString();
  }

  private void appendInternal(String fieldName, Object value) {
    boolean primitiveWrapper = (value instanceof Number) || (value instanceof Boolean)
      || (value instanceof Character);

    if (isRegistered(value) && !primitiveWrapper) {
      _buffer.append(value.getClass().getName() + '@' +
                     Integer.toHexString(System.identityHashCode(value)));
      return;
    }

    register(value);

    try {

      if (value instanceof byte[]) {

        ByteBuffer bb = PageChannel.wrap((byte[]) value);
        int len = bb.remaining();
        _buffer.append("(").append(len).append(") ").append(
            ByteUtil.toHexString(bb, bb.position(), Math.min(len, MAX_BYTE_DETAIL_LEN)));
        if (len > MAX_BYTE_DETAIL_LEN) {
          _buffer.append("...");
        }

      } else if (value.getClass().isArray()) {

        Object arr = value;
        _buffer.append('[');
        for (int i = 0; i < Array.getLength(arr); i++) {
          Object item = Array.get(arr, i);
          if (i > 0) {
            _buffer.append(_fieldSeparator);
          }
          if (item == null) {
            _buffer.append(NULL_TEXT);
          } else {
            appendInternal(fieldName, item); // recursive call
          }
        }
        _buffer.append(']');

      } else if (value instanceof Collection<?>) {

        String str = ((Collection<?>) value).stream()
          .map(v -> (v == null) ? NULL_TEXT : v.toString()).collect(Collectors.joining(","));
        _buffer.append('[').append(str).append(']');

      } else if (value instanceof Map<?, ?>) {

        String str = ((Map<?, ?>) value).entrySet().stream()
          .map(e -> e.getKey() + "=" + ((e.getValue() == null) ? NULL_TEXT : e.getValue()))
          .collect(Collectors.joining(","));
        _buffer.append('{').append(str).append('}');

      } else {

        _buffer.append(value);
      }

    } finally {
      unregister(value);
    }
  }

  protected void appendFieldsIn(final Class<?> clazz) {
    Field[] fields = clazz.getDeclaredFields();
    Arrays.sort(fields, Comparator.comparing(Field::getName));
    AccessibleObject.setAccessible(fields, true);
    for (final Field field : fields) {
      String fieldName = field.getName();
      if (acceptReflectionField(field)) {
        if(fieldName.startsWith("_")) {
          fieldName = fieldName.substring(1);
        }
        try {
          append(fieldName, field.get(_object));
        } catch (final IllegalAccessException ex) {
          //this shouldn't happen. Would get a Security exception instead
          throw new InternalError("Unexpected IllegalAccessException: " + ex.getMessage());
        }
      }
    }
  }

  private static boolean acceptReflectionField(final Field field) {
    // Reject field from inner class.
    return ((field.getName().indexOf('$') < 0) &&
            // Reject static fields.
            !Modifier.isStatic(field.getModifiers()));
  }

  private static String getShortClassName(Class<?> clazz) {
    String nm = clazz.getSimpleName();
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
      boolean match = true;
      for (int i = 0; i < sepLen; i++) {
        if (_buffer.charAt(len - 1 - i) != _fieldSeparator.charAt(sepLen - 1 - i)) {
          return;
        }
      }
      if (match) {
        _buffer.setLength(len - sepLen);
      }
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
