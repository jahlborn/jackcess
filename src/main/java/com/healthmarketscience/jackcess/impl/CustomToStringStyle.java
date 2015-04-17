/*
Copyright (c) 2013 James Ahlborn

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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang.builder.StandardToStringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Custom ToStringStyle for use with ToStringBuilder.
 *
 * @author James Ahlborn
 */
public class CustomToStringStyle extends StandardToStringStyle 
{
  private static final long serialVersionUID = 0L;

  private static final String ML_FIELD_SEP = SystemUtils.LINE_SEPARATOR + "  ";
  private static final String IMPL_SUFFIX = "Impl";
  private static final int MAX_BYTE_DETAIL_LEN = 20;
  
  public static final CustomToStringStyle INSTANCE = new CustomToStringStyle() {
    private static final long serialVersionUID = 0L;
    {
      setContentStart("[");
      setFieldSeparator(ML_FIELD_SEP);
      setFieldSeparatorAtStart(true);
      setFieldNameValueSeparator(": ");
      setArraySeparator("," + ML_FIELD_SEP);
      setContentEnd(SystemUtils.LINE_SEPARATOR + "]");
      setUseShortClassName(true);
    }
  };

  public static final CustomToStringStyle VALUE_INSTANCE = new CustomToStringStyle() {
    private static final long serialVersionUID = 0L;
    {
      setUseShortClassName(true);
      setUseIdentityHashCode(false);
    }
  };

  private CustomToStringStyle() {    
  }

  public static ToStringBuilder builder(Object obj) {
    return new ToStringBuilder(obj, INSTANCE);
  }

  public static ToStringBuilder valueBuilder(Object obj) {
    return new ToStringBuilder(obj, VALUE_INSTANCE);
  }

  @Override
  protected void appendClassName(StringBuffer buffer, Object obj) {
    if(obj instanceof String) {
      // the caller gave an "explicit" class name
      buffer.append(obj);
    } else {
      super.appendClassName(buffer, obj);
    }
  }

  @Override
  protected String getShortClassName(Class clss) {
    String shortName = super.getShortClassName(clss);
    if(shortName.endsWith(IMPL_SUFFIX)) {
      shortName = shortName.substring(0, 
                                      shortName.length() - IMPL_SUFFIX.length());
    }
    int idx = shortName.lastIndexOf('.');
    if(idx >= 0) {
      shortName = shortName.substring(idx + 1);
    }
    return shortName;
  }

  @Override
  protected void appendDetail(StringBuffer buffer, String fieldName, 
                              Object value) {
    if(value instanceof ByteBuffer) {
      appendDetail(buffer, (ByteBuffer)value);
    } else {
      buffer.append(indent(value));
    }
  }

  @Override
  protected void appendDetail(StringBuffer buffer, String fieldName, 
                              Collection value) {
    buffer.append("[");

    // gather contents of list in a new StringBuffer
    StringBuffer sb = new StringBuffer();
    Iterator<?> iter = value.iterator();
    if(iter.hasNext()) {
      if(isFieldSeparatorAtStart()) {
        appendFieldSeparator(sb);
      }
      appendValueDetail(sb, fieldName, iter.next());
    }
    while(iter.hasNext()) {
      sb.append(getArraySeparator());
      appendValueDetail(sb, fieldName, iter.next());
    }

    // indent entire list contents another level
    buffer.append(indent(sb));

    if(isFieldSeparatorAtStart()) {
      appendFieldSeparator(buffer);
    }
    buffer.append("]");
  }


  @Override
  protected void appendDetail(StringBuffer buffer, String fieldName,
                              Map value) {
    buffer.append("{");

    // gather contents of map in a new StringBuffer
    StringBuffer sb = new StringBuffer();
    @SuppressWarnings("unchecked")
    Iterator<Map.Entry<?,?>> iter = value.entrySet().iterator();
    if(iter.hasNext()) {
      if(isFieldSeparatorAtStart()) {
        appendFieldSeparator(sb);
      }
      Map.Entry<?,?> e = iter.next();
      sb.append(e.getKey()).append("=");
      appendValueDetail(sb, fieldName, e.getValue());
    }
    while(iter.hasNext()) {
      sb.append(getArraySeparator());
      Map.Entry<?,?> e = iter.next();
      sb.append(e.getKey()).append("=");
      appendValueDetail(sb, fieldName, e.getValue());
    }

    // indent entire map contents another level
    buffer.append(indent(sb));

    if(isFieldSeparatorAtStart()) {
      appendFieldSeparator(buffer);
    }
    buffer.append("}");
  }

  @Override
  protected void appendDetail(StringBuffer buffer, String fieldName, 
                              byte[] array) {
    appendDetail(buffer, PageChannel.wrap(array));
  }

  private void appendValueDetail(StringBuffer buffer, String fieldName, 
                                 Object value) {
    if (value == null) {
      appendNullText(buffer, fieldName);
    } else {
      appendInternal(buffer, fieldName, value, true);
    }    
  }

  private static void appendDetail(StringBuffer buffer, ByteBuffer bb) {
    int len = bb.remaining();
    buffer.append("(").append(len).append(") ");
    buffer.append(ByteUtil.toHexString(bb, bb.position(), 
                                       Math.min(len, MAX_BYTE_DETAIL_LEN)));
    if(len > MAX_BYTE_DETAIL_LEN) {
      buffer.append(" ...");
    }      
  }

  private static String indent(Object obj) {
    return ((obj != null) ? obj.toString().replaceAll(
                SystemUtils.LINE_SEPARATOR, ML_FIELD_SEP) : null);
  }
}
