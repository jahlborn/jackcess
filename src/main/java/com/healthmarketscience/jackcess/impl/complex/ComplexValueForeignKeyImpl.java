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

package com.healthmarketscience.jackcess.impl.complex;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.complex.AttachmentColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.complex.MultiValueColumnInfo;
import com.healthmarketscience.jackcess.complex.SingleValue;
import com.healthmarketscience.jackcess.complex.UnsupportedColumnInfo;
import com.healthmarketscience.jackcess.complex.UnsupportedValue;
import com.healthmarketscience.jackcess.complex.Version;
import com.healthmarketscience.jackcess.complex.VersionHistoryColumnInfo;

/**
 * Value which is returned for a complex column.  This value corresponds to a
 * foreign key in a secondary table which contains the actual complex data for
 * this row (which could be 0 or more complex values for a given row).  This
 * class contains various convenience methods for interacting with the actual
 * complex values.
 * <p>
 * This class will cache the associated complex values returned from one of
 * the lookup methods.  The various modification methods will clear this cache
 * automatically.  The {@link #reset} method may be called manually to clear
 * this internal cache.
 *
 * @author James Ahlborn
 */
public class ComplexValueForeignKeyImpl extends ComplexValueForeignKey
{
  private static final long serialVersionUID = 20110805L;

  private transient final Column _column;
  private final int _value;
  private transient List<? extends ComplexValue> _values;

  public ComplexValueForeignKeyImpl(Column column, int value) {
    _column = column;
    _value = value;
  }

  @Override
  public int get() {
    return _value;
  }

  @Override
  public Column getColumn() {
    return _column;
  }

  @Override
  public ComplexDataType getComplexType() {
    return getComplexInfo().getType();
  }

  protected ComplexColumnInfo<? extends ComplexValue> getComplexInfo() {
    return _column.getComplexInfo();
  }

  protected VersionHistoryColumnInfo getVersionInfo() {
    return (VersionHistoryColumnInfo)getComplexInfo();
  }

  protected AttachmentColumnInfo getAttachmentInfo() {
    return (AttachmentColumnInfo)getComplexInfo();
  }

  protected MultiValueColumnInfo getMultiValueInfo() {
    return (MultiValueColumnInfo)getComplexInfo();
  }

  protected UnsupportedColumnInfo getUnsupportedInfo() {
    return (UnsupportedColumnInfo)getComplexInfo();
  }

  @Override
  public int countValues() throws IOException {
    return getComplexInfo().countValues(get());
  }

  public List<Row> getRawValues() throws IOException {
    return getComplexInfo().getRawValues(get());
  }

  @Override
  public List<? extends ComplexValue> getValues() throws IOException {
    if(_values == null) {
      _values = getComplexInfo().getValues(this);
    }
    return _values;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Version> getVersions() throws IOException {
    if(getComplexType() != ComplexDataType.VERSION_HISTORY) {
      throw new UnsupportedOperationException();
    }
    return (List<Version>)getValues();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Attachment> getAttachments() throws IOException {
    if(getComplexType() != ComplexDataType.ATTACHMENT) {
      throw new UnsupportedOperationException();
    }
    return (List<Attachment>)getValues();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<SingleValue> getMultiValues() throws IOException {
    if(getComplexType() != ComplexDataType.MULTI_VALUE) {
      throw new UnsupportedOperationException();
    }
    return (List<SingleValue>)getValues();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<UnsupportedValue> getUnsupportedValues() throws IOException {
    if(getComplexType() != ComplexDataType.UNSUPPORTED) {
      throw new UnsupportedOperationException();
    }
    return (List<UnsupportedValue>)getValues();
  }

  @Override
  public void reset() {
    // discard any cached values
    _values = null;
  }

  @Override
  public Version addVersion(String value) throws IOException {
    return addVersionImpl(value, now());
  }

  @Override
  public Version addVersion(String value, Date modifiedDate) throws IOException {
    return addVersionImpl(value, modifiedDate);
  }

  @Override
  public Version addVersion(String value, LocalDateTime modifiedDate) throws IOException {
    return addVersionImpl(value, modifiedDate);
  }

  private Version addVersionImpl(String value, Object modifiedDate) throws IOException {
    reset();
    Version v = VersionHistoryColumnInfoImpl.newVersion(this, value, modifiedDate);
    getVersionInfo().addValue(v);
    return v;
  }

  @Override
  public Attachment addAttachment(byte[] data) throws IOException {
    return addAttachmentImpl(null, null, null, data, null, null);
  }

  @Override
  public Attachment addAttachment(
      String url, String name, String type, byte[] data,
      Date timeStamp, Integer flags)
    throws IOException
  {
    return addAttachmentImpl(url, name, type, data, timeStamp, flags);
  }

  @Override
  public Attachment addAttachment(
      String url, String name, String type, byte[] data,
      LocalDateTime timeStamp, Integer flags)
    throws IOException
  {
    return addAttachmentImpl(url, name, type, data, timeStamp, flags);
  }

  private Attachment addAttachmentImpl(
      String url, String name, String type, byte[] data,
      Object timeStamp, Integer flags)
    throws IOException
  {
    reset();
    Attachment a = AttachmentColumnInfoImpl.newAttachment(
        this, url, name, type, data, timeStamp, flags);
    getAttachmentInfo().addValue(a);
    return a;
  }

  @Override
  public Attachment addEncodedAttachment(byte[] encodedData)
    throws IOException
  {
    return addEncodedAttachmentImpl(null, null, null, encodedData, null, null);
  }

  @Override
  public Attachment addEncodedAttachment(
      String url, String name, String type, byte[] encodedData,
      Date timeStamp, Integer flags)
    throws IOException
  {
    return addEncodedAttachmentImpl(url, name, type, encodedData, timeStamp,
                                    flags);
  }

  @Override
  public Attachment addEncodedAttachment(
      String url, String name, String type, byte[] encodedData,
      LocalDateTime timeStamp, Integer flags)
    throws IOException
  {
    return addEncodedAttachmentImpl(url, name, type, encodedData, timeStamp,
                                    flags);
  }

  private Attachment addEncodedAttachmentImpl(
      String url, String name, String type, byte[] encodedData,
      Object timeStamp, Integer flags)
    throws IOException
  {
    reset();
    Attachment a = AttachmentColumnInfoImpl.newEncodedAttachment(
        this, url, name, type, encodedData, timeStamp, flags);
    getAttachmentInfo().addValue(a);
    return a;
  }

  @Override
  public Attachment updateAttachment(Attachment attachment) throws IOException {
    reset();
    getAttachmentInfo().updateValue(attachment);
    return attachment;
  }

  @Override
  public Attachment deleteAttachment(Attachment attachment) throws IOException {
    reset();
    getAttachmentInfo().deleteValue(attachment);
    return attachment;
  }

  @Override
  public SingleValue addMultiValue(Object value) throws IOException {
    reset();
    SingleValue v = MultiValueColumnInfoImpl.newSingleValue(this, value);
    getMultiValueInfo().addValue(v);
    return v;
  }

  @Override
  public SingleValue updateMultiValue(SingleValue value) throws IOException {
    reset();
    getMultiValueInfo().updateValue(value);
    return value;
  }

  @Override
  public SingleValue deleteMultiValue(SingleValue value) throws IOException {
    reset();
    getMultiValueInfo().deleteValue(value);
    return value;
  }

  @Override
  public UnsupportedValue addUnsupportedValue(Map<String,?> values)
    throws IOException
  {
    reset();
    UnsupportedValue v = UnsupportedColumnInfoImpl.newValue(this, values);
    getUnsupportedInfo().addValue(v);
    return v;
  }

  @Override
  public UnsupportedValue updateUnsupportedValue(UnsupportedValue value)
    throws IOException
  {
    reset();
    getUnsupportedInfo().updateValue(value);
    return value;
  }

  @Override
  public UnsupportedValue deleteUnsupportedValue(UnsupportedValue value)
    throws IOException
  {
    reset();
    getUnsupportedInfo().deleteValue(value);
    return value;
  }

  @Override
  public void deleteAllValues() throws IOException {
    reset();
    getComplexInfo().deleteAllValues(this);
  }

  @Override
  public boolean equals(Object o) {
    return(super.equals(o) &&
           (_column == ((ComplexValueForeignKeyImpl)o)._column));
  }

  private Object now() {
    Database db = getColumn().getDatabase();
    if(db.getDateTimeType() == DateTimeType.DATE) {
      return new Date();
    }
    return LocalDateTime.now(db.getZoneId());
  }
}
