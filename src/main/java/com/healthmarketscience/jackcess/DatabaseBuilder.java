/*
Copyright (c) 2012 James Ahlborn

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

package com.healthmarketscience.jackcess;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.impl.CodecProvider;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.PropertyMapImpl;
import com.healthmarketscience.jackcess.util.MemFileChannel;

/**
 * Builder style class for opening/creating a {@link Database}.
 * <p/>
 * Simple example usage:
 * <pre>
 *   Database db = DatabaseBuilder.open(new File("test.mdb"));
 * </pre>
 * <p/>
 * Advanced example usage:
 * <pre>
 *   Database db = new DatabaseBuilder(new File("test.mdb"))
 *     .setReadOnly(true)
 *     .open();
 * </pre>
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public class DatabaseBuilder 
{
  /** the file name of the mdb to open/create */
  private File _mdbFile;
  /** whether or not to open existing mdb read-only */
  private boolean _readOnly;
  /** whether or not to auto-sync writes to the filesystem */
  private boolean _autoSync = Database.DEFAULT_AUTO_SYNC;
  /** optional charset for mdbs with unspecified charsets */
  private Charset _charset;
  /** optional timezone override for interpreting dates */
  private TimeZone _timeZone;
  /** optional CodecProvider for handling encoded mdbs */
  private CodecProvider _codecProvider;
  /** FileFormat to use when creating a new mdb */
  private Database.FileFormat _fileFormat;
  /** optional pre-opened FileChannel, will _not_ be closed by Database
      close */
  private FileChannel _channel;
  /** database properties (if any) */
  private Map<String,PropertyMap.Property> _dbProps;
  /** database summary properties (if any) */
  private Map<String,PropertyMap.Property> _summaryProps;
  /** database user-defined (if any) */
  private Map<String,PropertyMap.Property> _userProps;

  
  public DatabaseBuilder() {
    this(null);
  }

  public DatabaseBuilder(File mdbFile) {
    _mdbFile = mdbFile;
  }

  /**
   * File containing an existing database for {@link #open} or target file for
   * new database for {@link #create} (in which case, <b>tf this file already
   * exists, it will be overwritten.</b>)
   * @usage _general_method_
   */
  public DatabaseBuilder setFile(File mdbFile) {
    _mdbFile = mdbFile;
    return this;
  }

  /**
   * Sets flag which, iff {@code true}, will force opening file in
   * read-only mode ({@link #open} only).
   * @usage _general_method_
   */
  public DatabaseBuilder setReadOnly(boolean readOnly) {
    _readOnly = readOnly;
    return this;
  }

  /**
   * Sets whether or not to enable auto-syncing on write.  if {@code true},
   * write operations will be immediately flushed to disk upon completion.
   * This leaves the database in a (fairly) consistent state on each write,
   * but can be very inefficient for many updates.  if {@code false}, flushing
   * to disk happens at the jvm's leisure, which can be much faster, but may
   * leave the database in an inconsistent state if failures are encountered
   * during writing.  Writes may be flushed at any time using {@link
   * Database#flush}.
   * @usage _intermediate_method_
   */
  public DatabaseBuilder setAutoSync(boolean autoSync) {
    _autoSync = autoSync;
    return this;
  }

  /**
   * Sets the Charset to use, if {@code null}, uses default.
   * @usage _intermediate_method_
   */
  public DatabaseBuilder setCharset(Charset charset) {
    _charset = charset;
    return this;
  }

  /**
   * Sets the TimeZone to use for interpreting dates, if {@code null}, uses
   * default
   * @usage _intermediate_method_
   */
  public DatabaseBuilder setTimeZone(TimeZone timeZone) {
    _timeZone = timeZone;
    return this;
  }

  /**
   * Sets the CodecProvider for handling page encoding/decoding, may be
   * {@code null} if no special encoding is necessary
   * @usage _intermediate_method_
   */
  public DatabaseBuilder setCodecProvider(CodecProvider codecProvider) {
    _codecProvider = codecProvider;
    return this;
  }

  /**
   * Sets the version of new database ({@link #create} only).
   * @usage _general_method_
   */
  public DatabaseBuilder setFileFormat(Database.FileFormat fileFormat) {
    _fileFormat = fileFormat;
    return this;
  }

  /**
   * Sets a pre-opened FileChannel.  if provided explicitly, <i>it will not be
   * closed by the Database instance</i>.  This allows ultimate control of
   * where the mdb file exists (which may not be on disk, e.g.
   * {@link MemFileChannel}).  If provided, the File parameter will be
   * available from {@link Database#getFile}, but otherwise ignored.
   * @usage _advanced_method_
   */
  public DatabaseBuilder setChannel(FileChannel channel) {
    _channel = channel;
    return this;
  }

  /**
   * Sets the database property with the given name to the given value.
   * Attempts to determine the type of the property (see
   * {@link PropertyMap#put(String,Object)} for details on determining the
   * property type).
   */
  public DatabaseBuilder putDatabaseProperty(String name, Object value) {
    return putDatabaseProperty(name, null, value);
  }
  
  /**
   * Sets the database property with the given name and type to the given
   * value.
   */
  public DatabaseBuilder putDatabaseProperty(String name, DataType type,
                                             Object value) {
    _dbProps = putProperty(_dbProps, name, type, value);
    return this;
  }
  
  /**
   * Sets the summary database property with the given name to the given
   * value.  Attempts to determine the type of the property (see
   * {@link PropertyMap#put(String,Object)} for details on determining the
   * property type).
   */
  public DatabaseBuilder putSummaryProperty(String name, Object value) {
    return putSummaryProperty(name, null, value);
  }
  
  /**
   * Sets the summary database property with the given name and type to
   * the given value.
   */
  public DatabaseBuilder putSummaryProperty(String name, DataType type,
                                            Object value) {
    _summaryProps = putProperty(_summaryProps, name, type, value);
    return this;
  }

  /**
   * Sets the user-defined database property with the given name to the given
   * value.  Attempts to determine the type of the property (see
   * {@link PropertyMap#put(String,Object)} for details on determining the
   * property type).
   */
  public DatabaseBuilder putUserDefinedProperty(String name, Object value) {
    return putUserDefinedProperty(name, null, value);
  }
  
  /**
   * Sets the user-defined database property with the given name and type to
   * the given value.
   */
  public DatabaseBuilder putUserDefinedProperty(String name, DataType type,
                                                Object value) {
    _userProps = putProperty(_userProps, name, type, value);
    return this;
  }

  private static Map<String,PropertyMap.Property> putProperty(
      Map<String,PropertyMap.Property> props, String name, DataType type,
      Object value)
  {
    if(props == null) {
      props = new HashMap<String,PropertyMap.Property>();
    }
    props.put(name, PropertyMapImpl.createProperty(name, type, value));
    return props;
  }

  /**
   * Opens an existingnew Database using the configured information.
   */
  public Database open() throws IOException {
    return DatabaseImpl.open(_mdbFile, _readOnly, _channel, _autoSync, _charset,
                             _timeZone, _codecProvider);
  }

  /**
   * Creates a new Database using the configured information.
   */
  public Database create() throws IOException {
    if (_fileFormat == FileFormat.GENERIC_JET4 && _userProps != null) {
      throw new UnsupportedOperationException(
          "Cannot create a GENERIC_JET4 file with user-defined properties");      
    }
    
    Database db = DatabaseImpl.create(_fileFormat, _mdbFile, _channel, _autoSync, 
                                      _charset, _timeZone);
    if(_dbProps != null) {
      PropertyMap props = db.getDatabaseProperties();
      props.putAll(_dbProps.values());
      props.save();
    }
    if(_summaryProps != null) {
      PropertyMap props = db.getSummaryProperties();
      props.putAll(_summaryProps.values());
      props.save();
    }
    if(_userProps != null) {
      PropertyMap props = db.getUserDefinedProperties();
      props.putAll(_userProps.values());
      props.save();
    }
    return db;
  }

  /**
   * Open an existing Database.  If the existing file is not writeable, the
   * file will be opened read-only.  Auto-syncing is enabled for the returned
   * Database.
   * 
   * @param mdbFile File containing the database
   * 
   * @see DatabaseBuilder for more flexible Database opening
   * @usage _general_method_
   */
  public static Database open(File mdbFile) throws IOException {
    return new DatabaseBuilder(mdbFile).open();
  }
  
  /**
   * Create a new Database for the given fileFormat
   * 
   * @param fileFormat version of new database.
   * @param mdbFile Location to write the new database to.  <b>If this file
   *    already exists, it will be overwritten.</b>
   *
   * @see DatabaseBuilder for more flexible Database creation
   * @usage _general_method_
   */
  public static Database create(Database.FileFormat fileFormat, File mdbFile) 
    throws IOException 
  {
    return new DatabaseBuilder(mdbFile).setFileFormat(fileFormat).create();
  }

  /**
   * Returns a SimpleDateFormat for the given format string which is
   * configured with a compatible Calendar instance (see
   * {@link #toCompatibleCalendar}).
   */
  public static SimpleDateFormat createDateFormat(String formatStr) {
    SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
    toCompatibleCalendar(sdf.getCalendar());
    return sdf;
  }

  /**
   * Ensures that the given {@link Calendar} is configured to be compatible
   * with how Access handles dates.  Specifically, alters the gregorian change
   * (the java default gregorian change switches to julian dates for dates pre
   * 1582-10-15, whereas Access uses <a href="https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar">proleptic gregorian dates</a>).
   */
  public static Calendar toCompatibleCalendar(Calendar cal) {
    if(cal instanceof GregorianCalendar) {
      ((GregorianCalendar)cal).setGregorianChange(new Date(Long.MIN_VALUE));
    }
    return cal;
  }
}
