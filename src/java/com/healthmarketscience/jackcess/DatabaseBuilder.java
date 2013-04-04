/*
Copyright (c) 2012 James Ahlborn

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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.TimeZone;

import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.CodecProvider;
import com.healthmarketscience.jackcess.util.MemFileChannel;

/**
 * Builder style class for opening/creating a Database.
 *
 * @author James Ahlborn
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
   * writes will be immediately flushed to disk.  This leaves the database in
   * a (fairly) consistent state on each write, but can be very inefficient
   * for many updates.  if {@code false}, flushing to disk happens at the
   * jvm's leisure, which can be much faster, but may leave the database in an
   * inconsistent state if failures are encountered during writing.  Writes
   * may be flushed at any time using {@link Database#flush}.
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
    return DatabaseImpl.create(_fileFormat, _mdbFile, _channel, _autoSync, 
                               _charset, _timeZone);
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
  

}
