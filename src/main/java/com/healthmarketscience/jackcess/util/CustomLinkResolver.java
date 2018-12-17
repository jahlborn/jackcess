/*
Copyright (c) 2017 James Ahlborn

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

package com.healthmarketscience.jackcess.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.ByteUtil;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;

/**
 * Utility base implementaton of LinkResolver which facilitates loading linked
 * tables from files which are not access databases.  The LinkResolver API
 * ultimately presents linked table information to the primary database using
 * the jackcess {@link Database} and {@link Table} classes.  In order to
 * consume linked tables in non-mdb files, they need to somehow be coerced
 * into the appropriate form.  The approach taken by this utility is to make
 * it easy to copy the external tables into a temporary mdb file for
 * consumption by the primary database.
 * <p>
 * The primary features of this utility:
 * <ul>
 * <li>Supports custom behavior for non-mdb files and default behavior for mdb
 *     files, see {@link #loadCustomFile}</li>
 * <li>Temp db can be an actual file or entirely in memory</li>
 * <li>Linked tables are loaded on-demand, see {@link #loadCustomTable}</li>
 * <li>Temp db files will be automatically deleted on close</li>
 * </ul>
 *
 * @author James Ahlborn
 * @usage _intermediate_class_
 */
public abstract class CustomLinkResolver implements LinkResolver
{
  private static final Random DB_ID = new Random();

  private static final String MEM_DB_PREFIX = "memdb_";
  private static final String FILE_DB_PREFIX = "linkeddb_";

  /** the default file format used for temp dbs */
  public static final FileFormat DEFAULT_FORMAT = FileFormat.V2000;
  /** temp dbs default to the filesystem, not in memory */
  public static final boolean DEFAULT_IN_MEMORY = false;
  /** temp dbs end up in the system temp dir by default */
  public static final Path DEFAULT_TEMP_DIR = null;

  private final FileFormat _defaultFormat;
  private final boolean _defaultInMemory;
  private final Path _defaultTempDir;

  /**
   * Creates a CustomLinkResolver using the default behavior for creating temp
   * dbs, see {@link #DEFAULT_FORMAT}, {@link #DEFAULT_IN_MEMORY} and
   * {@link #DEFAULT_TEMP_DIR}.
   */
  protected CustomLinkResolver() {
    this(DEFAULT_FORMAT, DEFAULT_IN_MEMORY, DEFAULT_TEMP_DIR);
  }

  /**
   * Creates a CustomLinkResolver with the given default behavior for creating
   * temp dbs.
   *
   * @param defaultFormat the default format for the temp db
   * @param defaultInMemory whether or not the temp db should be entirely in
   *                        memory by default (while this will be faster, it
   *                        should only be used if table data is expected to
   *                        fit entirely in memory)
   * @param defaultTempDir the default temp dir for a file based temp db
   *                       ({@code null} for the system defaqult temp
   *                       directory)
   */
  protected CustomLinkResolver(FileFormat defaultFormat, boolean defaultInMemory,
                               Path defaultTempDir)
  {
    _defaultFormat = defaultFormat;
    _defaultInMemory = defaultInMemory;
    _defaultTempDir = defaultTempDir;
  }

  protected FileFormat getDefaultFormat() {
    return _defaultFormat;
  }

  protected boolean isDefaultInMemory() {
    return _defaultInMemory;
  }

  protected Path getDefaultTempDirectory() {
    return _defaultTempDir;
  }

  /**
   * Custom implementation is:
   * <pre>
   *   // attempt to load the linkeeFileName as a custom file
   *   Object customFile = loadCustomFile(linkerDb, linkeeFileName);
   *
   *   if(customFile != null) {
   *     // this is a custom file, create and return relevant temp db
   *     return createTempDb(customFile, getDefaultFormat(), isDefaultInMemory(),
   *                         getDefaultTempDirectory());
   *   }
   *
   *   // not a custmom file, load using the default behavior
   *   return LinkResolver.DEFAULT.resolveLinkedDatabase(linkerDb, linkeeFileName);
   * </pre>
   *
   * @see #loadCustomFile
   * @see #createTempDb
   * @see LinkResolver#DEFAULT
   */
  public Database resolveLinkedDatabase(Database linkerDb, String linkeeFileName)
    throws IOException
  {
    Object customFile = loadCustomFile(linkerDb, linkeeFileName);
    if(customFile != null) {
      // if linker is read-only, open linkee read-only
      boolean readOnly = ((linkerDb instanceof DatabaseImpl) ?
                          ((DatabaseImpl)linkerDb).isReadOnly() : false);
      return createTempDb(customFile, getDefaultFormat(), isDefaultInMemory(),
                          getDefaultTempDirectory(), readOnly);
    }
    return LinkResolver.DEFAULT.resolveLinkedDatabase(linkerDb, linkeeFileName);
  }

  /**
   * Creates a temporary database for holding the table data from
   * linkeeFileName.
   *
   * @param customFile custom file state returned from {@link #loadCustomFile}
   * @param format the access format for the temp db
   * @param inMemory whether or not the temp db should be entirely in memory
   *                 (while this will be faster, it should only be used if
   *                 table data is expected to fit entirely in memory)
   * @param tempDir the temp dir for a file based temp db ({@code null} for
   *                the system default temp directory)
   *
   * @return the temp db for holding the linked table info
   */
  protected Database createTempDb(Object customFile, FileFormat format,
                                  boolean inMemory, Path tempDir,
                                  boolean readOnly)
    throws IOException
  {
    Path dbFile = null;
    FileChannel channel = null;
    boolean success = false;
    try {

      if(inMemory) {
        dbFile = Paths.get(MEM_DB_PREFIX + DB_ID.nextLong() +
                           format.getFileExtension());
        channel = MemFileChannel.newChannel();
      } else {
        dbFile = ((tempDir != null) ?
                  Files.createTempFile(tempDir, FILE_DB_PREFIX,
                                       format.getFileExtension()) :
                  Files.createTempFile(FILE_DB_PREFIX,
                                       format.getFileExtension()));
        channel = FileChannel.open(dbFile, DatabaseImpl.RW_CHANNEL_OPTS);
      }

      TempDatabaseImpl.initDbChannel(channel, format);
      TempDatabaseImpl db = new TempDatabaseImpl(this, customFile, dbFile,
                                                 channel, format, readOnly);
      success = true;
      return db;

    } finally {
      if(!success) {
        ByteUtil.closeQuietly(channel);
        deleteDbFile(dbFile);
        closeCustomFile(customFile);
      }
    }
  }

  private static void deleteDbFile(Path dbFile) {
    if((dbFile != null) &&
       dbFile.getFileName().toString().startsWith(FILE_DB_PREFIX)) {
      try {
        Files.deleteIfExists(dbFile);
      } catch(IOException ignores) {}
    }
  }

  private static void closeCustomFile(Object customFile) {
    if(customFile instanceof Closeable) {
      ByteUtil.closeQuietly((Closeable)customFile);
    }
  }

  /**
   * Called by {@link #resolveLinkedDatabase} to determine whether the
   * linkeeFileName should be treated as a custom file (thus utiliziing a temp
   * db) or a normal access db (loaded via the default behavior).  Loads any
   * state necessary for subsequently loading data from linkeeFileName.
   * <p>
   * The returned custom file state object will be maintained with the temp db
   * and passed to {@link #loadCustomTable} whenever a new table needs to be
   * loaded.  Also, if this object is {@link Closeable}, it will be closed
   * with the temp db.
   *
   * @param linkerDb the primary database in which the link is defined
   * @param linkeeFileName the name of the linked file
   *
   * @return non-{@code null} if linkeeFileName should be treated as a custom
   *         file (using a temp db) or {@code null} if it should be treated as
   *         a normal access db.
   */
  protected abstract Object loadCustomFile(
      Database linkerDb, String linkeeFileName) throws IOException;

  /**
   * Called by an instance of a temp db when a missing table is first requested.
   *
   * @param tempDb the temp db instance which should be populated with the
   *               relevant table info for the given tableName
   * @param customFile custom file state returned from {@link #loadCustomFile}
   * @param tableName the name of the table which is requested from the linked
   *                  file
   *
   * @return {@code true} if the table was available in the linked file,
   *         {@code false} otherwise
   */
  protected abstract boolean loadCustomTable(
      Database tempDb, Object customFile, String tableName)
    throws IOException;


  /**
   * Subclass of DatabaseImpl which allows us to load tables "on demand" as
   * well as delete the temporary db on close.
   */
  private static class TempDatabaseImpl extends DatabaseImpl
  {
    private final CustomLinkResolver _resolver;
    private final Object _customFile;

    protected TempDatabaseImpl(CustomLinkResolver resolver, Object customFile,
                               Path file, FileChannel channel,
                               FileFormat fileFormat, boolean readOnly)
      throws IOException
    {
      super(file, channel, true, false, fileFormat, null, null, null,
            readOnly);
      _resolver = resolver;
      _customFile = customFile;
    }

    @Override
    protected TableImpl getTable(String name, boolean includeSystemTables)
      throws IOException
    {
      TableImpl table = super.getTable(name, includeSystemTables);
      if((table == null) &&
         _resolver.loadCustomTable(this, _customFile, name)) {
        table = super.getTable(name, includeSystemTables);
      }
      return table;
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        deleteDbFile(getPath());
        closeCustomFile(_customFile);
      }
    }

    static FileChannel initDbChannel(FileChannel channel, FileFormat format)
      throws IOException
    {
      FileFormatDetails details = getFileFormatDetails(format);
      transferDbFrom(channel, getResourceAsStream(details.getEmptyFilePath()));
      return channel;
    }
  }

}
