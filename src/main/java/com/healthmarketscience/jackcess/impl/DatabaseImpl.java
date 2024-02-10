/*
Copyright (c) 2005 Health Market Science, Inc.

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.expr.EvalConfig;
import com.healthmarketscience.jackcess.impl.query.QueryImpl;
import com.healthmarketscience.jackcess.query.Query;
import com.healthmarketscience.jackcess.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 *
 * @author Tim McCune
 * @usage _intermediate_class_
 */
public class DatabaseImpl implements Database, DateTimeContext
{
  private static final Log LOG = LogFactory.getLog(DatabaseImpl.class);

  /** this is the default "userId" used if we cannot find existing info.  this
      seems to be some standard "Admin" userId for access files */
  private static final byte[] SYS_DEFAULT_SID = new byte[] {
    (byte) 0xA6, (byte) 0x33};

  /** the default value for the resource path used to load classpath
   *  resources.
   */
  public static final String DEFAULT_RESOURCE_PATH =
    "com/healthmarketscience/jackcess/";

  /** the resource path to be used when loading classpath resources */
  static final String RESOURCE_PATH =
    System.getProperty(RESOURCE_PATH_PROPERTY, DEFAULT_RESOURCE_PATH);

  /** whether or not this jvm has "broken" nio support */
  static final boolean BROKEN_NIO = Boolean.TRUE.toString().equalsIgnoreCase(
      System.getProperty(BROKEN_NIO_PROPERTY));

  /** additional internal details about each FileFormat */
  private static final Map<Database.FileFormat,FileFormatDetails> FILE_FORMAT_DETAILS =
    new EnumMap<Database.FileFormat,FileFormatDetails>(Database.FileFormat.class);

  static {
    addFileFormatDetails(FileFormat.V1997, null, JetFormat.VERSION_3);
    addFileFormatDetails(FileFormat.GENERIC_JET4, null, JetFormat.VERSION_4);
    addFileFormatDetails(FileFormat.V2000, "empty", JetFormat.VERSION_4);
    addFileFormatDetails(FileFormat.V2003, "empty2003", JetFormat.VERSION_4);
    addFileFormatDetails(FileFormat.V2007, "empty2007", JetFormat.VERSION_12);
    addFileFormatDetails(FileFormat.V2010, "empty2010", JetFormat.VERSION_14);
    addFileFormatDetails(FileFormat.V2016, "empty2016", JetFormat.VERSION_16);
    addFileFormatDetails(FileFormat.V2019, "empty2019", JetFormat.VERSION_17);
    addFileFormatDetails(FileFormat.MSISAM, null, JetFormat.VERSION_MSISAM);
  }

  /** System catalog always lives on page 2 */
  private static final int PAGE_SYSTEM_CATALOG = 2;
  /** Name of the system catalog */
  private static final String TABLE_SYSTEM_CATALOG = "MSysObjects";

  /** this is the access control bit field for created tables.  the value used
      is equivalent to full access (Visual Basic DAO PermissionEnum constant:
      dbSecFullAccess) */
  private static final Integer SYS_FULL_ACCESS_ACM = 1048575;

  /** ACE table column name of the actual access control entry */
  private static final String ACE_COL_ACM = "ACM";
  /** ACE table column name of the inheritable attributes flag */
  private static final String ACE_COL_F_INHERITABLE = "FInheritable";
  /** ACE table column name of the relevant objectId */
  private static final String ACE_COL_OBJECT_ID = "ObjectId";
  /** ACE table column name of the relevant userId */
  private static final String ACE_COL_SID = "SID";

  /** Relationship table column name of the column count */
  private static final String REL_COL_COLUMN_COUNT = "ccolumn";
  /** Relationship table column name of the flags */
  private static final String REL_COL_FLAGS = "grbit";
  /** Relationship table column name of the index of the columns */
  private static final String REL_COL_COLUMN_INDEX = "icolumn";
  /** Relationship table column name of the "to" column name */
  private static final String REL_COL_TO_COLUMN = "szColumn";
  /** Relationship table column name of the "to" table name */
  private static final String REL_COL_TO_TABLE = "szObject";
  /** Relationship table column name of the "from" column name */
  private static final String REL_COL_FROM_COLUMN = "szReferencedColumn";
  /** Relationship table column name of the "from" table name */
  private static final String REL_COL_FROM_TABLE = "szReferencedObject";
  /** Relationship table column name of the relationship */
  private static final String REL_COL_NAME = "szRelationship";

  /** System catalog column name of the page on which system object definitions
      are stored */
  private static final String CAT_COL_ID = "Id";
  /** System catalog column name of the name of a system object */
  private static final String CAT_COL_NAME = "Name";
  private static final String CAT_COL_OWNER = "Owner";
  /** System catalog column name of a system object's parent's id */
  private static final String CAT_COL_PARENT_ID = "ParentId";
  /** System catalog column name of the type of a system object */
  private static final String CAT_COL_TYPE = "Type";
  /** System catalog column name of the date a system object was created */
  private static final String CAT_COL_DATE_CREATE = "DateCreate";
  /** System catalog column name of the date a system object was updated */
  private static final String CAT_COL_DATE_UPDATE = "DateUpdate";
  /** System catalog column name of the flags column */
  private static final String CAT_COL_FLAGS = "Flags";
  /** System catalog column name of the properties column */
  static final String CAT_COL_PROPS = "LvProp";
  /** System catalog column name of the remote database */
  private static final String CAT_COL_DATABASE = "Database";
  /** System catalog column name of the remote table name */
  private static final String CAT_COL_FOREIGN_NAME = "ForeignName";
  /** System catalog column name of the remote connection name */
  private static final String CAT_COL_CONNECT_NAME = "Connect";

  /** top-level parentid for a database */
  private static final int DB_PARENT_ID = 0xF000000;

  /** the maximum size of any of the included "empty db" resources */
  private static final long MAX_EMPTYDB_SIZE = 440000L;

  /** this object is a "system" object */
  static final int SYSTEM_OBJECT_FLAG = 0x80000000;
  /** this object is another type of "system" object */
  static final int ALT_SYSTEM_OBJECT_FLAG = 0x02;
  /** this object is hidden */
  public static final int HIDDEN_OBJECT_FLAG = 0x08;
  /** all flags which seem to indicate some type of system object */
  static final int SYSTEM_OBJECT_FLAGS =
    SYSTEM_OBJECT_FLAG | ALT_SYSTEM_OBJECT_FLAG;

  /** read-only channel access mode */
  public static final OpenOption[] RO_CHANNEL_OPTS =
    {StandardOpenOption.READ};
  /** read/write channel access mode for existing files */
  public static final OpenOption[] RW_CHANNEL_OPTS =
    {StandardOpenOption.READ, StandardOpenOption.WRITE};
  /** read/write/create channel access mode for new files */
  public static final OpenOption[] RWC_CHANNEL_OPTS =
  {StandardOpenOption.READ, StandardOpenOption.WRITE,
   StandardOpenOption.CREATE};

  /** Name of the system object that is the parent of all tables */
  private static final String SYSTEM_OBJECT_NAME_TABLES = "Tables";
  /** Name of the system object that is the parent of all databases */
  private static final String SYSTEM_OBJECT_NAME_DATABASES = "Databases";
  /** Name of the system object that is the parent of all relationships */
  private static final String SYSTEM_OBJECT_NAME_RELATIONSHIPS = "Relationships";
  /** Name of the table that contains system access control entries */
  private static final String TABLE_SYSTEM_ACES = "MSysACEs";
  /** Name of the table that contains table relationships */
  private static final String TABLE_SYSTEM_RELATIONSHIPS = "MSysRelationships";
  /** Name of the table that contains queries */
  private static final String TABLE_SYSTEM_QUERIES = "MSysQueries";
  /** Name of the table that contains complex type information */
  private static final String TABLE_SYSTEM_COMPLEX_COLS = "MSysComplexColumns";
  /** Name of the main database properties object */
  private static final String OBJECT_NAME_DB_PROPS = "MSysDb";
  /** Name of the summary properties object */
  private static final String OBJECT_NAME_SUMMARY_PROPS = "SummaryInfo";
  /** Name of the user-defined properties object */
  private static final String OBJECT_NAME_USERDEF_PROPS = "UserDefined";
  /** System object type for table definitions */
  static final Short TYPE_TABLE = 1;
  /** System object type for linked odbc tables */
  private static final Short TYPE_LINKED_ODBC_TABLE = 4;
  /** System object type for query definitions */
  private static final Short TYPE_QUERY = 5;
  /** System object type for linked table definitions */
  private static final Short TYPE_LINKED_TABLE = 6;
  /** System object type for relationships */
  private static final Short TYPE_RELATIONSHIP = 8;

  /** max number of table lookups to cache */
  private static final int MAX_CACHED_LOOKUP_TABLES = 50;

  /** the columns to read when reading system catalog normally */
  private static Collection<String> SYSTEM_CATALOG_COLUMNS =
    new HashSet<String>(Arrays.asList(CAT_COL_NAME, CAT_COL_TYPE, CAT_COL_ID,
                                      CAT_COL_FLAGS, CAT_COL_PARENT_ID));
  /** the columns to read when finding table details */
  private static Collection<String> SYSTEM_CATALOG_TABLE_DETAIL_COLUMNS =
    new HashSet<String>(Arrays.asList(CAT_COL_NAME, CAT_COL_TYPE, CAT_COL_ID,
                                      CAT_COL_FLAGS, CAT_COL_PARENT_ID,
                                      CAT_COL_DATABASE, CAT_COL_FOREIGN_NAME,
                                      CAT_COL_CONNECT_NAME));
  /** the columns to read when getting object propertyes */
  private static Collection<String> SYSTEM_CATALOG_PROPS_COLUMNS =
    new HashSet<String>(Arrays.asList(CAT_COL_ID, CAT_COL_PROPS));
  /** the columns to read when grabbing dates */
  private static Collection<String> SYSTEM_CATALOG_DATE_COLUMNS =
    new HashSet<String>(Arrays.asList(CAT_COL_ID,
                                      CAT_COL_DATE_CREATE, CAT_COL_DATE_UPDATE));

  /** regex matching characters which are invalid in identifier names */
  private static final Pattern INVALID_IDENTIFIER_CHARS =
    Pattern.compile("[\\p{Cntrl}.!`\\]\\[]");

  /** regex to match a password in an ODBC string */
  private static final Pattern ODBC_PWD_PATTERN = Pattern.compile("\\bPWD=[^;]+");

  /** the File of the database */
  private final Path _file;
  /** the simple name of the database */
  private final String _name;
  /** whether or not this db is read-only */
  private final boolean _readOnly;
  /** Buffer to hold database pages */
  private ByteBuffer _buffer;
  /** ID of the Tables system object */
  private Integer _tableParentId;
  /** Format that the containing database is in */
  private final JetFormat _format;
  /**
   * Cache map of UPPERCASE table names to page numbers containing their
   * definition and their stored table name (max size
   * MAX_CACHED_LOOKUP_TABLES).
   */
  private final Map<String, TableInfo> _tableLookup =
    new SimpleCache<String,TableInfo>(MAX_CACHED_LOOKUP_TABLES);
  /** set of table names as stored in the mdb file, created on demand */
  private Set<String> _tableNames;
  /** Reads and writes database pages */
  private final PageChannel _pageChannel;
  /** System catalog table */
  private TableImpl _systemCatalog;
  /** utility table finder */
  private TableFinder _tableFinder;
  /** System access control entries table (initialized on first use) */
  private TableImpl _accessControlEntries;
  /** ID of the Relationships system object */
  private Integer _relParentId;
  /** SIDs to use for the ACEs added for new relationships */
  private final List<byte[]> _newRelSIDs = new ArrayList<byte[]>();
  /** System relationships table (initialized on first use) */
  private TableImpl _relationships;
  /** System queries table (initialized on first use) */
  private TableImpl _queries;
  /** System complex columns table (initialized on first use) */
  private TableImpl _complexCols;
  /** SIDs to use for the ACEs added for new tables */
  private final List<byte[]> _newTableSIDs = new ArrayList<byte[]>();
  /** optional error handler to use when row errors are encountered */
  private ErrorHandler _dbErrorHandler;
  /** the file format of the database */
  private FileFormat _fileFormat;
  /** charset to use when handling text */
  private Charset _charset;
  /** timezone to use when handling dates */
  private TimeZone _timeZone;
  /** zoneId to use when handling dates */
  private ZoneId _zoneId;
  /** language sort order to be used for textual columns */
  private ColumnImpl.SortOrder _defaultSortOrder;
  /** default code page to be used for textual columns (in some dbs) */
  private Short _defaultCodePage;
  /** the ordering used for table columns */
  private Table.ColumnOrder _columnOrder;
  /** whether or not enforcement of foreign-keys is enabled */
  private boolean _enforceForeignKeys;
  /** whether or not auto numbers can be directly inserted by the user */
  private boolean _allowAutoNumInsert;
  /** whether or not to evaluate expressions */
  private boolean _evaluateExpressions;
  /** factory for ColumnValidators */
  private ColumnValidatorFactory _validatorFactory = SimpleColumnValidatorFactory.INSTANCE;
  /** cache of in-use tables (or table definitions) */
  private final TableCache _tableCache = new TableCache();
  /** handler for reading/writing properteies */
  private PropertyMaps.Handler _propsHandler;
  /** ID of the Databases system object */
  private Integer _dbParentId;
  /** owner of objects we create */
  private byte[] _newObjOwner;
  /** core database properties */
  private PropertyMaps _dbPropMaps;
  /** summary properties */
  private PropertyMaps _summaryPropMaps;
  /** user-defined properties */
  private PropertyMaps _userDefPropMaps;
  /** linked table resolver */
  private LinkResolver _linkResolver;
  /** any linked databases which have been opened */
  private Map<String,Database> _linkedDbs;
  /** shared state used when enforcing foreign keys */
  private final FKEnforcer.SharedState _fkEnforcerSharedState =
    FKEnforcer.initSharedState();
  /** shared context for evaluating expressions */
  private DBEvalContext _evalCtx;
  /** factory for the appropriate date/time type */
  private ColumnImpl.DateTimeFactory _dtf;

  /**
   * Open an existing Database.  If the existing file is not writeable or the
   * readOnly flag is {@code true}, the file will be opened read-only.
   * @param mdbFile File containing the database
   * @param readOnly iff {@code true}, force opening file in read-only
   *                 mode
   * @param channel  pre-opened FileChannel.  if provided explicitly, it will
   *                 not be closed by this Database instance
   * @param autoSync whether or not to enable auto-syncing on write.  if
   *                 {@code true}, writes will be immediately flushed to disk.
   *                 This leaves the database in a (fairly) consistent state
   *                 on each write, but can be very inefficient for many
   *                 updates.  if {@code false}, flushing to disk happens at
   *                 the jvm's leisure, which can be much faster, but may
   *                 leave the database in an inconsistent state if failures
   *                 are encountered during writing.  Writes may be flushed at
   *                 any time using {@link #flush}.
   * @param charset  Charset to use, if {@code null}, uses default
   * @param timeZone TimeZone to use, if {@code null}, uses default
   * @param provider CodecProvider for handling page encoding/decoding, may be
   *                 {@code null} if no special encoding is necessary
   * @usage _advanced_method_
   */
  public static DatabaseImpl open(
      Path mdbFile, boolean readOnly, FileChannel channel,
      boolean autoSync, Charset charset, TimeZone timeZone,
      CodecProvider provider, boolean ignoreSystemCatalogIndex)
    throws IOException
  {
    boolean closeChannel = false;
    if(channel == null) {
      if(!Files.isReadable(mdbFile)) {
        throw new FileNotFoundException("given file does not exist: " +
                                        mdbFile);
      }

      // force read-only for non-writable files
      readOnly |= !Files.isWritable(mdbFile);

      // open file channel
      channel = openChannel(mdbFile, readOnly, false);
      closeChannel = true;
    }

    boolean success = false;
    try {

      boolean wrapChannelRO = false;
      if(!readOnly) {
        // verify that format supports writing
        JetFormat jetFormat = JetFormat.getFormat(channel);

        if(jetFormat.READ_ONLY) {
          // force read-only mode
          wrapChannelRO = true;
          readOnly = true;
        }
      } else if(!closeChannel) {
        // we are in read-only mode but the channel was opened externally, so
        // we don't know if it is enforcing read-only status.  wrap it just to
        // be safe
        wrapChannelRO = true;
      }

      if(wrapChannelRO) {
        // wrap the channel with a read-only version to enforce
        // non-writability
        channel = new ReadOnlyFileChannel(channel);
      }

      DatabaseImpl db = new DatabaseImpl(mdbFile, channel, closeChannel, autoSync,
                                         null, charset, timeZone, provider,
                                         readOnly, ignoreSystemCatalogIndex);
      success = true;
      return db;

    } finally {
      if(!success && closeChannel) {
        // something blew up, shutdown the channel (quietly)
        ByteUtil.closeQuietly(channel);
      }
    }
  }

  /**
   * Create a new Database for the given fileFormat
   * @param fileFormat version of new database.
   * @param mdbFile Location to write the new database to.  <b>If this file
   *                already exists, it will be overwritten.</b>
   * @param channel  pre-opened FileChannel.  if provided explicitly, it will
   *                 not be closed by this Database instance
   * @param autoSync whether or not to enable auto-syncing on write.  if
   *                 {@code true}, writes will be immediately flushed to disk.
   *                 This leaves the database in a (fairly) consistent state
   *                 on each write, but can be very inefficient for many
   *                 updates.  if {@code false}, flushing to disk happens at
   *                 the jvm's leisure, which can be much faster, but may
   *                 leave the database in an inconsistent state if failures
   *                 are encountered during writing.  Writes may be flushed at
   *                 any time using {@link #flush}.
   * @param charset  Charset to use, if {@code null}, uses default
   * @param timeZone TimeZone to use, if {@code null}, uses default
   * @usage _advanced_method_
   */
  public static DatabaseImpl create(FileFormat fileFormat, Path mdbFile,
                                    FileChannel channel, boolean autoSync,
                                    Charset charset, TimeZone timeZone)
    throws IOException
  {
    FileFormatDetails details = getFileFormatDetails(fileFormat);
    if (details.getFormat().READ_ONLY) {
      throw new IOException("File format " + fileFormat +
                            " does not support writing for " + mdbFile);
    }
    if(details.getEmptyFilePath() == null) {
      throw new IOException("File format " + fileFormat +
                            " does not support file creation for " + mdbFile);
    }

    boolean closeChannel = false;
    if(channel == null) {
      channel = openChannel(mdbFile, false, true);
      closeChannel = true;
    }

    boolean success = false;
    try {
      channel.truncate(0);
      transferDbFrom(channel, getResourceAsStream(details.getEmptyFilePath()));
      channel.force(true);
      DatabaseImpl db = new DatabaseImpl(mdbFile, channel, closeChannel, autoSync,
                                         fileFormat, charset, timeZone, null,
                                         false, false);
      success = true;
      return db;
    } finally {
      if(!success && closeChannel) {
        // something blew up, shutdown the channel (quietly)
        ByteUtil.closeQuietly(channel);
      }
    }
  }

  /**
   * Package visible only to support unit tests via DatabaseTest.openChannel().
   * @param mdbFile file to open
   * @param readOnly true if read-only
   * @return a FileChannel on the given file.
   * @exception FileNotFoundException
   *            if the mode is <tt>"r"</tt> but the given file object does
   *            not denote an existing regular file, or if the mode begins
   *            with <tt>"rw"</tt> but the given file object does not denote
   *            an existing, writable regular file and a new regular file of
   *            that name cannot be created, or if some other error occurs
   *            while opening or creating the file
   */
  static FileChannel openChannel(
      Path mdbFile, boolean readOnly, boolean create)
    throws IOException
  {
    OpenOption[] opts = (readOnly ? RO_CHANNEL_OPTS :
                         (create ? RWC_CHANNEL_OPTS : RW_CHANNEL_OPTS));
    return FileChannel.open(mdbFile, opts);
  }

  /**
   * Create a new database by reading it in from a FileChannel.
   * @param file the File to which the channel is connected
   * @param channel File channel of the database.  This needs to be a
   *    FileChannel instead of a ReadableByteChannel because we need to
   *    randomly jump around to various points in the file.
   * @param autoSync whether or not to enable auto-syncing on write.  if
   *                 {@code true}, writes will be immediately flushed to disk.
   *                 This leaves the database in a (fairly) consistent state
   *                 on each write, but can be very inefficient for many
   *                 updates.  if {@code false}, flushing to disk happens at
   *                 the jvm's leisure, which can be much faster, but may
   *                 leave the database in an inconsistent state if failures
   *                 are encountered during writing.  Writes may be flushed at
   *                 any time using {@link #flush}.
   * @param fileFormat version of new database (if known)
   * @param charset Charset to use, if {@code null}, uses default
   * @param timeZone TimeZone to use, if {@code null}, uses default
   */
  protected DatabaseImpl(Path file, FileChannel channel, boolean closeChannel,
                         boolean autoSync, FileFormat fileFormat, Charset charset,
                         TimeZone timeZone, CodecProvider provider,
                         boolean readOnly, boolean ignoreSystemCatalogIndex)
    throws IOException
  {
    _file = file;
    _name = getName(file);
    _readOnly = readOnly;
    _format = JetFormat.getFormat(channel);
    _charset = ((charset == null) ? getDefaultCharset(_format) : charset);
    _columnOrder = getDefaultColumnOrder();
    _enforceForeignKeys = getDefaultEnforceForeignKeys();
    _allowAutoNumInsert = getDefaultAllowAutoNumberInsert();
    _evaluateExpressions = getDefaultEvaluateExpressions();
    _fileFormat = fileFormat;
    setZoneInfo(timeZone, null);
    _dtf = ColumnImpl.getDateTimeFactory(getDefaultDateTimeType());
    _pageChannel = new PageChannel(channel, closeChannel, _format, autoSync);
    if(provider == null) {
      provider = DefaultCodecProvider.INSTANCE;
    }
    // note, it's slighly sketchy to pass ourselves along partially
    // constructed, but only our _format and _pageChannel refs should be
    // needed
    _pageChannel.initialize(this, provider);
    _buffer = _pageChannel.createPageBuffer();
    readSystemCatalog(ignoreSystemCatalogIndex);
  }

  @Override
  public File getFile() {
    return ((_file != null) ? _file.toFile() : null);
  }

  @Override
  public Path getPath() {
    return _file;
  }

  public String getName() {
    return _name;
  }

  public boolean isReadOnly() {
    return _readOnly;
  }

  /**
   * @usage _advanced_method_
   */
  public PageChannel getPageChannel() {
    return _pageChannel;
  }

  /**
   * @usage _advanced_method_
   */
  public JetFormat getFormat() {
    return _format;
  }

  /**
   * @return The system catalog table
   * @usage _advanced_method_
   */
  public TableImpl getSystemCatalog() {
    return _systemCatalog;
  }

  /**
   * @return The system Access Control Entries table (loaded on demand)
   * @usage _advanced_method_
   */
  public TableImpl getAccessControlEntries() throws IOException {
    if(_accessControlEntries == null) {
      _accessControlEntries = getRequiredSystemTable(TABLE_SYSTEM_ACES);
    }
    return _accessControlEntries;
  }

  /**
   * @return the complex column system table (loaded on demand)
   * @usage _advanced_method_
   */
  public TableImpl getSystemComplexColumns() throws IOException {
    if(_complexCols == null) {
      _complexCols = getRequiredSystemTable(TABLE_SYSTEM_COMPLEX_COLS);
    }
    return _complexCols;
  }

  @Override
  public ErrorHandler getErrorHandler() {
    return((_dbErrorHandler != null) ? _dbErrorHandler : ErrorHandler.DEFAULT);
  }

  @Override
  public void setErrorHandler(ErrorHandler newErrorHandler) {
    _dbErrorHandler = newErrorHandler;
  }

  @Override
  public LinkResolver getLinkResolver() {
    return((_linkResolver != null) ? _linkResolver : LinkResolver.DEFAULT);
  }

  @Override
  public void setLinkResolver(LinkResolver newLinkResolver) {
    _linkResolver = newLinkResolver;
  }

  @Override
  public Map<String,Database> getLinkedDatabases() {
    return ((_linkedDbs == null) ? Collections.<String,Database>emptyMap() :
            Collections.unmodifiableMap(_linkedDbs));
  }

  @Override
  public boolean isLinkedTable(Table table) throws IOException {

    if((table == null) || (this == table.getDatabase())) {
      // if the table is null or this db owns the table, not linked
      return false;
    }

    // common case, local table name == remote table name
    TableInfo tableInfo = lookupTable(table.getName());
    if((tableInfo != null) &&
       (tableInfo.getType() == TableMetaData.Type.LINKED) &&
       matchesLinkedTable(table, tableInfo.getLinkedTableName(),
                          tableInfo.getLinkedDbName())) {
      return true;
    }

    // but, the local table name may not match the remote table name, so we
    // need to do a search if the common case fails
    return _tableFinder.isLinkedTable(table);
  }

  private boolean matchesLinkedTable(Table table, String linkedTableName,
                                     String linkedDbName) {
    return (table.getName().equalsIgnoreCase(linkedTableName) &&
            (_linkedDbs != null) &&
            (_linkedDbs.get(linkedDbName) == table.getDatabase()));
  }

  @Override
  public TimeZone getTimeZone() {
    return _timeZone;
  }

  @Override
  public void setTimeZone(TimeZone newTimeZone) {
    setZoneInfo(newTimeZone, null);
  }

  @Override
  public ZoneId getZoneId() {
    return _zoneId;
  }

  @Override
  public void setZoneId(ZoneId newZoneId) {
    setZoneInfo(null, newZoneId);
  }

  private void setZoneInfo(TimeZone newTimeZone, ZoneId newZoneId) {
    if(newTimeZone != null) {
      newZoneId = newTimeZone.toZoneId();
    } else if(newZoneId != null) {
      newTimeZone = TimeZone.getTimeZone(newZoneId);
    } else {
      newTimeZone = getDefaultTimeZone();
      newZoneId = newTimeZone.toZoneId();
    }

    _timeZone = newTimeZone;
    _zoneId = newZoneId;
  }

  @Override
  public DateTimeType getDateTimeType() {
    return _dtf.getType();
  }

  @Override
  public void setDateTimeType(DateTimeType dateTimeType) {
    _dtf = ColumnImpl.getDateTimeFactory(dateTimeType);
  }

  @Override
  public ColumnImpl.DateTimeFactory getDateTimeFactory() {
    return _dtf;
  }

  @Override
  public Charset getCharset()
  {
    return _charset;
  }

  @Override
  public void setCharset(Charset newCharset) {
    if(newCharset == null) {
      newCharset = getDefaultCharset(getFormat());
    }
    _charset = newCharset;
  }

  @Override
  public Table.ColumnOrder getColumnOrder() {
    return _columnOrder;
  }

  @Override
  public void setColumnOrder(Table.ColumnOrder newColumnOrder) {
    if(newColumnOrder == null) {
      newColumnOrder = getDefaultColumnOrder();
    }
    _columnOrder = newColumnOrder;
  }

  @Override
  public boolean isEnforceForeignKeys() {
    return _enforceForeignKeys;
  }

  @Override
  public void setEnforceForeignKeys(Boolean newEnforceForeignKeys) {
    if(newEnforceForeignKeys == null) {
      newEnforceForeignKeys = getDefaultEnforceForeignKeys();
    }
    _enforceForeignKeys = newEnforceForeignKeys;
  }

  @Override
  public boolean isAllowAutoNumberInsert() {
    return _allowAutoNumInsert;
  }

  @Override
  public void setAllowAutoNumberInsert(Boolean allowAutoNumInsert) {
    if(allowAutoNumInsert == null) {
      allowAutoNumInsert = getDefaultAllowAutoNumberInsert();
    }
    _allowAutoNumInsert = allowAutoNumInsert;
  }

  @Override
  public boolean isEvaluateExpressions() {
    return _evaluateExpressions;
  }

  @Override
  public void setEvaluateExpressions(Boolean evaluateExpressions) {
    if(evaluateExpressions == null) {
      evaluateExpressions = getDefaultEvaluateExpressions();
    }
    _evaluateExpressions = evaluateExpressions;
  }

  @Override
  public ColumnValidatorFactory getColumnValidatorFactory() {
    return _validatorFactory;
  }

  @Override
  public void setColumnValidatorFactory(ColumnValidatorFactory newFactory) {
    if(newFactory == null) {
      newFactory = SimpleColumnValidatorFactory.INSTANCE;
    }
    _validatorFactory = newFactory;
  }

  /**
   * @usage _advanced_method_
   */
  FKEnforcer.SharedState getFKEnforcerSharedState() {
    return _fkEnforcerSharedState;
  }

  @Override
  public EvalConfig getEvalConfig() {
    return getEvalContext();
  }

  /**
   * @usage _advanced_method_
   */
  DBEvalContext getEvalContext() {
    if(_evalCtx == null) {
      _evalCtx = new DBEvalContext(this);
    }
    return _evalCtx;
  }

  /**
   * Returns a SimpleDateFormat for the given format string which is
   * configured with a compatible Calendar instance (see
   * {@link DatabaseBuilder#toCompatibleCalendar}) and this database's
   * {@link TimeZone}.
   */
  public SimpleDateFormat createDateFormat(String formatStr) {
    SimpleDateFormat sdf = DatabaseBuilder.createDateFormat(formatStr);
    sdf.setTimeZone(getTimeZone());
    return sdf;
  }

  /**
   * @returns the current handler for reading/writing properties, creating if
   * necessary
   */
  private PropertyMaps.Handler getPropsHandler() {
    if(_propsHandler == null) {
      _propsHandler = new PropertyMaps.Handler(this);
    }
    return _propsHandler;
  }

  @Override
  public FileFormat getFileFormat() throws IOException {

    if(_fileFormat == null) {

      Map<String,FileFormat> possibleFileFormats =
        getFormat().getPossibleFileFormats();

      if(possibleFileFormats.size() == 1) {

        // single possible format (null key), easy enough
        _fileFormat = possibleFileFormats.get(null);

      } else {

        // need to check the "AccessVersion" property
        String accessVersion = (String)getDatabaseProperties().getValue(
            PropertyMap.ACCESS_VERSION_PROP);

        if(StringUtil.isBlank(accessVersion)) {
          // no access version, fall back to "generic"
          accessVersion = null;
        }

        _fileFormat = possibleFileFormats.get(accessVersion);

        if(_fileFormat == null) {
          throw new IllegalStateException(withErrorContext(
                  "Could not determine FileFormat"));
        }
      }
    }
    return _fileFormat;
  }

  /**
   * @return a (possibly cached) page ByteBuffer for internal use.  the
   *         returned buffer should be released using
   *         {@link #releaseSharedBuffer} when no longer in use
   */
  private ByteBuffer takeSharedBuffer() {
    // we try to re-use a single shared _buffer, but occassionally, it may be
    // needed by multiple operations at the same time (e.g. loading a
    // secondary table while loading a primary table).  this method ensures
    // that we don't corrupt the _buffer, but instead force the second caller
    // to use a new buffer.
    if(_buffer != null) {
      ByteBuffer curBuffer = _buffer;
      _buffer = null;
      return curBuffer;
    }
    return _pageChannel.createPageBuffer();
  }

  /**
   * Relinquishes use of a page ByteBuffer returned by
   * {@link #takeSharedBuffer}.
   */
  private void releaseSharedBuffer(ByteBuffer buffer) {
    // we always stuff the returned buffer back into _buffer.  it doesn't
    // really matter if multiple values over-write, at the end of the day, we
    // just need one shared buffer
    _buffer = buffer;
  }

  /**
   * @return the currently configured database default language sort order for
   *         textual columns
   * @usage _intermediate_method_
   */
  public ColumnImpl.SortOrder getDefaultSortOrder() throws IOException {

    if(_defaultSortOrder == null) {
      initRootPageInfo();
    }
    return _defaultSortOrder;
  }

  /**
   * @return the currently configured database default code page for textual
   *         data (may not be relevant to all database versions)
   * @usage _intermediate_method_
   */
  public short getDefaultCodePage() throws IOException {

    if(_defaultCodePage == null) {
      initRootPageInfo();
    }
    return _defaultCodePage;
  }

  /**
   * Reads various config info from the db page 0.
   */
  private void initRootPageInfo() throws IOException {
    ByteBuffer buffer = takeSharedBuffer();
    try {
      _pageChannel.readRootPage(buffer);
      _defaultSortOrder = ColumnImpl.readSortOrder(
          buffer, _format.OFFSET_SORT_ORDER, _format);
      _defaultCodePage = buffer.getShort(_format.OFFSET_CODE_PAGE);
    } finally {
      releaseSharedBuffer(buffer);
    }
  }

  /**
   * @return a PropertyMaps instance decoded from the given bytes (always
   *         returns non-{@code null} result).
   * @usage _intermediate_method_
   */
  public PropertyMaps readProperties(byte[] propsBytes, int objectId,
                                     RowIdImpl rowId)
    throws IOException
  {
    return getPropsHandler().read(propsBytes, objectId, rowId, null);
  }

  /**
   * Read the system catalog
   */
  private void readSystemCatalog(boolean ignoreSystemCatalogIndex)
    throws IOException {
    _systemCatalog = loadTable(TABLE_SYSTEM_CATALOG, PAGE_SYSTEM_CATALOG,
                               SYSTEM_OBJECT_FLAGS, TYPE_TABLE);

    if(!ignoreSystemCatalogIndex) {
      try {
        _tableFinder = new DefaultTableFinder(
            _systemCatalog.newCursor()
            .setIndexByColumnNames(CAT_COL_PARENT_ID, CAT_COL_NAME)
            .setColumnMatcher(CaseInsensitiveColumnMatcher.INSTANCE)
            .toIndexCursor());
      } catch(IllegalArgumentException e) {
        if(LOG.isDebugEnabled()) {
          LOG.debug(withErrorContext(
                        "Could not find expected index on table " +
                        _systemCatalog.getName()));
        }
        // use table scan instead
        _tableFinder = new FallbackTableFinder(
            _systemCatalog.newCursor()
            .setColumnMatcher(CaseInsensitiveColumnMatcher.INSTANCE)
            .toCursor());
      }
    } else {
      if(LOG.isDebugEnabled()) {
        LOG.debug(withErrorContext(
                      "Ignoring index on table " + _systemCatalog.getName()));
      }
      // use table scan instead
      _tableFinder = new FallbackTableFinder(
          _systemCatalog.newCursor()
          .setColumnMatcher(CaseInsensitiveColumnMatcher.INSTANCE)
          .toCursor());
    }

    _tableParentId = _tableFinder.findObjectId(DB_PARENT_ID,
                                               SYSTEM_OBJECT_NAME_TABLES);

    if(_tableParentId == null) {
      throw new IOException(withErrorContext(
              "Did not find required parent table id"));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(withErrorContext(
          "Finished reading system catalog.  Tables: " + getTableNames()));
    }
  }

  @Override
  public Set<String> getTableNames() throws IOException {
    if(_tableNames == null) {
      _tableNames = getTableNames(true, false, true);
    }
    return _tableNames;
  }

  @Override
  public Set<String> getSystemTableNames() throws IOException {
    return getTableNames(false, true, false);
  }

  private Set<String> getTableNames(boolean normalTables, boolean systemTables,
                                    boolean linkedTables)
    throws IOException
  {
    Set<String> tableNames = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    _tableFinder.getTableNames(tableNames, normalTables, systemTables,
                               linkedTables);
    return tableNames;
  }

  @Override
  public Iterator<Table> iterator() {
    try {
      return new TableIterator(getTableNames());
    } catch(IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public Iterator<Table> iterator(TableIterableBuilder builder) {
    try {
      return new TableIterator(getTableNames(builder.isIncludeNormalTables(),
                                             builder.isIncludeSystemTables(),
                                             builder.isIncludeLinkedTables()));
    } catch(IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public TableIterableBuilder newIterable() {
    return new TableIterableBuilder(this);
  }

  @Override
  public Iterable<TableMetaData> newTableMetaDataIterable() {
    return new Iterable<TableMetaData>() {
      @Override
      public Iterator<TableMetaData> iterator() {
        try {
          return _tableFinder.iterateTableMetaData();
        } catch(IOException e) {
          throw new RuntimeIOException(e);
        }
      }
    };
  }

  @Override
  public TableImpl getTable(String name) throws IOException {
    return getTable(name, false);
  }

  @Override
  public TableMetaData getTableMetaData(String name) throws IOException {
    return getTableInfo(name, true);
  }

  /**
   * @param tableDefPageNumber the page number of a table definition
   * @return The table, or null if it doesn't exist
   * @usage _advanced_method_
   */
  public TableImpl getTable(int tableDefPageNumber) throws IOException {
    return loadTable(null, tableDefPageNumber, 0, null);
  }

  /**
   * @param name Table name
   * @param includeSystemTables whether to consider returning a system table
   * @return The table, or null if it doesn't exist
   */
  protected TableImpl getTable(String name, boolean includeSystemTables)
    throws IOException
  {
    TableInfo tableInfo = getTableInfo(name, includeSystemTables);
    return ((tableInfo != null) ?
            getTable(tableInfo, includeSystemTables) : null);
  }

  private TableInfo getTableInfo(String name, boolean includeSystemTables)
    throws IOException
  {
    TableInfo tableInfo = lookupTable(name);

    if ((tableInfo == null) || (tableInfo.pageNumber == null)) {
      return null;
    }
    if(!includeSystemTables && tableInfo.isSystem()) {
      return null;
    }

    return tableInfo;
  }

  private TableImpl getTable(TableInfo tableInfo, boolean includeSystemTables)
    throws IOException
  {
    if(tableInfo.getType() == TableMetaData.Type.LINKED) {

      if(_linkedDbs == null) {
        _linkedDbs = new HashMap<String,Database>();
      }

      String linkedDbName = tableInfo.getLinkedDbName();
      String linkedTableName = tableInfo.getLinkedTableName();
      Database linkedDb = _linkedDbs.get(linkedDbName);
      if(linkedDb == null) {
        linkedDb = getLinkResolver().resolveLinkedDatabase(this, linkedDbName);
        _linkedDbs.put(linkedDbName, linkedDb);
      }

      return ((DatabaseImpl)linkedDb).getTable(linkedTableName,
                                               includeSystemTables);
    }

    return loadTable(tableInfo.tableName, tableInfo.pageNumber,
                     tableInfo.flags, tableInfo.tableType);
  }

  /**
   * Create a new table in this database
   * @param name Name of the table to create
   * @param columns List of Columns in the table
   * @deprecated use {@link TableBuilder} instead
   */
  @Deprecated
  public void createTable(String name, List<ColumnBuilder> columns)
    throws IOException
  {
    createTable(name, columns, null);
  }

  /**
   * Create a new table in this database
   * @param name Name of the table to create
   * @param columns List of Columns in the table
   * @param indexes List of IndexBuilders describing indexes for the table
   * @deprecated use {@link TableBuilder} instead
   */
  @Deprecated
  public void createTable(String name, List<ColumnBuilder> columns,
                          List<IndexBuilder> indexes)
    throws IOException
  {
    new TableBuilder(name)
      .addColumns(columns)
      .addIndexes(indexes)
      .toTable(this);
  }

  @Override
  public void createLinkedTable(String name, String linkedDbName,
                                String linkedTableName)
    throws IOException
  {
    if(lookupTable(name) != null) {
      throw new IllegalArgumentException(withErrorContext(
          "Cannot create linked table with name of existing table '" + name +
          "'"));
    }

    validateIdentifierName(name, getFormat().MAX_TABLE_NAME_LENGTH, "table");
    validateName(linkedDbName, DataType.MEMO.getMaxSize(),
                 "linked database");
    validateIdentifierName(linkedTableName, getFormat().MAX_TABLE_NAME_LENGTH,
                           "linked table");

    getPageChannel().startWrite();
    try {

      int linkedTableId = _tableFinder.getNextFreeSyntheticId();

      addNewTable(name, linkedTableId, TYPE_LINKED_TABLE, linkedDbName,
                  linkedTableName);

    } finally {
      getPageChannel().finishWrite();
    }
  }

  /**
   * Adds a newly created table to the relevant internal database structures.
   */
  void addNewTable(String name, int tdefPageNumber, Short type,
                   String linkedDbName, String linkedTableName)
    throws IOException
  {
    //Add this table to our internal list.
    addTable(name, Integer.valueOf(tdefPageNumber), type, linkedDbName,
             linkedTableName);

    //Add this table to system tables
    addToSystemCatalog(name, tdefPageNumber, type, linkedDbName,
                       linkedTableName, _tableParentId);
    addToAccessControlEntries(tdefPageNumber, _tableParentId, _newTableSIDs);
  }

  @Override
  public List<Relationship> getRelationships(Table table1, Table table2)
    throws IOException
  {
    return getRelationships((TableImpl)table1, (TableImpl)table2);
  }

  public List<Relationship> getRelationships(
      TableImpl table1, TableImpl table2)
    throws IOException
  {
    int nameCmp = table1.getName().compareTo(table2.getName());
    if(nameCmp == 0) {
      throw new IllegalArgumentException(withErrorContext(
              "Must provide two different tables"));
    }
    if(nameCmp > 0) {
      // we "order" the two tables given so that we will return a collection
      // of relationships in the same order regardless of whether we are given
      // (TableFoo, TableBar) or (TableBar, TableFoo).
      TableImpl tmp = table1;
      table1 = table2;
      table2 = tmp;
    }

    return getRelationshipsImpl(table1, table2, true);
  }

  @Override
  public List<Relationship> getRelationships(Table table)
    throws IOException
  {
    if(table == null) {
      throw new IllegalArgumentException(withErrorContext("Must provide a table"));
    }
    // since we are getting relationships specific to certain table include
    // all tables
    return getRelationshipsImpl((TableImpl)table, null, true);
  }

  @Override
  public List<Relationship> getRelationships()
    throws IOException
  {
    return getRelationshipsImpl(null, null, false);
  }

  @Override
  public List<Relationship> getSystemRelationships()
    throws IOException
  {
    return getRelationshipsImpl(null, null, true);
  }

  private List<Relationship> getRelationshipsImpl(
      TableImpl table1, TableImpl table2, boolean includeSystemTables)
    throws IOException
  {
    initRelationships();

    List<Relationship> relationships = new ArrayList<Relationship>();

    if(table1 != null) {
      Cursor cursor = createCursorWithOptionalIndex(
          _relationships, REL_COL_FROM_TABLE, table1.getName());
      collectRelationships(cursor, table1, table2, relationships,
                           includeSystemTables);
      cursor = createCursorWithOptionalIndex(
          _relationships, REL_COL_TO_TABLE, table1.getName());
      collectRelationships(cursor, table2, table1, relationships,
                           includeSystemTables);
    } else {
      collectRelationships(new CursorBuilder(_relationships).toCursor(),
                           null, null, relationships, includeSystemTables);
    }

    return relationships;
  }

  RelationshipImpl writeRelationship(RelationshipCreator creator)
    throws IOException
  {
    initRelationships();

    String name = createRelationshipName(creator);
    RelationshipImpl newRel = creator.createRelationshipImpl(name);

    ColumnImpl ccol = _relationships.getColumn(REL_COL_COLUMN_COUNT);
    ColumnImpl flagCol = _relationships.getColumn(REL_COL_FLAGS);
    ColumnImpl icol = _relationships.getColumn(REL_COL_COLUMN_INDEX);
    ColumnImpl nameCol = _relationships.getColumn(REL_COL_NAME);
    ColumnImpl fromTableCol = _relationships.getColumn(REL_COL_FROM_TABLE);
    ColumnImpl fromColCol = _relationships.getColumn(REL_COL_FROM_COLUMN);
    ColumnImpl toTableCol = _relationships.getColumn(REL_COL_TO_TABLE);
    ColumnImpl toColCol = _relationships.getColumn(REL_COL_TO_COLUMN);

    int numCols = newRel.getFromColumns().size();
    List<Object[]> rows = new ArrayList<Object[]>(numCols);
    for(int i = 0; i < numCols; ++i) {
      Object[] row = new Object[_relationships.getColumnCount()];
      ccol.setRowValue(row, numCols);
      flagCol.setRowValue(row, newRel.getFlags());
      icol.setRowValue(row, i);
      nameCol.setRowValue(row, name);
      fromTableCol.setRowValue(row, newRel.getFromTable().getName());
      fromColCol.setRowValue(row, newRel.getFromColumns().get(i).getName());
      toTableCol.setRowValue(row, newRel.getToTable().getName());
      toColCol.setRowValue(row, newRel.getToColumns().get(i).getName());
      rows.add(row);
    }

    getPageChannel().startWrite();
    try {

      int relObjId = _tableFinder.getNextFreeSyntheticId();
      _relationships.addRows(rows);
      addToSystemCatalog(name, relObjId, TYPE_RELATIONSHIP, null, null,
                         _relParentId);
      addToAccessControlEntries(relObjId, _relParentId, _newRelSIDs);

    } finally {
      getPageChannel().finishWrite();
    }

    return newRel;
  }

  private void initRelationships() throws IOException {
    // the relationships table does not get loaded until first accessed
    if(_relationships == null) {
      // need the parent id of the relationships objects
      _relParentId = _tableFinder.findObjectId(DB_PARENT_ID,
                                               SYSTEM_OBJECT_NAME_RELATIONSHIPS);
      _relationships = getRequiredSystemTable(TABLE_SYSTEM_RELATIONSHIPS);
    }
  }

  private String createRelationshipName(RelationshipCreator creator) {
    // ensure that the final identifier name does not get too long
    // - the primary name is limited to ((max / 2) - 3)
    // - the total name is limited to (max - 3)
    int maxIdLen = getFormat().MAX_INDEX_NAME_LENGTH;
    int limit = (maxIdLen / 2) - 3;
    String origName = creator.getName();
    if (origName == null) {
      origName = creator.getPrimaryTable().getName();
      if(origName.length() > limit) {
        origName = origName.substring(0, limit);
      }
      origName += creator.getSecondaryTable().getName();
    }
    limit = maxIdLen - 3;
    if(origName.length() > limit) {
      origName = origName.substring(0, limit);
    }

    // now ensure name is unique
    Set<String> names = new HashSet<String>();

    // collect the names of all relationships for uniqueness check
    for(Row row :
          CursorImpl.createCursor(_systemCatalog).newIterable().setColumnNames(
              SYSTEM_CATALOG_COLUMNS))
    {
      String name = row.getString(CAT_COL_NAME);
      if (name != null && TYPE_RELATIONSHIP.equals(row.get(CAT_COL_TYPE))) {
        names.add(toLookupName(name));
      }
    }

    if(creator.hasReferentialIntegrity()) {
      // relationship name will also be index name in secondary table, so must
      // check those names as well
      for(Index idx : creator.getSecondaryTable().getIndexes()) {
        names.add(toLookupName(idx.getName()));
      }
    }

    String baseName = toLookupName(origName);
    String name = baseName;
    int i = 0;
    while(names.contains(name)) {
      name = baseName + (++i);
    }

    return ((i == 0) ? origName : (origName + i));
  }

  @Override
  public List<Query> getQueries() throws IOException
  {
    // the queries table does not get loaded until first accessed
    if(_queries == null) {
      _queries = getRequiredSystemTable(TABLE_SYSTEM_QUERIES);
    }

    // find all the queries from the system catalog
    List<Row> queryInfo = new ArrayList<Row>();
    Map<Integer,List<QueryImpl.Row>> queryRowMap =
      new HashMap<Integer,List<QueryImpl.Row>>();
    for(Row row :
          CursorImpl.createCursor(_systemCatalog).newIterable().setColumnNames(
              SYSTEM_CATALOG_COLUMNS))
    {
      String name = row.getString(CAT_COL_NAME);
      if (name != null && TYPE_QUERY.equals(row.get(CAT_COL_TYPE))) {
        queryInfo.add(row);
        Integer id = row.getInt(CAT_COL_ID);
        queryRowMap.put(id, new ArrayList<QueryImpl.Row>());
      }
    }

    // find all the query rows
    for(Row row : CursorImpl.createCursor(_queries)) {
      QueryImpl.Row queryRow = new QueryImpl.Row(row);
      List<QueryImpl.Row> queryRows = queryRowMap.get(queryRow.objectId);
      if(queryRows == null) {
        LOG.warn(withErrorContext(
                     "Found rows for query with id " + queryRow.objectId +
                     " missing from system catalog"));
        continue;
      }
      queryRows.add(queryRow);
    }

    // lastly, generate all the queries
    List<Query> queries = new ArrayList<Query>();
    for(Row row : queryInfo) {
      String name = row.getString(CAT_COL_NAME);
      Integer id = row.getInt(CAT_COL_ID);
      int flags = row.getInt(CAT_COL_FLAGS);
      List<QueryImpl.Row> queryRows = queryRowMap.get(id);
      queries.add(QueryImpl.create(flags, name, queryRows, id));
    }

    return queries;
  }

  @Override
  public TableImpl getSystemTable(String tableName) throws IOException
  {
    return getTable(tableName, true);
  }

  private TableImpl getRequiredSystemTable(String tableName) throws IOException
  {
    TableImpl table = getSystemTable(tableName);
    if(table == null) {
      throw new IOException(withErrorContext(
              "Could not find system table " + tableName));
    }
    return table;
  }

  @Override
  public PropertyMap getDatabaseProperties() throws IOException {
    if(_dbPropMaps == null) {
      _dbPropMaps = getPropertiesForDbObject(OBJECT_NAME_DB_PROPS);
    }
    return _dbPropMaps.getDefault();
  }

  @Override
  public PropertyMap getSummaryProperties() throws IOException {
    if(_summaryPropMaps == null) {
      _summaryPropMaps = getPropertiesForDbObject(OBJECT_NAME_SUMMARY_PROPS);
    }
    return _summaryPropMaps.getDefault();
  }

  @Override
  public PropertyMap getUserDefinedProperties() throws IOException {
    if(_userDefPropMaps == null) {
      _userDefPropMaps = getPropertiesForDbObject(OBJECT_NAME_USERDEF_PROPS);
    }
    return _userDefPropMaps.getDefault();
  }

  /**
   * @return the PropertyMaps for the object with the given id
   * @usage _advanced_method_
   */
  public PropertyMaps getPropertiesForObject(
      int objectId, PropertyMaps.Owner owner)
    throws IOException
  {
    return readProperties(
        objectId, _tableFinder.getObjectRow(
            objectId, SYSTEM_CATALOG_PROPS_COLUMNS), owner);
  }

  LocalDateTime getCreateDateForObject(int objectId) throws IOException {
    return getDateForObject(objectId, CAT_COL_DATE_CREATE);
  }

  LocalDateTime getUpdateDateForObject(int objectId) throws IOException {
    return getDateForObject(objectId, CAT_COL_DATE_UPDATE);
  }

  private LocalDateTime getDateForObject(int objectId, String dateCol)
    throws IOException {
    Row row = _tableFinder.getObjectRow(objectId, SYSTEM_CATALOG_DATE_COLUMNS);
    if(row == null) {
      return null;
    }
    Object date = row.get(dateCol);
    return ((date != null) ? ColumnImpl.toLocalDateTime(date, this) : null);
  }

  private Integer getDbParentId() throws IOException {
    if(_dbParentId == null) {
      // need the parent id of the databases objects
      _dbParentId = _tableFinder.findObjectId(DB_PARENT_ID,
                                              SYSTEM_OBJECT_NAME_DATABASES);
      if(_dbParentId == null) {
        throw new IOException(withErrorContext(
                "Did not find required parent db id"));
      }
    }
    return _dbParentId;
  }

  private byte[] getNewObjectOwner() throws IOException {
    if(_newObjOwner == null) {
      // there doesn't seem to be any obvious way to find the main "owner" of
      // an access db, but certain db objects seem to have the common db
      // owner.  we attempt to grab the db properties object and use its
      // owner.
      Row msysDbRow = _tableFinder.getObjectRow(
          getDbParentId(), OBJECT_NAME_DB_PROPS,
          Collections.singleton(CAT_COL_OWNER));
      byte[] owner = null;
      if(msysDbRow != null) {
        owner = msysDbRow.getBytes(CAT_COL_OWNER);
      }
      _newObjOwner = (((owner != null) && (owner.length > 0)) ?
                      owner : SYS_DEFAULT_SID);
    }
    return _newObjOwner;
  }

  /**
   * @return property group for the given "database" object
   */
  private PropertyMaps getPropertiesForDbObject(String dbName)
    throws IOException
  {
    return readProperties(
        -1, _tableFinder.getObjectRow(
            getDbParentId(), dbName, SYSTEM_CATALOG_PROPS_COLUMNS), null);
  }

  private PropertyMaps readProperties(int objectId, Row objectRow,
                                      PropertyMaps.Owner owner)
    throws IOException
  {
    byte[] propsBytes = null;
    RowIdImpl rowId = null;
    if(objectRow != null) {
      propsBytes = objectRow.getBytes(CAT_COL_PROPS);
      objectId = objectRow.getInt(CAT_COL_ID);
      rowId = (RowIdImpl)objectRow.getId();
    }
    return getPropsHandler().read(propsBytes, objectId, rowId, owner);
  }

  @Override
  public String getDatabasePassword() throws IOException
  {
    ByteBuffer buffer = takeSharedBuffer();
    try {
      _pageChannel.readRootPage(buffer);

      byte[] pwdBytes = new byte[_format.SIZE_PASSWORD];
      buffer.position(_format.OFFSET_PASSWORD);
      buffer.get(pwdBytes);

      // de-mask password using extra password mask if necessary (the extra
      // password mask is generated from the database creation date stored in
      // the header)
      byte[] pwdMask = getPasswordMask(buffer, _format);
      if(pwdMask != null) {
        for(int i = 0; i < pwdBytes.length; ++i) {
          pwdBytes[i] ^= pwdMask[i % pwdMask.length];
        }
      }

      boolean hasPassword = false;
      for(int i = 0; i < pwdBytes.length; ++i) {
        if(pwdBytes[i] != 0) {
          hasPassword = true;
          break;
        }
      }

      if(!hasPassword) {
        return null;
      }

      String pwd = ColumnImpl.decodeUncompressedText(pwdBytes, getCharset());

      // remove any trailing null chars
      int idx = pwd.indexOf('\0');
      if(idx >= 0) {
        pwd = pwd.substring(0, idx);
      }

      return pwd;
    } finally {
      releaseSharedBuffer(buffer);
    }
  }

  /**
   * Finds the relationships matching the given from and to tables from the
   * given cursor and adds them to the given list.
   */
  private void collectRelationships(
      Cursor cursor, TableImpl fromTable, TableImpl toTable,
      List<Relationship> relationships, boolean includeSystemTables)
    throws IOException
  {
    String fromTableName = ((fromTable != null) ? fromTable.getName() : null);
    String toTableName = ((toTable != null) ? toTable.getName() : null);

    for(Row row : cursor) {
      String fromName = row.getString(REL_COL_FROM_TABLE);
      String toName = row.getString(REL_COL_TO_TABLE);

      if(((fromTableName == null) ||
          fromTableName.equalsIgnoreCase(fromName)) &&
         ((toTableName == null) ||
          toTableName.equalsIgnoreCase(toName))) {

        String relName = row.getString(REL_COL_NAME);

        // found more info for a relationship.  see if we already have some
        // info for this relationship
        Relationship rel = null;
        for(Relationship tmp : relationships) {
          if(tmp.getName().equalsIgnoreCase(relName)) {
            rel = tmp;
            break;
          }
        }

        TableImpl relFromTable = fromTable;
        if(relFromTable == null) {
          relFromTable = getTable(fromName, includeSystemTables);
          if(relFromTable == null) {
            // invalid table or ignoring system tables, just ignore
            continue;
          }
        }
        TableImpl relToTable = toTable;
        if(relToTable == null) {
          relToTable = getTable(toName, includeSystemTables);
          if(relToTable == null) {
            // invalid table or ignoring system tables, just ignore
            continue;
          }
        }

        if(rel == null) {
          // new relationship
          int numCols = row.getInt(REL_COL_COLUMN_COUNT);
          int flags = row.getInt(REL_COL_FLAGS);
          rel = new RelationshipImpl(relName, relFromTable, relToTable,
                                     flags, numCols);
          relationships.add(rel);
        }

        // add column info
        int colIdx = row.getInt(REL_COL_COLUMN_INDEX);
        ColumnImpl fromCol = relFromTable.getColumn(
            row.getString(REL_COL_FROM_COLUMN));
        ColumnImpl toCol = relToTable.getColumn(
            row.getString(REL_COL_TO_COLUMN));

        rel.getFromColumns().set(colIdx, fromCol);
        rel.getToColumns().set(colIdx, toCol);
      }
    }
  }

  /**
   * Add a new table to the system catalog
   * @param name Table name
   * @param objectId id of the new object
   */
  private void addToSystemCatalog(String name, int objectId, Short type,
                                  String linkedDbName, String linkedTableName,
                                  Integer parentId)
    throws IOException
  {
    byte[] owner = getNewObjectOwner();
    Object[] catalogRow = new Object[_systemCatalog.getColumnCount()];
    int idx = 0;
    Date creationTime = new Date();
    for (Iterator<ColumnImpl> iter = _systemCatalog.getColumns().iterator();
         iter.hasNext(); idx++)
    {
      ColumnImpl col = iter.next();
      if (CAT_COL_ID.equals(col.getName())) {
        catalogRow[idx] = Integer.valueOf(objectId);
      } else if (CAT_COL_NAME.equals(col.getName())) {
        catalogRow[idx] = name;
      } else if (CAT_COL_TYPE.equals(col.getName())) {
        catalogRow[idx] = type;
      } else if (CAT_COL_DATE_CREATE.equals(col.getName()) ||
                 CAT_COL_DATE_UPDATE.equals(col.getName())) {
        catalogRow[idx] = creationTime;
      } else if (CAT_COL_PARENT_ID.equals(col.getName())) {
        catalogRow[idx] = parentId;
      } else if (CAT_COL_FLAGS.equals(col.getName())) {
        catalogRow[idx] = Integer.valueOf(0);
      } else if (CAT_COL_OWNER.equals(col.getName())) {
        catalogRow[idx] = owner;
      } else if (CAT_COL_DATABASE.equals(col.getName())) {
        catalogRow[idx] = linkedDbName;
      } else if (CAT_COL_FOREIGN_NAME.equals(col.getName())) {
        catalogRow[idx] = linkedTableName;
      }
    }
    _systemCatalog.addRow(catalogRow);
  }

  /**
   * Adds a new object to the system's access control entries
   */
  private void addToAccessControlEntries(
      Integer objectId, Integer parentId, List<byte[]> sids)
    throws IOException
  {
    if(sids.isEmpty()) {
      collectNewObjectSIDs(parentId, sids);
    }

    TableImpl acEntries = getAccessControlEntries();
    ColumnImpl acmCol = acEntries.getColumn(ACE_COL_ACM);
    ColumnImpl inheritCol = acEntries.getColumn(ACE_COL_F_INHERITABLE);
    ColumnImpl objIdCol = acEntries.getColumn(ACE_COL_OBJECT_ID);
    ColumnImpl sidCol = acEntries.getColumn(ACE_COL_SID);

    // construct a collection of ACE entries
    List<Object[]> aceRows = new ArrayList<Object[]>(sids.size());
    for(byte[] sid : sids) {
      Object[] aceRow = new Object[acEntries.getColumnCount()];
      acmCol.setRowValue(aceRow, SYS_FULL_ACCESS_ACM);
      inheritCol.setRowValue(aceRow, Boolean.FALSE);
      objIdCol.setRowValue(aceRow, objectId);
      sidCol.setRowValue(aceRow, sid);
      aceRows.add(aceRow);
    }
    acEntries.addRows(aceRows);
  }

  /**
   * Find collection of SIDs for the given parent id.
   */
  private void collectNewObjectSIDs(Integer parentId, List<byte[]> sids)
    throws IOException
  {
    // search for ACEs matching the given parentId.  use the index on the
    // objectId column if found (should be there)
    Cursor cursor = createCursorWithOptionalIndex(
        getAccessControlEntries(), ACE_COL_OBJECT_ID, parentId);

    for(Row row : cursor) {
      Integer objId = row.getInt(ACE_COL_OBJECT_ID);
      if(parentId.equals(objId)) {
        sids.add(row.getBytes(ACE_COL_SID));
      }
    }

    if(sids.isEmpty()) {
      // if all else fails, use the hard-coded default
      sids.add(SYS_DEFAULT_SID);
    }
  }

  /**
   * Reads a table with the given name from the given pageNumber.
   */
  private TableImpl loadTable(String name, int pageNumber, int flags, Short type)
    throws IOException
  {
    // first, check for existing table
    TableImpl table = _tableCache.get(pageNumber);
    if(table != null) {
      return table;
    }

    if(name == null) {
      // lookup table info from system catalog
      Row objectRow = _tableFinder.getObjectRow(
          pageNumber, SYSTEM_CATALOG_COLUMNS);
      if(objectRow == null) {
        return null;
      }

      name = objectRow.getString(CAT_COL_NAME);
      flags = objectRow.getInt(CAT_COL_FLAGS);
      type = objectRow.getShort(CAT_COL_TYPE);
    }

    // need to load table from db
    return _tableCache.put(readTable(name, pageNumber, flags, type));
  }

  /**
   * Reads a table with the given name from the given pageNumber.
   */
  private TableImpl readTable(
      String name, int pageNumber, int flags, Short type)
    throws IOException
  {
    ByteBuffer buffer = takeSharedBuffer();
    try {
      // need to load table from db
      _pageChannel.readPage(buffer, pageNumber);
      byte pageType = buffer.get(0);
      if (pageType != PageTypes.TABLE_DEF) {
        throw new IOException(withErrorContext(
            "Looking for " + name + " at page " + pageNumber +
            ", but page type is " + pageType));
      }
      return (!TYPE_LINKED_ODBC_TABLE.equals(type) ?
              new TableImpl(this, buffer, pageNumber, name, flags) :
              new TableDefinitionImpl(this, buffer, pageNumber, name, flags));
    } finally {
      releaseSharedBuffer(buffer);
    }
  }

  /**
   * Creates a Cursor restricted to the given column value if possible (using
   * an existing index), otherwise a simple table cursor.
   */
  private Cursor createCursorWithOptionalIndex(
      TableImpl table, String colName, Object colValue)
    throws IOException
  {
    try {
      return table.newCursor()
        .setIndexByColumnNames(colName)
        .setSpecificEntry(colValue)
        .toCursor();
    } catch(IllegalArgumentException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug(withErrorContext(
            "Could not find expected index on table " + table.getName()));
      }
    }
    // use table scan instead
    return CursorImpl.createCursor(table);
  }

  @Override
  public void flush() throws IOException {
    if(_linkedDbs != null) {
      for(Database linkedDb : _linkedDbs.values()) {
        linkedDb.flush();
      }
    }
    _pageChannel.flush();
  }

  @Override
  public void close() throws IOException {
    if(_linkedDbs != null) {
      for(Database linkedDb : _linkedDbs.values()) {
        linkedDb.close();
      }
    }
    _pageChannel.close();
  }

  public void validateNewTableName(String name) throws IOException {
    validateIdentifierName(name, getFormat().MAX_TABLE_NAME_LENGTH, "table");

    if(lookupTable(name) != null) {
      throw new IllegalArgumentException(withErrorContext(
              "Cannot create table with name of existing table '" + name + "'"));
    }
  }

  /**
   * Validates an identifier name.
   *
   * Names of fields, controls, and objects in Microsoft Access:
   * <ul>
   * <li>Can include any combination of letters, numbers, spaces, and special
   *     characters except a period (.), an exclamation point (!), an accent
   *     grave (`), and brackets ([ ]).</li>
   * <li>Can't begin with leading spaces.</li>
   * <li>Can't include control characters (ASCII values 0 through 31).</li>
   * </ul>
   *
   * @usage _advanced_method_
   */
  public static void validateIdentifierName(String name,
                                            int maxLength,
                                            String identifierType)
  {
    // basic name validation
    validateName(name, maxLength, identifierType);

    // additional identifier validation
    if(INVALID_IDENTIFIER_CHARS.matcher(name).find()) {
      throw new IllegalArgumentException(
          identifierType + " name '" + name + "' contains invalid characters");
    }

    // cannot start with spaces
    if(name.charAt(0) == ' ') {
      throw new IllegalArgumentException(
          identifierType + " name '" + name +
          "' cannot start with a space character");
    }
  }

  /**
   * Validates a name.
   */
  private static void validateName(String name, int maxLength, String nameType)
  {
    if(StringUtil.isBlank(name)) {
      throw new IllegalArgumentException(
          nameType + " must have non-blank name");
    }
    if(name.length() > maxLength) {
      throw new IllegalArgumentException(
          nameType + " name is longer than max length of " + maxLength +
          ": " + name);
    }
  }

  @Override
  public String toString() {
    return StringUtil.reflectionToString(this);
  }

  /**
   * Adds a table to the _tableLookup and resets the _tableNames set
   */
  private void addTable(String tableName, Integer pageNumber, Short type,
                        String linkedDbName, String linkedTableName)
  {
    _tableLookup.put(toLookupName(tableName),
                     createTableInfo(tableName, pageNumber, 0, type,
                                     linkedDbName, linkedTableName, null));
    // clear this, will be created next time needed
    _tableNames = null;
  }

  private static TableInfo createTableInfo(
      String tableName, Short type, Row row) {

    Integer pageNumber = row.getInt(CAT_COL_ID);
    int flags = row.getInt(CAT_COL_FLAGS);
    String linkedDbName = row.getString(CAT_COL_DATABASE);
    String linkedTableName = row.getString(CAT_COL_FOREIGN_NAME);
    String connectName = row.getString(CAT_COL_CONNECT_NAME);

    return createTableInfo(tableName, pageNumber, flags, type, linkedDbName,
                           linkedTableName, connectName);
  }

  /**
   * Creates a TableInfo instance appropriate for the given table data.
   */
  private static TableInfo createTableInfo(
      String tableName, Integer pageNumber, int flags, Short type,
      String linkedDbName, String linkedTableName, String connectName)
  {
    if(TYPE_LINKED_TABLE.equals(type)) {
      return new LinkedTableInfo(pageNumber, tableName, flags, type,
                                 linkedDbName, linkedTableName);
    } else if(TYPE_LINKED_ODBC_TABLE.equals(type)) {
      return new LinkedODBCTableInfo(pageNumber, tableName, flags, type,
                                     connectName, linkedTableName);
    }
    return new TableInfo(pageNumber, tableName, flags, type);
  }

  /**
   * @return the tableInfo of the given table, if any
   */
  private TableInfo lookupTable(String tableName) throws IOException {

    String lookupTableName = toLookupName(tableName);
    TableInfo tableInfo = _tableLookup.get(lookupTableName);
    if(tableInfo != null) {
      return tableInfo;
    }

    tableInfo = _tableFinder.lookupTable(tableName);

    if(tableInfo != null) {
      // cache for later
      _tableLookup.put(lookupTableName, tableInfo);
    }

    return tableInfo;
  }

  /**
   * @return a string usable in the _tableLookup map.
   */
  public static String toLookupName(String name) {
    return ((name != null) ? name.toUpperCase() : null);
  }

  /**
   * @return {@code true} if the given flags indicate that an object is some
   *         sort of system object, {@code false} otherwise.
   */
  private static boolean isSystemObject(int flags) {
    return ((flags & SYSTEM_OBJECT_FLAGS) != 0);
  }

  /**
   * Returns the default TimeZone.  This is normally the platform default
   * TimeZone as returned by {@link TimeZone#getDefault}, but can be
   * overridden using the system property
   * {@value com.healthmarketscience.jackcess.Database#TIMEZONE_PROPERTY}.
   * @usage _advanced_method_
   */
  public static TimeZone getDefaultTimeZone()
  {
    String tzProp = System.getProperty(TIMEZONE_PROPERTY);
    if(tzProp != null) {
      tzProp = tzProp.trim();
      if(tzProp.length() > 0) {
        return TimeZone.getTimeZone(tzProp);
      }
    }

    // use system default
    return TimeZone.getDefault();
  }

  /**
   * Returns the default Charset for the given JetFormat.  This may or may not
   * be platform specific, depending on the format, but can be overridden
   * using a system property composed of the prefix
   * {@value com.healthmarketscience.jackcess.Database#CHARSET_PROPERTY_PREFIX}
   * followed by the JetFormat version to which the charset should apply,
   * e.g. {@code "com.healthmarketscience.jackcess.charset.VERSION_3"}.
   * @usage _advanced_method_
   */
  public static Charset getDefaultCharset(JetFormat format)
  {
    String csProp = System.getProperty(CHARSET_PROPERTY_PREFIX + format);
    if(csProp != null) {
      csProp = csProp.trim();
      if(csProp.length() > 0) {
        return Charset.forName(csProp);
      }
    }

    // use format default
    return format.CHARSET;
  }

  /**
   * Returns the default Table.ColumnOrder.  This defaults to
   * {@link Database#DEFAULT_COLUMN_ORDER}, but can be overridden using the system
   * property {@value com.healthmarketscience.jackcess.Database#COLUMN_ORDER_PROPERTY}.
   * @usage _advanced_method_
   */
  public static Table.ColumnOrder getDefaultColumnOrder()
  {
    return getEnumSystemProperty(Table.ColumnOrder.class, COLUMN_ORDER_PROPERTY,
                                 DEFAULT_COLUMN_ORDER);
  }

  /**
   * Returns the default enforce foreign-keys policy.  This defaults to
   * {@code true}, but can be overridden using the system
   * property {@value com.healthmarketscience.jackcess.Database#FK_ENFORCE_PROPERTY}.
   * @usage _advanced_method_
   */
  public static boolean getDefaultEnforceForeignKeys()
  {
    String prop = System.getProperty(FK_ENFORCE_PROPERTY);
    if(prop != null) {
      return Boolean.TRUE.toString().equalsIgnoreCase(prop);
    }
    return true;
  }

  /**
   * Returns the default allow auto number insert policy.  This defaults to
   * {@code false}, but can be overridden using the system
   * property {@value com.healthmarketscience.jackcess.Database#ALLOW_AUTONUM_INSERT_PROPERTY}.
   * @usage _advanced_method_
   */
  public static boolean getDefaultAllowAutoNumberInsert()
  {
    String prop = System.getProperty(ALLOW_AUTONUM_INSERT_PROPERTY);
    if(prop != null) {
      return Boolean.TRUE.toString().equalsIgnoreCase(prop);
    }
    return false;
  }

  /**
   * Returns the default enable expression evaluation policy.  This defaults to
   * {@code true}, but can be overridden using the system
   * property {@value com.healthmarketscience.jackcess.Database#ENABLE_EXPRESSION_EVALUATION_PROPERTY}.
   * @usage _advanced_method_
   */
  public static boolean getDefaultEvaluateExpressions()
  {
    String prop = System.getProperty(ENABLE_EXPRESSION_EVALUATION_PROPERTY);
    if(prop != null) {
      return Boolean.TRUE.toString().equalsIgnoreCase(prop);
    }
    return true;
  }

  /**
   * Returns the default DateTimeType.  This defaults to
   * {@link DateTimeType#LOCAL_DATE_TIME}, but can be overridden using the system
   * property {@value com.healthmarketscience.jackcess.Database#DATE_TIME_TYPE_PROPERTY}.
   * @usage _advanced_method_
   */
  public static DateTimeType getDefaultDateTimeType() {
    return getEnumSystemProperty(DateTimeType.class, DATE_TIME_TYPE_PROPERTY,
                                 DateTimeType.LOCAL_DATE_TIME);
  }

  /**
   * Copies the given db InputStream to the given channel using the most
   * efficient means possible.
   */
  protected static void transferDbFrom(FileChannel channel, InputStream in)
    throws IOException
  {
    ReadableByteChannel readChannel = Channels.newChannel(in);
    if(!BROKEN_NIO) {
      // sane implementation
      channel.transferFrom(readChannel, 0, MAX_EMPTYDB_SIZE);
    } else {
      // do things the hard way for broken vms
      ByteBuffer bb = ByteBuffer.allocate(8096);
      while(readChannel.read(bb) >= 0) {
        bb.flip();
        channel.write(bb);
        bb.clear();
      }
    }
  }

  /**
   * Returns the password mask retrieved from the given header page and
   * format, or {@code null} if this format does not use a password mask.
   * @usage _advanced_method_
   */
  public static byte[] getPasswordMask(ByteBuffer buffer, JetFormat format)
  {
    // get extra password mask if necessary (the extra password mask is
    // generated from the database creation date stored in the header)
    int pwdMaskPos = format.OFFSET_HEADER_DATE;
    if(pwdMaskPos < 0) {
      return null;
    }

    buffer.position(pwdMaskPos);
    double dateVal = Double.longBitsToDouble(buffer.getLong());

    byte[] pwdMask = new byte[4];
    PageChannel.wrap(pwdMask).putInt((int)dateVal);

    return pwdMask;
  }

  protected static InputStream getResourceAsStream(String resourceName)
    throws IOException
  {
    InputStream stream = DatabaseImpl.class.getClassLoader()
      .getResourceAsStream(resourceName);

    if(stream == null) {

      stream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(resourceName);

      if(stream == null) {
        throw new IOException("Could not load jackcess resource " +
                              resourceName);
      }
    }

    return stream;
  }

  private static boolean isTableType(Short objType) {
    return(TYPE_TABLE.equals(objType) || isAnyLinkedTableType(objType));
  }

  public static FileFormatDetails getFileFormatDetails(FileFormat fileFormat) {
    return FILE_FORMAT_DETAILS.get(fileFormat);
  }

  private static void addFileFormatDetails(
      FileFormat fileFormat, String emptyFileName, JetFormat format)
  {
    String emptyFile =
      ((emptyFileName != null) ?
       RESOURCE_PATH + emptyFileName + fileFormat.getFileExtension() : null);
    FILE_FORMAT_DETAILS.put(fileFormat, new FileFormatDetails(emptyFile, format));
  }

  private static String getName(Path file) {
    if(file == null) {
      return "<UNKNOWN.DB>";
    }
    return file.getFileName().toString();
  }

  private String withErrorContext(String msg) {
    return withErrorContext(msg, getName());
  }

  private static String withErrorContext(String msg, String dbName) {
    return msg + " (Db=" + dbName + ")";
  }

  private static <E extends Enum<E>> E getEnumSystemProperty(
      Class<E> enumClass, String propName, E defaultValue)
  {
    String prop = System.getProperty(propName);
    if(prop != null) {
      prop = prop.trim().toUpperCase();
      if(!prop.isEmpty()) {
        return Enum.valueOf(enumClass, prop);
      }
    }
    return defaultValue;
  }

  private static boolean isAnyLinkedTableType(Short type) {
    return (TYPE_LINKED_TABLE.equals(type) ||
            TYPE_LINKED_ODBC_TABLE.equals(type));
  }

  /**
   * Utility class for storing table page number and actual name.
   */
  private static class TableInfo implements TableMetaData
  {
    public final Integer pageNumber;
    public final String tableName;
    public final int flags;
    public final Short tableType;

    private TableInfo(Integer newPageNumber, String newTableName, int newFlags,
                      Short newTableType) {
      pageNumber = newPageNumber;
      tableName = newTableName;
      flags = newFlags;
      tableType = newTableType;
    }

    @Override
    public Type getType() {
      return Type.LOCAL;
    }

    @Override
    public String getName() {
      return tableName;
    }

    @Override
    public boolean isLinked() {
      return false;
    }

    @Override
    public boolean isSystem() {
      return isSystemObject(flags);
    }

    @Override
    public String getLinkedTableName() {
      return null;
    }

    @Override
    public String getLinkedDbName() {
      return null;
    }

    @Override
    public String getConnectionName() {
      return null;
    }

    @Override
    public Table open(Database db) throws IOException {
      return ((DatabaseImpl)db).getTable(this, true);
    }

    @Override
    public TableDefinition getTableDefinition(Database db) throws IOException {
      return null;
    }

    @Override
    public String toString() {
      ToStringBuilder sb = ToStringBuilder.valueBuilder("TableMetaData")
        .append("name", getName());
        if(isSystem()) {
          sb.append("isSystem", isSystem());
        }
        if(isLinked()) {
          sb.append("isLinked", isLinked())
            .append("linkedTableName", getLinkedTableName())
            .append("linkedDbName", getLinkedDbName())
            .append("connectionName", maskPassword(getConnectionName()));
        }
        return sb.toString();
    }

    private static String maskPassword(String connectionName) {
      return ((connectionName != null) ?
              ODBC_PWD_PATTERN.matcher(connectionName).replaceAll("PWD=XXXXXX") :
              null);
    }
  }

  /**
   * Utility class for storing linked table info
   */
  private static class LinkedTableInfo extends TableInfo
  {
    private final String _linkedDbName;
    private final String _linkedTableName;

    private LinkedTableInfo(Integer newPageNumber, String newTableName,
                            int newFlags, Short newTableType,
                            String newLinkedDbName,
                            String newLinkedTableName) {
      super(newPageNumber, newTableName, newFlags, newTableType);
      _linkedDbName = newLinkedDbName;
      _linkedTableName = newLinkedTableName;
    }

    @Override
    public Type getType() {
      return Type.LINKED;
    }

    @Override
    public boolean isLinked() {
      return true;
    }

    @Override
    public String getLinkedTableName() {
      return _linkedTableName;
    }

    @Override
    public String getLinkedDbName() {
      return _linkedDbName;
    }
  }

  /**
   * Utility class for storing linked ODBC table info
   */
  private static class LinkedODBCTableInfo extends TableInfo
  {
    private final String _linkedTableName;
    private final String _connectionName;

    private LinkedODBCTableInfo(Integer newPageNumber, String newTableName,
                                int newFlags, Short newTableType,
                                String connectName,
                                String newLinkedTableName) {
      super(newPageNumber, newTableName, newFlags, newTableType);
      _linkedTableName = newLinkedTableName;
      _connectionName = connectName;
    }

    @Override
    public Type getType() {
      return Type.LINKED_ODBC;
    }

    @Override
    public boolean isLinked() {
      return true;
    }

    @Override
    public String getLinkedTableName() {
      return _linkedTableName;
    }

    @Override
    public String getConnectionName() {
      return _connectionName;
    }

    @Override
    public Table open(Database db) throws IOException {
      return null;
    }

    @Override
    public TableDefinition getTableDefinition(Database db) throws IOException {
      return (((pageNumber != null) && (pageNumber > 0)) ?
              ((DatabaseImpl)db).getTable(this, true) :
              null);
    }
  }

  /**
   * Table iterator for this database, unmodifiable.
   */
  private class TableIterator implements Iterator<Table>
  {
    private Iterator<String> _tableNameIter;

    private TableIterator(Set<String> tableNames) {
      _tableNameIter = tableNames.iterator();
    }

    @Override
    public boolean hasNext() {
      return _tableNameIter.hasNext();
    }

    @Override
    public Table next() {
      if(!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        return getTable(_tableNameIter.next(), true);
      } catch(IOException e) {
        throw new RuntimeIOException(e);
      }
    }
  }

  /**
   * Utility class for handling table lookups.
   */
  private abstract class TableFinder
  {
    public Integer findObjectId(Integer parentId, String name)
      throws IOException
    {
      Cursor cur = findRow(parentId, name);
      if(cur == null) {
        return null;
      }
      ColumnImpl idCol = _systemCatalog.getColumn(CAT_COL_ID);
      return (Integer)cur.getCurrentRowValue(idCol);
    }

    public Row getObjectRow(Integer parentId, String name,
                            Collection<String> columns)
      throws IOException
    {
      Cursor cur = findRow(parentId, name);
      return ((cur != null) ? cur.getCurrentRow(columns) : null);
    }

    public Row getObjectRow(
        Integer objectId, Collection<String> columns)
      throws IOException
    {
      Cursor cur = findRow(objectId);
      return ((cur != null) ? cur.getCurrentRow(columns) : null);
    }

    public void getTableNames(Set<String> tableNames,
                              boolean normalTables,
                              boolean systemTables,
                              boolean linkedTables)
      throws IOException
    {
      for(Row row : getTableNamesCursor().newIterable().setColumnNames(
              SYSTEM_CATALOG_COLUMNS)) {

        String tableName = row.getString(CAT_COL_NAME);
        int flags = row.getInt(CAT_COL_FLAGS);
        Short type = row.getShort(CAT_COL_TYPE);
        int parentId = row.getInt(CAT_COL_PARENT_ID);

        if(parentId != _tableParentId) {
          continue;
        }

        if(TYPE_TABLE.equals(type)) {
          if(!isSystemObject(flags)) {
            if(normalTables) {
              tableNames.add(tableName);
            }
          } else if(systemTables) {
            tableNames.add(tableName);
          }
        } else if(linkedTables && isAnyLinkedTableType(type)) {
          tableNames.add(tableName);
        }
      }
    }

    public boolean isLinkedTable(Table table) throws IOException
    {
      for(Row row : getTableNamesCursor().newIterable().setColumnNames(
              SYSTEM_CATALOG_TABLE_DETAIL_COLUMNS)) {
        Short type = row.getShort(CAT_COL_TYPE);
        String linkedDbName = row.getString(CAT_COL_DATABASE);
        String linkedTableName = row.getString(CAT_COL_FOREIGN_NAME);

        if(TYPE_LINKED_TABLE.equals(type) &&
           matchesLinkedTable(table, linkedTableName, linkedDbName)) {
          return true;
        }
      }
      return false;
    }

    public int getNextFreeSyntheticId() throws IOException {
      int maxSynthId = findMaxSyntheticId();
      if(maxSynthId >= -1) {
        // bummer, no more ids available
        throw new IllegalStateException(withErrorContext(
                "Too many database objects!"));
      }
      return maxSynthId + 1;
    }

    public Iterator<TableMetaData> iterateTableMetaData() throws IOException {
      return new Iterator<TableMetaData>() {
        private final Iterator<Row> _iter =
          getTableNamesCursor().newIterable().setColumnNames(
              SYSTEM_CATALOG_TABLE_DETAIL_COLUMNS).iterator();
        private TableMetaData _next;

        @Override
        public boolean hasNext() {
          if((_next == null) && _iter.hasNext()) {
            _next = nextTableMetaData(_iter);
          }
          return (_next != null);
        }

        @Override
        public TableMetaData next() {
          if(!hasNext()) {
            throw new NoSuchElementException();
          }

          TableMetaData next = _next;
          _next = null;
          return next;
        }
      };
    }

    private TableMetaData nextTableMetaData(Iterator<Row> detailIter) {

      while(detailIter.hasNext()) {
        Row row = detailIter.next();

        Short type = row.getShort(CAT_COL_TYPE);
        if(!isTableType(type)) {
          continue;
        }

        int parentId = row.getInt(CAT_COL_PARENT_ID);
        if(parentId != _tableParentId) {
          continue;
        }

        String realName = row.getString(CAT_COL_NAME);

        return createTableInfo(realName, type, row);
      }

      return null;
    }

    protected abstract Cursor findRow(Integer parentId, String name)
      throws IOException;

    protected abstract Cursor findRow(Integer objectId)
      throws IOException;

    protected abstract Cursor getTableNamesCursor() throws IOException;

    public abstract TableInfo lookupTable(String tableName)
      throws IOException;

    protected abstract int findMaxSyntheticId() throws IOException;
  }

  /**
   * Normal table lookup handler, using catalog table index.
   */
  private final class DefaultTableFinder extends TableFinder
  {
    private final IndexCursor _systemCatalogCursor;
    private IndexCursor _systemCatalogIdCursor;

    private DefaultTableFinder(IndexCursor systemCatalogCursor) {
      _systemCatalogCursor = systemCatalogCursor;
    }

    private void initIdCursor() throws IOException {
      if(_systemCatalogIdCursor == null) {
        _systemCatalogIdCursor = _systemCatalog.newCursor()
          .setIndexByColumnNames(CAT_COL_ID)
          .toIndexCursor();
      }
    }

    @Override
    protected Cursor findRow(Integer parentId, String name)
      throws IOException
    {
      return (_systemCatalogCursor.findFirstRowByEntry(parentId, name) ?
              _systemCatalogCursor : null);
    }

    @Override
    protected Cursor findRow(Integer objectId) throws IOException
    {
      initIdCursor();
      return (_systemCatalogIdCursor.findFirstRowByEntry(objectId) ?
              _systemCatalogIdCursor : null);
    }

    @Override
    public TableInfo lookupTable(String tableName) throws IOException {

      if(findRow(_tableParentId, tableName) == null) {
        return null;
      }

      Row row = _systemCatalogCursor.getCurrentRow(
          SYSTEM_CATALOG_TABLE_DETAIL_COLUMNS);
      Short type = row.getShort(CAT_COL_TYPE);

      if(!isTableType(type)) {
        return null;
      }

      String realName = row.getString(CAT_COL_NAME);

      return createTableInfo(realName, type, row);
    }

    @Override
    protected Cursor getTableNamesCursor() throws IOException {
      return _systemCatalogCursor.getIndex().newCursor()
        .setStartEntry(_tableParentId, IndexData.MIN_VALUE)
        .setEndEntry(_tableParentId, IndexData.MAX_VALUE)
        .toIndexCursor();
    }

    @Override
    protected int findMaxSyntheticId() throws IOException {
      initIdCursor();
      _systemCatalogIdCursor.reset();

      // synthetic ids count up from min integer.  so the current, highest,
      // in-use synthetic id is the max id < 0.
      _systemCatalogIdCursor.findClosestRowByEntry(0);
      if(!_systemCatalogIdCursor.moveToPreviousRow()) {
        return Integer.MIN_VALUE;
      }
      ColumnImpl idCol = _systemCatalog.getColumn(CAT_COL_ID);
      return (Integer)_systemCatalogIdCursor.getCurrentRowValue(idCol);
    }
  }

  /**
   * Fallback table lookup handler, using catalog table scans.
   */
  private final class FallbackTableFinder extends TableFinder
  {
    private final Cursor _systemCatalogCursor;

    private FallbackTableFinder(Cursor systemCatalogCursor) {
      _systemCatalogCursor = systemCatalogCursor;
    }

    @Override
    protected Cursor findRow(Integer parentId, String name)
      throws IOException
    {
      Map<String,Object> rowPat = new HashMap<String,Object>();
      rowPat.put(CAT_COL_PARENT_ID, parentId);
      rowPat.put(CAT_COL_NAME, name);
      return (_systemCatalogCursor.findFirstRow(rowPat) ?
              _systemCatalogCursor : null);
    }

    @Override
    protected Cursor findRow(Integer objectId) throws IOException
    {
      ColumnImpl idCol = _systemCatalog.getColumn(CAT_COL_ID);
      return (_systemCatalogCursor.findFirstRow(idCol, objectId) ?
              _systemCatalogCursor : null);
    }

    @Override
    public TableInfo lookupTable(String tableName) throws IOException {

      for(Row row : _systemCatalogCursor.newIterable().setColumnNames(
              SYSTEM_CATALOG_TABLE_DETAIL_COLUMNS)) {

        Short type = row.getShort(CAT_COL_TYPE);
        if(!isTableType(type)) {
          continue;
        }

        int parentId = row.getInt(CAT_COL_PARENT_ID);
        if(parentId != _tableParentId) {
          continue;
        }

        String realName = row.getString(CAT_COL_NAME);
        if(!tableName.equalsIgnoreCase(realName)) {
          continue;
        }

        return createTableInfo(realName, type, row);
      }

      return null;
    }

    @Override
    protected Cursor getTableNamesCursor() throws IOException {
      return _systemCatalogCursor;
    }

    @Override
    protected int findMaxSyntheticId() throws IOException {
      // find max id < 0
      ColumnImpl idCol = _systemCatalog.getColumn(CAT_COL_ID);
      _systemCatalogCursor.reset();
      int curMaxSynthId = Integer.MIN_VALUE;
      while(_systemCatalogCursor.moveToNextRow()) {
        int id = (Integer)_systemCatalogCursor.getCurrentRowValue(idCol);
        if((id > curMaxSynthId) && (id < 0)) {
          curMaxSynthId = id;
        }
      }
      return curMaxSynthId;
    }
  }

  /**
   * WeakReference for a Table which holds the table pageNumber (for later
   * cache purging).
   */
  private static final class WeakTableReference extends WeakReference<TableImpl>
  {
    private final Integer _pageNumber;

    private WeakTableReference(Integer pageNumber, TableImpl table,
                               ReferenceQueue<TableImpl> queue) {
      super(table, queue);
      _pageNumber = pageNumber;
    }

    public Integer getPageNumber() {
      return _pageNumber;
    }
  }

  /**
   * Cache of currently in-use tables, allows re-use of existing tables.
   */
  private static final class TableCache
  {
    private final Map<Integer,WeakTableReference> _tables =
      new HashMap<Integer,WeakTableReference>();
    private final ReferenceQueue<TableImpl> _queue =
      new ReferenceQueue<TableImpl>();

    public TableImpl get(Integer pageNumber) {
      WeakTableReference ref = _tables.get(pageNumber);
      return ((ref != null) ? ref.get() : null);
    }

    public TableImpl put(TableImpl table) {
      purgeOldRefs();

      Integer pageNumber = table.getTableDefPageNumber();
      WeakTableReference ref = new WeakTableReference(
          pageNumber, table, _queue);
      _tables.put(pageNumber, ref);

      return table;
    }

    private void purgeOldRefs() {
      WeakTableReference oldRef = null;
      while((oldRef = (WeakTableReference)_queue.poll()) != null) {
        _tables.remove(oldRef.getPageNumber());
      }
    }
  }

  /**
   * Internal details for each FileForrmat
   * @usage _advanced_class_
   */
  public static final class FileFormatDetails
  {
    private final String _emptyFile;
    private final JetFormat _format;

    private FileFormatDetails(String emptyFile, JetFormat format) {
      _emptyFile = emptyFile;
      _format = format;
    }

    public String getEmptyFilePath() {
      return _emptyFile;
    }

    public JetFormat getFormat() {
      return _format;
    }
  }
}
