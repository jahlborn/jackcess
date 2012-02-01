/*
Copyright (c) 2005 Health Market Science, Inc.

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

You can contact Health Market Science at info@healthmarketscience.com
or at the following address:

Health Market Science
2700 Horizon Drive
Suite 200
King of Prussia, PA 19406
*/

package com.healthmarketscience.jackcess;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import com.healthmarketscience.jackcess.query.Query;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An Access database.
 * <p>
 * There is optional support for large indexes (enabled by default).  This
 * optional support can be disabled via a few different means:
 * <ul>
 * <li>Setting the system property {@value #USE_BIG_INDEX_PROPERTY} to
 *     {@code "false"} will disable "large" index support across the jvm</li>
 * <li>Calling {@link #setUseBigIndex} on a Database instance will override
 *     any system property setting for "large" index support for all tables
 *     subsequently created from that instance</li>
 * <li>Calling {@link #getTable(String,boolean)} can selectively
 *     enable/disable "large" index support on a per-table basis (overriding
 *     any Database or system property setting)</li>
 * </ul>
 *
 * @author Tim McCune
 * @usage _general_class_
 */
public class Database
  implements Iterable<Table>, Closeable, Flushable
{
  
  private static final Log LOG = LogFactory.getLog(Database.class);

  /** this is the default "userId" used if we cannot find existing info.  this
      seems to be some standard "Admin" userId for access files */
  private static final byte[] SYS_DEFAULT_SID = new byte[2];
  static {
    SYS_DEFAULT_SID[0] = (byte) 0xA6;
    SYS_DEFAULT_SID[1] = (byte) 0x33;
  }

  /** default value for the auto-sync value ({@code true}).  this is slower,
   *  but leaves more chance of a useable database in the face of failures.
   * @usage _general_field_
   */
  public static final boolean DEFAULT_AUTO_SYNC = true;

  /** the default value for the resource path used to load classpath
   *  resources.
   * @usage _general_field_
   */
  public static final String DEFAULT_RESOURCE_PATH = 
    "com/healthmarketscience/jackcess/";

  /**
   * the default sort order for table columns.
   * @usage _intermediate_field_
   */
  public static final Table.ColumnOrder DEFAULT_COLUMN_ORDER = 
    Table.ColumnOrder.DATA;

  /** (boolean) system property which can be used to disable the default big
   *  index support.
   * @usage _general_field_
   */
  public static final String USE_BIG_INDEX_PROPERTY =
    "com.healthmarketscience.jackcess.bigIndex";

  /** system property which can be used to set the default TimeZone used for
   *  date calculations.
   * @usage _general_field_
   */
  public static final String TIMEZONE_PROPERTY =
    "com.healthmarketscience.jackcess.timeZone";

  /** system property prefix which can be used to set the default Charset
   *  used for text data (full property includes the JetFormat version).
   * @usage _general_field_
   */
  public static final String CHARSET_PROPERTY_PREFIX =
    "com.healthmarketscience.jackcess.charset.";

  /** system property which can be used to set the path from which classpath
   *  resources are loaded (must end with a "/" if non-empty).  Default value
   *  is {@link #DEFAULT_RESOURCE_PATH} if unspecified.
   * @usage _general_field_
   */
  public static final String RESOURCE_PATH_PROPERTY = 
    "com.healthmarketscience.jackcess.resourcePath";

  /** (boolean) system property which can be used to indicate that the current
   *  vm has a poor nio implementation (specifically for
   *  FileChannel.transferFrom)
   * @usage _intermediate_field_
   */
  public static final String BROKEN_NIO_PROPERTY = 
    "com.healthmarketscience.jackcess.brokenNio";

  /** system property which can be used to set the default sort order for
   *  table columns.  Value should be one {@link Table.ColumnOrder} enum
   *  values.
   * @usage _intermediate_field_
   */
  public static final String COLUMN_ORDER_PROPERTY = 
    "com.healthmarketscience.jackcess.columnOrder";

  /**
   * default error handler used if none provided (just rethrows exception)
   * @usage _general_field_
   */
  public static final ErrorHandler DEFAULT_ERROR_HANDLER = new ErrorHandler() {
      public Object handleRowError(Column column,
                                   byte[] columnData,
                                   Table.RowState rowState,
                                   Exception error)
        throws IOException
      {
        // really can only be RuntimeException or IOException
        if(error instanceof IOException) {
          throw (IOException)error;
        }
        throw (RuntimeException)error;
      }
    };

  /**
   * default link resolver used if none provided
   * @usage _general_field_
   */
  public static final LinkResolver DEFAULT_LINK_RESOLVER = new LinkResolver() {
      public Database resolveLinkedDatabase(Database linkerDb, 
                                            String linkeeFileName)
        throws IOException
      {
        return Database.open(new File(linkeeFileName));
      }
    };

  /** the resource path to be used when loading classpath resources */
  static final String RESOURCE_PATH = 
    System.getProperty(RESOURCE_PATH_PROPERTY, DEFAULT_RESOURCE_PATH);

  /** whether or not this jvm has "broken" nio support */
  static final boolean BROKEN_NIO = Boolean.TRUE.toString().equalsIgnoreCase(
      System.getProperty(BROKEN_NIO_PROPERTY));
  
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
  private static final String CAT_COL_PROPS = "LvProp";
  /** System catalog column name of the remote database */
  private static final String CAT_COL_DATABASE = "Database";
  /** System catalog column name of the remote table name */
  private static final String CAT_COL_FOREIGN_NAME = "ForeignName";

  /** top-level parentid for a database */
  private static final int DB_PARENT_ID = 0xF000000;

  /** the maximum size of any of the included "empty db" resources */
  private static final long MAX_EMPTYDB_SIZE = 350000L;

  /** this object is a "system" object */
  static final int SYSTEM_OBJECT_FLAG = 0x80000000;
  /** this object is another type of "system" object */
  static final int ALT_SYSTEM_OBJECT_FLAG = 0x02;
  /** this object is hidden */
  static final int HIDDEN_OBJECT_FLAG = 0x08;
  /** all flags which seem to indicate some type of system object */
  static final int SYSTEM_OBJECT_FLAGS = 
    SYSTEM_OBJECT_FLAG | ALT_SYSTEM_OBJECT_FLAG;

  /**
   * Enum which indicates which version of Access created the database.
   * @usage _general_class_
   */
  public static enum FileFormat {

    V1997(null, JetFormat.VERSION_3),
    V2000(RESOURCE_PATH + "empty.mdb", JetFormat.VERSION_4),
    V2003(RESOURCE_PATH + "empty2003.mdb", JetFormat.VERSION_4),
    V2007(RESOURCE_PATH + "empty2007.accdb", JetFormat.VERSION_12, ".accdb"),
    V2010(RESOURCE_PATH + "empty2010.accdb", JetFormat.VERSION_14, ".accdb"),
    MSISAM(null, JetFormat.VERSION_MSISAM, ".mny");

    private final String _emptyFile;
    private final JetFormat _format;
    private final String _ext;

    private FileFormat(String emptyDBFile, JetFormat jetFormat) {
      this(emptyDBFile, jetFormat, ".mdb");
    }

    private FileFormat(String emptyDBFile, JetFormat jetFormat, String ext) {
      _emptyFile = emptyDBFile;
      _format = jetFormat;
      _ext = ext;
    }

    public JetFormat getJetFormat() { return _format; }

    public String getFileExtension() { return _ext; }

    @Override
    public String toString() { return name() + ", jetFormat: " + getJetFormat(); }
  }

  /** Prefix for column or table names that are reserved words */
  private static final String ESCAPE_PREFIX = "x";
  /** Name of the system object that is the parent of all tables */
  private static final String SYSTEM_OBJECT_NAME_TABLES = "Tables";
  /** Name of the system object that is the parent of all databases */
  private static final String SYSTEM_OBJECT_NAME_DATABASES = "Databases";
  /** Name of the system object that is the parent of all relationships */
  private static final String SYSTEM_OBJECT_NAME_RELATIONSHIPS = 
    "Relationships";
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
  /** System object type for query definitions */
  private static final Short TYPE_QUERY = 5;
  /** System object type for linked table definitions */
  private static final Short TYPE_LINKED_TABLE = 6;

  /** max number of table lookups to cache */
  private static final int MAX_CACHED_LOOKUP_TABLES = 50;

  /** the columns to read when reading system catalog normally */
  private static Collection<String> SYSTEM_CATALOG_COLUMNS =
    new HashSet<String>(Arrays.asList(CAT_COL_NAME, CAT_COL_TYPE, CAT_COL_ID,
                                      CAT_COL_FLAGS, CAT_COL_DATABASE, 
                                      CAT_COL_FOREIGN_NAME));
  /** the columns to read when finding table names */
  private static Collection<String> SYSTEM_CATALOG_TABLE_NAME_COLUMNS =
    new HashSet<String>(Arrays.asList(CAT_COL_NAME, CAT_COL_TYPE, CAT_COL_ID, 
                                      CAT_COL_FLAGS, CAT_COL_PARENT_ID));
  /** the columns to read when getting object propertyes */
  private static Collection<String> SYSTEM_CATALOG_PROPS_COLUMNS =
    new HashSet<String>(Arrays.asList(CAT_COL_ID, CAT_COL_PROPS));
  
  
  /**
   * All of the reserved words in Access that should be escaped when creating
   * table or column names
   */
  private static final Set<String> RESERVED_WORDS = new HashSet<String>();
  static {
    //Yup, there's a lot.
    RESERVED_WORDS.addAll(Arrays.asList(
       "add", "all", "alphanumeric", "alter", "and", "any", "application", "as",
       "asc", "assistant", "autoincrement", "avg", "between", "binary", "bit",
       "boolean", "by", "byte", "char", "character", "column", "compactdatabase",
       "constraint", "container", "count", "counter", "create", "createdatabase",
       "createfield", "creategroup", "createindex", "createobject", "createproperty",
       "createrelation", "createtabledef", "createuser", "createworkspace",
       "currency", "currentuser", "database", "date", "datetime", "delete",
       "desc", "description", "disallow", "distinct", "distinctrow", "document",
       "double", "drop", "echo", "else", "end", "eqv", "error", "exists", "exit",
       "false", "field", "fields", "fillcache", "float", "float4", "float8",
       "foreign", "form", "forms", "from", "full", "function", "general",
       "getobject", "getoption", "gotopage", "group", "group by", "guid", "having",
       "idle", "ieeedouble", "ieeesingle", "if", "ignore", "imp", "in", "index",
       "indexes", "inner", "insert", "inserttext", "int", "integer", "integer1",
       "integer2", "integer4", "into", "is", "join", "key", "lastmodified", "left",
       "level", "like", "logical", "logical1", "long", "longbinary", "longtext",
       "macro", "match", "max", "min", "mod", "memo", "module", "money", "move",
       "name", "newpassword", "no", "not", "null", "number", "numeric", "object",
       "oleobject", "off", "on", "openrecordset", "option", "or", "order", "outer",
       "owneraccess", "parameter", "parameters", "partial", "percent", "pivot",
       "primary", "procedure", "property", "queries", "query", "quit", "real",
       "recalc", "recordset", "references", "refresh", "refreshlink",
       "registerdatabase", "relation", "repaint", "repairdatabase", "report",
       "reports", "requery", "right", "screen", "section", "select", "set",
       "setfocus", "setoption", "short", "single", "smallint", "some", "sql",
       "stdev", "stdevp", "string", "sum", "table", "tabledef", "tabledefs",
       "tableid", "text", "time", "timestamp", "top", "transform", "true", "type",
       "union", "unique", "update", "user", "value", "values", "var", "varp",
       "varbinary", "varchar", "where", "with", "workspace", "xor", "year", "yes",
       "yesno"
    ));
  }

  /** the File of the database */
  private final File _file;
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
    new LinkedHashMap<String, TableInfo>() {
    private static final long serialVersionUID = 0L;
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, TableInfo> e) {
      return(size() > MAX_CACHED_LOOKUP_TABLES);
    }
  };
  /** set of table names as stored in the mdb file, created on demand */
  private Set<String> _tableNames;
  /** Reads and writes database pages */
  private final PageChannel _pageChannel;
  /** System catalog table */
  private Table _systemCatalog;
  /** utility table finder */
  private TableFinder _tableFinder;
  /** System access control entries table (initialized on first use) */
  private Table _accessControlEntries;
  /** System relationships table (initialized on first use) */
  private Table _relationships;
  /** System queries table (initialized on first use) */
  private Table _queries;
  /** System complex columns table (initialized on first use) */
  private Table _complexCols;
  /** SIDs to use for the ACEs added for new tables */
  private final List<byte[]> _newTableSIDs = new ArrayList<byte[]>();
  /** "big index support" is optional, but enabled by default */
  private Boolean _useBigIndex;
  /** optional error handler to use when row errors are encountered */
  private ErrorHandler _dbErrorHandler;
  /** the file format of the database */
  private FileFormat _fileFormat;
  /** charset to use when handling text */
  private Charset _charset;
  /** timezone to use when handling dates */
  private TimeZone _timeZone;
  /** language sort order to be used for textual columns */
  private Column.SortOrder _defaultSortOrder;
  /** default code page to be used for textual columns (in some dbs) */
  private Short _defaultCodePage;
  /** the ordering used for table columns */
  private Table.ColumnOrder _columnOrder;
  /** cache of in-use tables */
  private final TableCache _tableCache = new TableCache();
  /** handler for reading/writing properteies */
  private PropertyMaps.Handler _propsHandler;
  /** ID of the Databases system object */
  private Integer _dbParentId;
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


  /**
   * Open an existing Database.  If the existing file is not writeable, the
   * file will be opened read-only.  Auto-syncing is enabled for the returned
   * Database.
   * <p>
   * Equivalent to:
   * {@code  open(mdbFile, false);}
   * 
   * @param mdbFile File containing the database
   * 
   * @see #open(File,boolean)
   * @usage _general_method_
   */
  public static Database open(File mdbFile) throws IOException {
    return open(mdbFile, false);
  }
  
  /**
   * Open an existing Database.  If the existing file is not writeable or the
   * readOnly flag is <code>true</code>, the file will be opened read-only.
   * Auto-syncing is enabled for the returned Database.
   * <p>
   * Equivalent to:
   * {@code  open(mdbFile, readOnly, DEFAULT_AUTO_SYNC);}
   * 
   * @param mdbFile File containing the database
   * @param readOnly iff <code>true</code>, force opening file in read-only
   *                 mode
   *
   * @see #open(File,boolean,boolean)
   * @usage _general_method_
   */
  public static Database open(File mdbFile, boolean readOnly)
    throws IOException
  {
    return open(mdbFile, readOnly, DEFAULT_AUTO_SYNC);
  }
  
  /**
   * Open an existing Database.  If the existing file is not writeable or the
   * readOnly flag is <code>true</code>, the file will be opened read-only.
   * @param mdbFile File containing the database
   * @param readOnly iff <code>true</code>, force opening file in read-only
   *                 mode
   * @param autoSync whether or not to enable auto-syncing on write.  if
   *                 {@code true}, writes will be immediately flushed to disk.
   *                 This leaves the database in a (fairly) consistent state
   *                 on each write, but can be very inefficient for many
   *                 updates.  if {@code false}, flushing to disk happens at
   *                 the jvm's leisure, which can be much faster, but may
   *                 leave the database in an inconsistent state if failures
   *                 are encountered during writing.
   * @usage _general_method_
   */
  public static Database open(File mdbFile, boolean readOnly, boolean autoSync)
    throws IOException
  {    
    return open(mdbFile, readOnly, autoSync, null, null);
  }

  /**
   * Open an existing Database.  If the existing file is not writeable or the
   * readOnly flag is <code>true</code>, the file will be opened read-only.
   * @param mdbFile File containing the database
   * @param readOnly iff <code>true</code>, force opening file in read-only
   *                 mode
   * @param autoSync whether or not to enable auto-syncing on write.  if
   *                 {@code true}, writes will be immediately flushed to disk.
   *                 This leaves the database in a (fairly) consistent state
   *                 on each write, but can be very inefficient for many
   *                 updates.  if {@code false}, flushing to disk happens at
   *                 the jvm's leisure, which can be much faster, but may
   *                 leave the database in an inconsistent state if failures
   *                 are encountered during writing.
   * @param charset Charset to use, if {@code null}, uses default
   * @param timeZone TimeZone to use, if {@code null}, uses default
   * @usage _intermediate_method_
   */
  public static Database open(File mdbFile, boolean readOnly, boolean autoSync,
                              Charset charset, TimeZone timeZone)
    throws IOException
  {    
    return open(mdbFile, readOnly, autoSync, charset, timeZone, null);
  }

  /**
   * Open an existing Database.  If the existing file is not writeable or the
   * readOnly flag is <code>true</code>, the file will be opened read-only.
   * @param mdbFile File containing the database
   * @param readOnly iff <code>true</code>, force opening file in read-only
   *                 mode
   * @param autoSync whether or not to enable auto-syncing on write.  if
   *                 {@code true}, writes will be immediately flushed to disk.
   *                 This leaves the database in a (fairly) consistent state
   *                 on each write, but can be very inefficient for many
   *                 updates.  if {@code false}, flushing to disk happens at
   *                 the jvm's leisure, which can be much faster, but may
   *                 leave the database in an inconsistent state if failures
   *                 are encountered during writing.
   * @param charset Charset to use, if {@code null}, uses default
   * @param timeZone TimeZone to use, if {@code null}, uses default
   * @param provider CodecProvider for handling page encoding/decoding, may be
   *                 {@code null} if no special encoding is necessary
   * @usage _intermediate_method_
   */
  public static Database open(File mdbFile, boolean readOnly, boolean autoSync,
                              Charset charset, TimeZone timeZone, 
                              CodecProvider provider)
    throws IOException
  {
    if(!mdbFile.exists() || !mdbFile.canRead()) {
      throw new FileNotFoundException("given file does not exist: " + mdbFile);
    }

    // force read-only for non-writable files
    readOnly |= !mdbFile.canWrite();

    // open file channel
    FileChannel channel = openChannel(mdbFile, readOnly);

    if(!readOnly) {

      // verify that format supports writing
      JetFormat jetFormat = JetFormat.getFormat(channel);

      if(jetFormat.READ_ONLY) {
        // shutdown the channel (quietly)
        try {
          channel.close();
        } catch(Exception ignored) {
          // we don't care
        }
        throw new IOException("jet format '" + jetFormat + "' does not support writing");
      }
    }

    return new Database(mdbFile, channel, autoSync, null, charset, timeZone, 
                        provider);
  }
  
  /**
   * Create a new Access 2000 Database 
   * <p>
   * Equivalent to:
   * {@code  create(FileFormat.V2000, mdbFile, DEFAULT_AUTO_SYNC);}
   * 
   * @param mdbFile Location to write the new database to.  <b>If this file
   *    already exists, it will be overwritten.</b>
   *
   * @see #create(File,boolean)
   * @usage _general_method_
   */
  public static Database create(File mdbFile) throws IOException {
    return create(mdbFile, DEFAULT_AUTO_SYNC);
  }
  
  /**
   * Create a new Database for the given fileFormat
   * <p>
   * Equivalent to:
   * {@code  create(fileFormat, mdbFile, DEFAULT_AUTO_SYNC);}
   * 
   * @param fileFormat version of new database.
   * @param mdbFile Location to write the new database to.  <b>If this file
   *    already exists, it will be overwritten.</b>
   *
   * @see #create(File,boolean)
   * @usage _general_method_
   */
  public static Database create(FileFormat fileFormat, File mdbFile) 
    throws IOException 
  {
    return create(fileFormat, mdbFile, DEFAULT_AUTO_SYNC);
  }
  
  /**
   * Create a new Access 2000 Database
   * <p>
   * Equivalent to:
   * {@code  create(FileFormat.V2000, mdbFile, DEFAULT_AUTO_SYNC);}
   * 
   * @param mdbFile Location to write the new database to.  <b>If this file
   *    already exists, it will be overwritten.</b>
   * @param autoSync whether or not to enable auto-syncing on write.  if
   *                 {@code true}, writes will be immediately flushed to disk.
   *                 This leaves the database in a (fairly) consistent state
   *                 on each write, but can be very inefficient for many
   *                 updates.  if {@code false}, flushing to disk happens at
   *                 the jvm's leisure, which can be much faster, but may
   *                 leave the database in an inconsistent state if failures
   *                 are encountered during writing.
   * @usage _general_method_
   */
  public static Database create(File mdbFile, boolean autoSync)
    throws IOException
  {
    return create(FileFormat.V2000, mdbFile, autoSync);
  }

  /**
   * Create a new Database for the given fileFormat
   * @param fileFormat version of new database.
   * @param mdbFile Location to write the new database to.  <b>If this file
   *    already exists, it will be overwritten.</b>
   * @param autoSync whether or not to enable auto-syncing on write.  if
   *                 {@code true}, writes will be immediately flushed to disk.
   *                 This leaves the database in a (fairly) consistent state
   *                 on each write, but can be very inefficient for many
   *                 updates.  if {@code false}, flushing to disk happens at
   *                 the jvm's leisure, which can be much faster, but may
   *                 leave the database in an inconsistent state if failures
   *                 are encountered during writing.
   * @usage _general_method_
   */
  public static Database create(FileFormat fileFormat, File mdbFile, 
                                boolean autoSync)
    throws IOException
  {
    return create(fileFormat, mdbFile, autoSync, null, null);
  }

  /**
   * Create a new Database for the given fileFormat
   * @param fileFormat version of new database.
   * @param mdbFile Location to write the new database to.  <b>If this file
   *    already exists, it will be overwritten.</b>
   * @param autoSync whether or not to enable auto-syncing on write.  if
   *                 {@code true}, writes will be immediately flushed to disk.
   *                 This leaves the database in a (fairly) consistent state
   *                 on each write, but can be very inefficient for many
   *                 updates.  if {@code false}, flushing to disk happens at
   *                 the jvm's leisure, which can be much faster, but may
   *                 leave the database in an inconsistent state if failures
   *                 are encountered during writing.
   * @param charset Charset to use, if {@code null}, uses default
   * @param timeZone TimeZone to use, if {@code null}, uses default
   * @usage _intermediate_method_
   */
  public static Database create(FileFormat fileFormat, File mdbFile, 
                                boolean autoSync, Charset charset,
                                TimeZone timeZone)
    throws IOException
  {
    if (fileFormat.getJetFormat().READ_ONLY) {
      throw new IOException("jet format '" + fileFormat.getJetFormat() + "' does not support writing");
    }

    FileChannel channel = openChannel(mdbFile, false);
    channel.truncate(0);
    transferFrom(channel, getResourceAsStream(fileFormat._emptyFile));
    channel.force(true);
    return new Database(mdbFile, channel, autoSync, fileFormat, charset, 
                        timeZone, null);
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
  static FileChannel openChannel(final File mdbFile, final boolean readOnly)
    throws FileNotFoundException
  {
    final String mode = (readOnly ? "r" : "rw");
    return new RandomAccessFile(mdbFile, mode).getChannel();
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
   *                 are encountered during writing.
   * @param fileFormat version of new database (if known)
   * @param charset Charset to use, if {@code null}, uses default
   * @param timeZone TimeZone to use, if {@code null}, uses default
   */
  protected Database(File file, FileChannel channel, boolean autoSync,
                     FileFormat fileFormat, Charset charset, TimeZone timeZone,
                     CodecProvider provider)
    throws IOException
  {
    boolean success = false;
    try {
      _file = file;
      _format = JetFormat.getFormat(channel);
      _charset = ((charset == null) ? getDefaultCharset(_format) : charset);
      _columnOrder = getDefaultColumnOrder();
      _fileFormat = fileFormat;
      _pageChannel = new PageChannel(channel, _format, autoSync);
      _timeZone = ((timeZone == null) ? getDefaultTimeZone() : timeZone);
      if(provider == null) {
        provider = DefaultCodecProvider.INSTANCE;
      }
      // note, it's slighly sketchy to pass ourselves along partially
      // constructed, but only our _format and _pageChannel refs should be
      // needed
      _pageChannel.initialize(this, provider);
      _buffer = _pageChannel.createPageBuffer();
      readSystemCatalog();
      success = true;

    } finally {
      if(!success && (channel != null)) {
        // something blew up, shutdown the channel (quietly)
        try {
          channel.close();
        } catch(Exception ignored) {
          // we don't care
        }
      }
    }
  }

  /**
   * Returns the File underlying this Database
   */
  public File getFile() {
    return _file;
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
  public Table getSystemCatalog() {
    return _systemCatalog;
  }
  
  /**
   * @return The system Access Control Entries table (loaded on demand)
   * @usage _advanced_method_
   */
  public Table getAccessControlEntries() throws IOException {
    if(_accessControlEntries == null) {
      _accessControlEntries = getSystemTable(TABLE_SYSTEM_ACES);
      if(_accessControlEntries == null) {
        throw new IOException("Could not find system table " +
                              TABLE_SYSTEM_ACES);
      }

    }
    return _accessControlEntries;
  }

  /**
   * @return the complex column system table (loaded on demand)
   * @usage _advanced_method_
   */
  public Table getSystemComplexColumns() throws IOException {
    if(_complexCols == null) {
      _complexCols = getSystemTable(TABLE_SYSTEM_COMPLEX_COLS);
      if(_complexCols == null) {
        throw new IOException("Could not find system table " +
                              TABLE_SYSTEM_COMPLEX_COLS);
      }
    }
    return _complexCols;
  }
    
  /**
   * Whether or not big index support is enabled for tables.
   * @usage _advanced_method_
   */
  public boolean doUseBigIndex() {
    return (_useBigIndex != null ? _useBigIndex : true);
  }

  /**
   * Set whether or not big index support is enabled for tables.
   * @usage _intermediate_method_
   */
  public void setUseBigIndex(boolean useBigIndex) {
    _useBigIndex = useBigIndex;
  }

  /**
   * Gets the currently configured ErrorHandler (always non-{@code null}).
   * This will be used to handle all errors unless overridden at the Table or
   * Cursor level.
   * @usage _intermediate_method_
   */
  public ErrorHandler getErrorHandler() {
    return((_dbErrorHandler != null) ? _dbErrorHandler :
           DEFAULT_ERROR_HANDLER);
  }

  /**
   * Sets a new ErrorHandler.  If {@code null}, resets to the
   * {@link #DEFAULT_ERROR_HANDLER}.
   * @usage _intermediate_method_
   */
  public void setErrorHandler(ErrorHandler newErrorHandler) {
    _dbErrorHandler = newErrorHandler;
  }    

  /**
   * Gets the currently configured LinkResolver (always non-{@code null}).
   * This will be used to handle all linked database loading.
   * @usage _intermediate_method_
   */
  public LinkResolver getLinkResolver() {
    return((_linkResolver != null) ? _linkResolver : DEFAULT_LINK_RESOLVER);
  }

  /**
   * Sets a new LinkResolver.  If {@code null}, resets to the
   * {@link #DEFAULT_LINK_RESOLVER}.
   * @usage _intermediate_method_
   */
  public void setLinkResolver(LinkResolver newLinkResolver) {
    _linkResolver = newLinkResolver;
  }    

  /**
   * Returns an unmodifiable view of the currently loaded linked databases,
   * mapped from the linked database file name to the linked database.  This
   * information may be useful for implementing a LinkResolver.
   * @usage _intermediate_method_
   */
  public Map<String,Database> getLinkedDatabases() {
    return ((_linkedDbs == null) ? Collections.<String,Database>emptyMap() : 
            Collections.unmodifiableMap(_linkedDbs));
  }

  /**
   * Gets currently configured TimeZone (always non-{@code null}).
   * @usage _intermediate_method_
   */
  public TimeZone getTimeZone() {
    return _timeZone;
  }

  /**
   * Sets a new TimeZone.  If {@code null}, resets to the value returned by
   * {@link #getDefaultTimeZone}.
   * @usage _intermediate_method_
   */
  public void setTimeZone(TimeZone newTimeZone) {
    if(newTimeZone == null) {
      newTimeZone = getDefaultTimeZone();
    }
    _timeZone = newTimeZone;
  }    

  /**
   * Gets currently configured Charset (always non-{@code null}).
   * @usage _intermediate_method_
   */
  public Charset getCharset()
  {
    return _charset;
  }

  /**
   * Sets a new Charset.  If {@code null}, resets to the value returned by
   * {@link #getDefaultCharset}.
   * @usage _intermediate_method_
   */
  public void setCharset(Charset newCharset) {
    if(newCharset == null) {
      newCharset = getDefaultCharset(getFormat());
    }
    _charset = newCharset;
  }

  /**
   * Gets currently configured {@link Table.ColumnOrder} (always non-{@code
   * null}).
   * @usage _intermediate_method_
   */
  public Table.ColumnOrder getColumnOrder() {
    return _columnOrder;
  }

  /**
   * Sets a new Table.ColumnOrder.  If {@code null}, resets to the value
   * returned by {@link #getDefaultColumnOrder}.
   * @usage _intermediate_method_
   */
  public void setColumnOrder(Table.ColumnOrder newColumnOrder) {
    if(newColumnOrder == null) {
      newColumnOrder = getDefaultColumnOrder();
    }
    _columnOrder = newColumnOrder;
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

  /**
   * Returns the FileFormat of this database (which may involve inspecting the
   * database itself).
   * @throws IllegalStateException if the file format cannot be determined
   * @usage _general_method_
   */
  public FileFormat getFileFormat() throws IOException {

    if(_fileFormat == null) {

      Map<String,Database.FileFormat> possibleFileFormats =
        getFormat().getPossibleFileFormats();

      if(possibleFileFormats.size() == 1) {

        // single possible format (null key), easy enough
        _fileFormat = possibleFileFormats.get(null);

      } else {

        // need to check the "AccessVersion" property
        String accessVersion = (String)getDatabaseProperties().getValue(
            PropertyMap.ACCESS_VERSION_PROP);
        
        _fileFormat = possibleFileFormats.get(accessVersion);
        
        if(_fileFormat == null) {
          throw new IllegalStateException("Could not determine FileFormat");
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
  public Column.SortOrder getDefaultSortOrder() throws IOException {

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
      _pageChannel.readPage(buffer, 0);
      _defaultSortOrder = Column.readSortOrder(
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
  public PropertyMaps readProperties(byte[] propsBytes, int objectId)
    throws IOException 
  {
    return getPropsHandler().read(propsBytes, objectId);
  }
  
  /**
   * Read the system catalog
   */
  private void readSystemCatalog() throws IOException {
    _systemCatalog = readTable(TABLE_SYSTEM_CATALOG, PAGE_SYSTEM_CATALOG,
                               SYSTEM_OBJECT_FLAGS, defaultUseBigIndex());

    try {
      _tableFinder = new DefaultTableFinder(
          new CursorBuilder(_systemCatalog)
            .setIndexByColumnNames(CAT_COL_PARENT_ID, CAT_COL_NAME)
            .setColumnMatcher(CaseInsensitiveColumnMatcher.INSTANCE)
            .toIndexCursor());
    } catch(IllegalArgumentException e) {
      LOG.info("Could not find expected index on table " + 
               _systemCatalog.getName());
      // use table scan instead
      _tableFinder = new FallbackTableFinder(
          new CursorBuilder(_systemCatalog)
            .setColumnMatcher(CaseInsensitiveColumnMatcher.INSTANCE)
            .toCursor());
    }

    _tableParentId = _tableFinder.findObjectId(DB_PARENT_ID, 
                                               SYSTEM_OBJECT_NAME_TABLES);

    if(_tableParentId == null) {  
      throw new IOException("Did not find required parent table id");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished reading system catalog.  Tables: " +
                getTableNames());
    }
  }
  
  /**
   * @return The names of all of the user tables (String)
   * @usage _general_method_
   */
  public Set<String> getTableNames() throws IOException {
    if(_tableNames == null) {
      Set<String> tableNames =
        new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
      _tableFinder.getTableNames(tableNames, false);
      _tableNames = tableNames;
    }
    return _tableNames;
  }

  /**
   * @return The names of all of the system tables (String).  Note, in order
   *         to read these tables, you must use {@link #getSystemTable}.
   *         <i>Extreme care should be taken if modifying these tables
   *         directly!</i>.
   * @usage _intermediate_method_
   */
  public Set<String> getSystemTableNames() throws IOException {
    Set<String> sysTableNames =
      new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    _tableFinder.getTableNames(sysTableNames, true);
    return sysTableNames;
  }

  /**
   * @return an unmodifiable Iterator of the user Tables in this Database.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   * @throws ConcurrentModificationException if a table is added to the
   *         database while an Iterator is in use.
   * @usage _general_method_
   */
  public Iterator<Table> iterator() {
    return new TableIterator();
  }

  /**
   * @param name Table name
   * @return The table, or null if it doesn't exist
   * @usage _general_method_
   */
  public Table getTable(String name) throws IOException {
    return getTable(name, defaultUseBigIndex());
  }
  
  /**
   * @param name Table name
   * @param useBigIndex whether or not "big index support" should be enabled
   *                    for the table (this value will override any other
   *                    settings)
   * @return The table, or null if it doesn't exist
   * @usage _intermediate_method_
   */
  public Table getTable(String name, boolean useBigIndex) throws IOException {
    return getTable(name, false, useBigIndex);
  }

  /**
   * @param tableDefPageNumber the page number of a table definition
   * @return The table, or null if it doesn't exist
   * @usage _advanced_method_
   */
  public Table getTable(int tableDefPageNumber) throws IOException {

    // first, check for existing table
    Table table = _tableCache.get(tableDefPageNumber);
    if(table != null) {
      return table;
    }
    
    // lookup table info from system catalog
    Map<String,Object> objectRow = _tableFinder.getObjectRow(
        tableDefPageNumber, SYSTEM_CATALOG_COLUMNS);
    if(objectRow == null) {
      return null;
    }

    String name = (String)objectRow.get(CAT_COL_NAME);
    int flags = (Integer)objectRow.get(CAT_COL_FLAGS);

    return readTable(name, tableDefPageNumber, flags, defaultUseBigIndex());
  }

  /**
   * @param name Table name
   * @param includeSystemTables whether to consider returning a system table
   * @param useBigIndex whether or not "big index support" should be enabled
   *                    for the table (this value will override any other
   *                    settings)
   * @return The table, or null if it doesn't exist
   */
  private Table getTable(String name, boolean includeSystemTables, 
                         boolean useBigIndex) 
    throws IOException 
  {
    TableInfo tableInfo = lookupTable(name);
    
    if ((tableInfo == null) || (tableInfo.pageNumber == null)) {
      return null;
    }
    if(!includeSystemTables && isSystemObject(tableInfo.flags)) {
      return null;
    }

    if(tableInfo.isLinked()) {

      if(_linkedDbs == null) {
        _linkedDbs = new HashMap<String,Database>();
      }

      String linkedDbName = ((LinkedTableInfo)tableInfo).linkedDbName;
      String linkedTableName = ((LinkedTableInfo)tableInfo).linkedTableName;
      Database linkedDb = _linkedDbs.get(linkedDbName);
      if(linkedDb == null) {
        linkedDb = getLinkResolver().resolveLinkedDatabase(this, linkedDbName);
        _linkedDbs.put(linkedDbName, linkedDb);
      }
      
      return linkedDb.getTable(linkedTableName, includeSystemTables, 
                               useBigIndex);
    }

    return readTable(tableInfo.tableName, tableInfo.pageNumber,
                     tableInfo.flags, useBigIndex);
  }
  
  /**
   * Create a new table in this database
   * @param name Name of the table to create
   * @param columns List of Columns in the table
   * @usage _general_method_
   */
  public void createTable(String name, List<Column> columns)
    throws IOException
  {
    createTable(name, columns, null);
  }

  /**
   * Create a new table in this database
   * @param name Name of the table to create
   * @param columns List of Columns in the table
   * @param indexes List of IndexBuilders describing indexes for the table
   * @usage _general_method_
   */
  public void createTable(String name, List<Column> columns,
                          List<IndexBuilder> indexes)
    throws IOException
  {
    if(lookupTable(name) != null) {
      throw new IllegalArgumentException(
          "Cannot create table with name of existing table");
    }

    new TableCreator(this, name, columns, indexes).createTable();
  }

  /**
   * Create a new table in this database
   * @param name Name of the table to create
   * @usage _general_method_
   */
  public void createLinkedTable(String name, String linkedDbName, 
                                String linkedTableName)
    throws IOException
  {
    if(lookupTable(name) != null) {
      throw new IllegalArgumentException(
          "Cannot create linked table with name of existing table");
    }

    validateIdentifierName(name, getFormat().MAX_TABLE_NAME_LENGTH, "table");
    validateIdentifierName(linkedDbName, DataType.MEMO.getMaxSize(), 
                           "linked database");
    validateIdentifierName(linkedTableName, getFormat().MAX_TABLE_NAME_LENGTH, 
                           "linked table");

    int linkedTableId = _tableFinder.getNextFreeSyntheticId();

    addNewTable(name, linkedTableId, TYPE_LINKED_TABLE, linkedDbName, 
                linkedTableName);
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
                       linkedTableName);
    addToAccessControlEntries(tdefPageNumber);
  }

  /**
   * Finds all the relationships in the database between the given tables.
   * @usage _intermediate_method_
   */
  public List<Relationship> getRelationships(Table table1, Table table2)
    throws IOException
  {
    // the relationships table does not get loaded until first accessed
    if(_relationships == null) {
      _relationships = getSystemTable(TABLE_SYSTEM_RELATIONSHIPS);
      if(_relationships == null) {
        throw new IOException("Could not find system relationships table");
      }
    }

    int nameCmp = table1.getName().compareTo(table2.getName());
    if(nameCmp == 0) {
      throw new IllegalArgumentException("Must provide two different tables");
    }
    if(nameCmp > 0) {
      // we "order" the two tables given so that we will return a collection
      // of relationships in the same order regardless of whether we are given
      // (TableFoo, TableBar) or (TableBar, TableFoo).
      Table tmp = table1;
      table1 = table2;
      table2 = tmp;
    }
      

    List<Relationship> relationships = new ArrayList<Relationship>();
    Cursor cursor = createCursorWithOptionalIndex(
        _relationships, REL_COL_FROM_TABLE, table1.getName());
    collectRelationships(cursor, table1, table2, relationships);
    cursor = createCursorWithOptionalIndex(
        _relationships, REL_COL_TO_TABLE, table1.getName());
    collectRelationships(cursor, table2, table1, relationships);
    
    return relationships;
  }

  /**
   * Finds all the queries in the database.
   * @usage _intermediate_method_
   */
  public List<Query> getQueries()
    throws IOException
  {
    // the queries table does not get loaded until first accessed
    if(_queries == null) {
      _queries = getSystemTable(TABLE_SYSTEM_QUERIES);
      if(_queries == null) {
        throw new IOException("Could not find system queries table");
      }
    }

    // find all the queries from the system catalog
    List<Map<String,Object>> queryInfo = new ArrayList<Map<String,Object>>();
    Map<Integer,List<Query.Row>> queryRowMap = 
      new HashMap<Integer,List<Query.Row>>();
    for(Map<String,Object> row :
          Cursor.createCursor(_systemCatalog).iterable(SYSTEM_CATALOG_COLUMNS))
    {
      String name = (String) row.get(CAT_COL_NAME);
      if (name != null && TYPE_QUERY.equals(row.get(CAT_COL_TYPE))) {
        queryInfo.add(row);
        Integer id = (Integer)row.get(CAT_COL_ID);
        queryRowMap.put(id, new ArrayList<Query.Row>());
      }
    }

    // find all the query rows
    for(Map<String,Object> row : Cursor.createCursor(_queries)) {
      Query.Row queryRow = new Query.Row(row);
      List<Query.Row> queryRows = queryRowMap.get(queryRow.objectId);
      if(queryRows == null) {
        LOG.warn("Found rows for query with id " + queryRow.objectId +
                 " missing from system catalog");
        continue;
      }
      queryRows.add(queryRow);
    }

    // lastly, generate all the queries
    List<Query> queries = new ArrayList<Query>();
    for(Map<String,Object> row : queryInfo) {
      String name = (String) row.get(CAT_COL_NAME);
      Integer id = (Integer)row.get(CAT_COL_ID);
      int flags = (Integer)row.get(CAT_COL_FLAGS);
      List<Query.Row> queryRows = queryRowMap.get(id);
      queries.add(Query.create(flags, name, queryRows, id));
    }

    return queries;
  }

  /**
   * Returns a reference to <i>any</i> available table in this access
   * database, including system tables.
   * <p>
   * Warning, this method is not designed for common use, only for the
   * occassional time when access to a system table is necessary.  Messing
   * with system tables can strip the paint off your house and give your whole
   * family a permanent, orange afro.  You have been warned.
   * 
   * @param tableName Table name, may be a system table
   * @return The table, or {@code null} if it doesn't exist
   * @usage _intermediate_method_
   */
  public Table getSystemTable(String tableName)
    throws IOException
  {
    return getTable(tableName, true, defaultUseBigIndex());
  }

  /**
   * @return the core properties for the database
   * @usage _general_method_
   */
  public PropertyMap getDatabaseProperties() throws IOException {
    if(_dbPropMaps == null) {
      _dbPropMaps = getPropertiesForDbObject(OBJECT_NAME_DB_PROPS);
    }
    return _dbPropMaps.getDefault();
  }

  /**
   * @return the summary properties for the database
   * @usage _general_method_
   */
  public PropertyMap getSummaryProperties() throws IOException {
    if(_summaryPropMaps == null) {
      _summaryPropMaps = getPropertiesForDbObject(OBJECT_NAME_SUMMARY_PROPS);
    }
    return _summaryPropMaps.getDefault();
  }

  /**
   * @return the user-defined properties for the database
   * @usage _general_method_
   */
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
  public PropertyMaps getPropertiesForObject(int objectId)
    throws IOException
  {
    Map<String,Object> objectRow = _tableFinder.getObjectRow(
        objectId, SYSTEM_CATALOG_PROPS_COLUMNS);
    byte[] propsBytes = null;
    if(objectRow != null) {
      propsBytes = (byte[])objectRow.get(CAT_COL_PROPS);
    }
    return readProperties(propsBytes, objectId);
  }

  /**
   * @return property group for the given "database" object
   */
  private PropertyMaps getPropertiesForDbObject(String dbName)
    throws IOException
  {
    if(_dbParentId == null) {
      // need the parent if of the databases objects
      _dbParentId = _tableFinder.findObjectId(DB_PARENT_ID, 
                                              SYSTEM_OBJECT_NAME_DATABASES);
      if(_dbParentId == null) {  
        throw new IOException("Did not find required parent db id");
      }
    }

    Map<String,Object> objectRow = _tableFinder.getObjectRow(
        _dbParentId, dbName, SYSTEM_CATALOG_PROPS_COLUMNS);
    byte[] propsBytes = null;
    int objectId = -1;
    if(objectRow != null) {
      propsBytes = (byte[])objectRow.get(CAT_COL_PROPS);
      objectId = (Integer)objectRow.get(CAT_COL_ID);
    }
    return readProperties(propsBytes, objectId);
  }

  /**
   * @return the current database password, or {@code null} if none set.
   * @usage _general_method_
   */
  public String getDatabasePassword() throws IOException
  {
    ByteBuffer buffer = takeSharedBuffer();
    try {
      _pageChannel.readPage(buffer, 0);

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

      String pwd = Column.decodeUncompressedText(pwdBytes, getCharset());

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
  private static void collectRelationships(
      Cursor cursor, Table fromTable, Table toTable,
      List<Relationship> relationships)
  {
    for(Map<String,Object> row : cursor) {
      String fromName = (String)row.get(REL_COL_FROM_TABLE);
      String toName = (String)row.get(REL_COL_TO_TABLE);
      
      if(fromTable.getName().equalsIgnoreCase(fromName) &&
         toTable.getName().equalsIgnoreCase(toName))
      {

        String relName = (String)row.get(REL_COL_NAME);
        
        // found more info for a relationship.  see if we already have some
        // info for this relationship
        Relationship rel = null;
        for(Relationship tmp : relationships) {
          if(tmp.getName().equalsIgnoreCase(relName)) {
            rel = tmp;
            break;
          }
        }

        if(rel == null) {
          // new relationship
          int numCols = (Integer)row.get(REL_COL_COLUMN_COUNT);
          int flags = (Integer)row.get(REL_COL_FLAGS);
          rel = new Relationship(relName, fromTable, toTable,
                                 flags, numCols);
          relationships.add(rel);
        }

        // add column info
        int colIdx = (Integer)row.get(REL_COL_COLUMN_INDEX);
        Column fromCol = fromTable.getColumn(
            (String)row.get(REL_COL_FROM_COLUMN));
        Column toCol = toTable.getColumn(
            (String)row.get(REL_COL_TO_COLUMN));

        rel.getFromColumns().set(colIdx, fromCol);
        rel.getToColumns().set(colIdx, toCol);
      }
    }    
  }
  
  /**
   * Add a new table to the system catalog
   * @param name Table name
   * @param pageNumber Page number that contains the table definition
   */
  private void addToSystemCatalog(String name, int pageNumber, Short type, 
                                  String linkedDbName, String linkedTableName)
    throws IOException
  {
    Object[] catalogRow = new Object[_systemCatalog.getColumnCount()];
    int idx = 0;
    Date creationTime = new Date();
    for (Iterator<Column> iter = _systemCatalog.getColumns().iterator();
         iter.hasNext(); idx++)
    {
      Column col = iter.next();
      if (CAT_COL_ID.equals(col.getName())) {
        catalogRow[idx] = Integer.valueOf(pageNumber);
      } else if (CAT_COL_NAME.equals(col.getName())) {
        catalogRow[idx] = name;
      } else if (CAT_COL_TYPE.equals(col.getName())) {
        catalogRow[idx] = type;
      } else if (CAT_COL_DATE_CREATE.equals(col.getName()) ||
                 CAT_COL_DATE_UPDATE.equals(col.getName())) {
        catalogRow[idx] = creationTime;
      } else if (CAT_COL_PARENT_ID.equals(col.getName())) {
        catalogRow[idx] = _tableParentId;
      } else if (CAT_COL_FLAGS.equals(col.getName())) {
        catalogRow[idx] = Integer.valueOf(0);
      } else if (CAT_COL_OWNER.equals(col.getName())) {
        byte[] owner = new byte[2];
        catalogRow[idx] = owner;
        owner[0] = (byte) 0xcf;
        owner[1] = (byte) 0x5f;
      } else if (CAT_COL_DATABASE.equals(col.getName())) {
        catalogRow[idx] = linkedDbName;
      } else if (CAT_COL_FOREIGN_NAME.equals(col.getName())) {
        catalogRow[idx] = linkedTableName;
      }
    }
    _systemCatalog.addRow(catalogRow);
  }
  
  /**
   * Add a new table to the system's access control entries
   * @param pageNumber Page number that contains the table definition
   */
  private void addToAccessControlEntries(int pageNumber) throws IOException {

    if(_newTableSIDs.isEmpty()) {
      initNewTableSIDs();
    }

    Table acEntries = getAccessControlEntries();
    Column acmCol = acEntries.getColumn(ACE_COL_ACM);
    Column inheritCol = acEntries.getColumn(ACE_COL_F_INHERITABLE);
    Column objIdCol = acEntries.getColumn(ACE_COL_OBJECT_ID);
    Column sidCol = acEntries.getColumn(ACE_COL_SID);

    // construct a collection of ACE entries mimicing those of our parent, the
    // "Tables" system object
    List<Object[]> aceRows = new ArrayList<Object[]>(_newTableSIDs.size());
    for(byte[] sid : _newTableSIDs) {
      Object[] aceRow = new Object[acEntries.getColumnCount()];
      acmCol.setRowValue(aceRow, SYS_FULL_ACCESS_ACM);
      inheritCol.setRowValue(aceRow, Boolean.FALSE);
      objIdCol.setRowValue(aceRow, Integer.valueOf(pageNumber));
      sidCol.setRowValue(aceRow, sid);
      aceRows.add(aceRow);
    }
    acEntries.addRows(aceRows);  
  }

  /**
   * Determines the collection of SIDs which need to be added to new tables.
   */
  private void initNewTableSIDs() throws IOException
  {
    // search for ACEs matching the tableParentId.  use the index on the
    // objectId column if found (should be there)
    Cursor cursor = createCursorWithOptionalIndex(
        getAccessControlEntries(), ACE_COL_OBJECT_ID, _tableParentId);
    
    for(Map<String, Object> row : cursor) {
      Integer objId = (Integer)row.get(ACE_COL_OBJECT_ID);
      if(_tableParentId.equals(objId)) {
        _newTableSIDs.add((byte[])row.get(ACE_COL_SID));
      }
    }

    if(_newTableSIDs.isEmpty()) {
      // if all else fails, use the hard-coded default
      _newTableSIDs.add(SYS_DEFAULT_SID);
    }
  }

  /**
   * Reads a table with the given name from the given pageNumber.
   */
  private Table readTable(String name, int pageNumber, int flags,
                          boolean useBigIndex)
    throws IOException
  {
    // first, check for existing table
    Table table = _tableCache.get(pageNumber);
    if(table != null) {
      return table;
    }
    
    ByteBuffer buffer = takeSharedBuffer();
    try {
      // need to load table from db
      _pageChannel.readPage(buffer, pageNumber);
      byte pageType = buffer.get(0);
      if (pageType != PageTypes.TABLE_DEF) {
        throw new IOException(
            "Looking for " + name + " at page " + pageNumber +
            ", but page type is " + pageType);
      }
      return _tableCache.put(
          new Table(this, buffer, pageNumber, name, flags, useBigIndex));
    } finally {
      releaseSharedBuffer(buffer);
    }
  }

  /**
   * Creates a Cursor restricted to the given column value if possible (using
   * an existing index), otherwise a simple table cursor.
   */
  private static Cursor createCursorWithOptionalIndex(
      Table table, String colName, Object colValue)
    throws IOException
  {
    try {
      return new CursorBuilder(table)
        .setIndexByColumns(table.getColumn(colName))
        .setSpecificEntry(colValue)
        .toCursor();
    } catch(IllegalArgumentException e) {
      LOG.info("Could not find expected index on table " + table.getName());
    }
    // use table scan instead
    return Cursor.createCursor(table);
  }
  
  /**
   * Copy an existing JDBC ResultSet into a new table in this database
   * 
   * @param name Name of the new table to create
   * @param source ResultSet to copy from
   *
   * @return the name of the copied table
   *
   * @see ImportUtil#importResultSet(ResultSet,Database,String)
   * @usage _general_method_
   */
  public String copyTable(String name, ResultSet source)
    throws SQLException, IOException
  {
    return ImportUtil.importResultSet(source, this, name);
  }
  
  /**
   * Copy an existing JDBC ResultSet into a new table in this database
   * 
   * @param name Name of the new table to create
   * @param source ResultSet to copy from
   * @param filter valid import filter
   *
   * @return the name of the imported table
   *
   * @see ImportUtil#importResultSet(ResultSet,Database,String,ImportFilter)
   * @usage _general_method_
   */
  public String copyTable(String name, ResultSet source, ImportFilter filter)
    throws SQLException, IOException
  {
    return ImportUtil.importResultSet(source, this, name, filter);
  }
  
  /**
   * Copy a delimited text file into a new table in this database
   * 
   * @param name Name of the new table to create
   * @param f Source file to import
   * @param delim Regular expression representing the delimiter string.
   *
   * @return the name of the imported table
   *
   * @see ImportUtil#importFile(File,Database,String,String)
   * @usage _general_method_
   */
  public String importFile(String name, File f, String delim)
    throws IOException
  {
    return ImportUtil.importFile(f, this, name, delim);
  }

  /**
   * Copy a delimited text file into a new table in this database
   * 
   * @param name Name of the new table to create
   * @param f Source file to import
   * @param delim Regular expression representing the delimiter string.
   * @param filter valid import filter
   *
   * @return the name of the imported table
   *
   * @see ImportUtil#importFile(File,Database,String,String,ImportFilter)
   * @usage _general_method_
   */
  public String importFile(String name, File f, String delim,
                           ImportFilter filter)
    throws IOException
  {
    return ImportUtil.importFile(f, this, name, delim, filter);
  }

  /**
   * Copy a delimited text file into a new table in this database
   * 
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   *
   * @return the name of the imported table
   *
   * @see ImportUtil#importReader(BufferedReader,Database,String,String)
   * @usage _general_method_
   */
  public String importReader(String name, BufferedReader in, String delim)
    throws IOException
  {
    return ImportUtil.importReader(in, this, name, delim);
  }
  
  /**
   * Copy a delimited text file into a new table in this database
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   * @param filter valid import filter
   *
   * @return the name of the imported table
   *
   * @see ImportUtil#importReader(BufferedReader,Database,String,String,ImportFilter)
   * @usage _general_method_
   */
  public String importReader(String name, BufferedReader in, String delim,
                             ImportFilter filter)
    throws IOException
  {
    return ImportUtil.importReader(in, this, name, delim, filter);
  }

  /**
   * Flushes any current changes to the database file (and any linked
   * databases) to disk.
   * @usage _general_method_
   */
  public void flush() throws IOException {
    if(_linkedDbs != null) {
      for(Database linkedDb : _linkedDbs.values()) {
        linkedDb.flush();
      }
    }
    _pageChannel.flush();
  }
  
  /**
   * Close the database file (and any linked databases)
   * @usage _general_method_
   */
  public void close() throws IOException {
    if(_linkedDbs != null) {
      for(Database linkedDb : _linkedDbs.values()) {
        linkedDb.close();
      }
    }
    _pageChannel.close();
  }
  
  /**
   * @return A table or column name escaped for Access
   * @usage _general_method_
   */
  public static String escapeIdentifier(String s) {
    if (isReservedWord(s)) {
      return ESCAPE_PREFIX + s; 
    }
    return s;
  }

  /**
   * @return {@code true} if the given string is a reserved word,
   *         {@code false} otherwise
   * @usage _general_method_
   */
  public static boolean isReservedWord(String s) {
    return RESERVED_WORDS.contains(s.toLowerCase());
  }

  /**
   * Validates an identifier name.
   * @usage _advanced_method_
   */
  public static void validateIdentifierName(String name,
                                            int maxLength,
                                            String identifierType)
  {
    if((name == null) || (name.trim().length() == 0)) {
      throw new IllegalArgumentException(
          identifierType + " must have non-empty name");
    }
    if(name.length() > maxLength) {
      throw new IllegalArgumentException(
          identifierType + " name is longer than max length of " + maxLength +
          ": " + name);
    }
  }
  
  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

  /**
   * Adds a table to the _tableLookup and resets the _tableNames set
   */
  private void addTable(String tableName, Integer pageNumber, Short type, 
                        String linkedDbName, String linkedTableName)
  {
    _tableLookup.put(toLookupName(tableName),
                     createTableInfo(tableName, pageNumber, 0, type, 
                                     linkedDbName, linkedTableName));
    // clear this, will be created next time needed
    _tableNames = null;
  }

  /**
   * Creates a TableInfo instance appropriate for the given table data.
   */
  private static TableInfo createTableInfo(
      String tableName, Integer pageNumber, int flags, Short type, 
      String linkedDbName, String linkedTableName)
  {
    if(TYPE_LINKED_TABLE.equals(type)) {
      return new LinkedTableInfo(pageNumber, tableName, flags, linkedDbName,
                                 linkedTableName);
    }
    return new TableInfo(pageNumber, tableName, flags);
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
  static String toLookupName(String name) {
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
   * Returns {@code false} if "big index support" has been disabled explicity
   * on the this Database or via a system property, {@code true} otherwise.
   * @usage _advanced_method_
   */
  public boolean defaultUseBigIndex() {
    if(_useBigIndex != null) {
      return _useBigIndex;
    }
    String prop = System.getProperty(USE_BIG_INDEX_PROPERTY);
    if(prop != null) {
      return Boolean.TRUE.toString().equalsIgnoreCase(prop);
    }
    return true;
  }

  /**
   * Returns the default TimeZone.  This is normally the platform default
   * TimeZone as returned by {@link TimeZone#getDefault}, but can be
   * overridden using the system property {@value #TIMEZONE_PROPERTY}.
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
   * {@value #CHARSET_PROPERTY_PREFIX} followed by the JetFormat version to
   * which the charset should apply, e.g. {@code
   * "com.healthmarketscience.jackcess.charset.VERSION_3"}.
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
   * {@link #DEFAULT_COLUMN_ORDER}, but can be overridden using the system
   * property {@value #COLUMN_ORDER_PROPERTY}.
   * @usage _advanced_method_
   */
  public static Table.ColumnOrder getDefaultColumnOrder()
  {
    String coProp = System.getProperty(COLUMN_ORDER_PROPERTY);
    if(coProp != null) {
      coProp = coProp.trim();
      if(coProp.length() > 0) {
        return Table.ColumnOrder.valueOf(coProp);
      }
    }

    // use default order
    return DEFAULT_COLUMN_ORDER;
  }
  
  /**
   * Copies the given InputStream to the given channel using the most
   * efficient means possible.
   */
  private static void transferFrom(FileChannel channel, InputStream in)
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
   */
  static byte[] getPasswordMask(ByteBuffer buffer, JetFormat format)
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
    ByteBuffer.wrap(pwdMask).order(PageChannel.DEFAULT_BYTE_ORDER)
      .putInt((int)dateVal);

    return pwdMask;
  }

  static InputStream getResourceAsStream(String resourceName)
    throws IOException
  {
    InputStream stream = Database.class.getClassLoader()
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
    return(TYPE_TABLE.equals(objType) || TYPE_LINKED_TABLE.equals(objType));
  }

  /**
   * Utility class for storing table page number and actual name.
   */
  private static class TableInfo
  {
    public final Integer pageNumber;
    public final String tableName;
    public final int flags;

    private TableInfo(Integer newPageNumber, String newTableName, int newFlags) {
      pageNumber = newPageNumber;
      tableName = newTableName;
      flags = newFlags;
    }

    public boolean isLinked() {
      return false;
    }
  }

  /**
   * Utility class for storing linked table info
   */
  private static class LinkedTableInfo extends TableInfo
  {
    private final String linkedDbName;
    private final String linkedTableName;

    private LinkedTableInfo(Integer newPageNumber, String newTableName, 
                            int newFlags, String newLinkedDbName, 
                            String newLinkedTableName) {
      super(newPageNumber, newTableName, newFlags);
      linkedDbName = newLinkedDbName;
      linkedTableName = newLinkedTableName;
    }

    @Override
    public boolean isLinked() {
      return true;
    }
  }

  /**
   * Table iterator for this database, unmodifiable.
   */
  private class TableIterator implements Iterator<Table>
  {
    private Iterator<String> _tableNameIter;

    private TableIterator() {
      try {
        _tableNameIter = getTableNames().iterator();
      } catch(IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public boolean hasNext() {
      return _tableNameIter.hasNext();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public Table next() {
      if(!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        return getTable(_tableNameIter.next());
      } catch(IOException e) {
        throw new IllegalStateException(e);
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
      Column idCol = _systemCatalog.getColumn(CAT_COL_ID);
      return (Integer)cur.getCurrentRowValue(idCol);
    }

    public Map<String,Object> getObjectRow(Integer parentId, String name,
                                           Collection<String> columns) 
      throws IOException 
    {
      Cursor cur = findRow(parentId, name);
      return ((cur != null) ? cur.getCurrentRow(columns) : null);
    }

    public Map<String,Object> getObjectRow(
        Integer objectId, Collection<String> columns)
      throws IOException
    {
      Cursor cur = findRow(objectId);
      return ((cur != null) ? cur.getCurrentRow(columns) : null);
    }

    public void getTableNames(Set<String> tableNames,
                              boolean systemTables)
      throws IOException
    {
      for(Map<String,Object> row : getTableNamesCursor().iterable(
              SYSTEM_CATALOG_TABLE_NAME_COLUMNS)) {

        String tableName = (String)row.get(CAT_COL_NAME);
        int flags = (Integer)row.get(CAT_COL_FLAGS);
        Short type = (Short)row.get(CAT_COL_TYPE);
        int parentId = (Integer)row.get(CAT_COL_PARENT_ID);

        if((parentId == _tableParentId) && isTableType(type) && 
           (isSystemObject(flags) == systemTables)) {
          tableNames.add(tableName);
        }
      }
    }

    protected abstract Cursor findRow(Integer parentId, String name)
      throws IOException;

    protected abstract Cursor findRow(Integer objectId) 
      throws IOException;

    protected abstract Cursor getTableNamesCursor() throws IOException;

    public abstract TableInfo lookupTable(String tableName)
      throws IOException;

    protected abstract int findMaxSyntheticId() throws IOException;

    public int getNextFreeSyntheticId() throws IOException
    {
      int maxSynthId = findMaxSyntheticId();
      if(maxSynthId >= -1) {
        // bummer, no more ids available
        throw new IllegalStateException("Too many database objects!");
      }
      return maxSynthId + 1;
    }
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
        _systemCatalogIdCursor = new CursorBuilder(_systemCatalog)
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

      Map<String,Object> row = _systemCatalogCursor.getCurrentRow(
          SYSTEM_CATALOG_COLUMNS);
      Integer pageNumber = (Integer)row.get(CAT_COL_ID);
      String realName = (String)row.get(CAT_COL_NAME);
      int flags = (Integer)row.get(CAT_COL_FLAGS);
      Short type = (Short)row.get(CAT_COL_TYPE);

      if(!isTableType(type)) {
        return null;
      }

      String linkedDbName = (String)row.get(CAT_COL_DATABASE);
      String linkedTableName = (String)row.get(CAT_COL_FOREIGN_NAME);

      return createTableInfo(realName, pageNumber, flags, type, linkedDbName,
                             linkedTableName);
    }
    
    @Override
    protected Cursor getTableNamesCursor() throws IOException {
      return new CursorBuilder(_systemCatalog)
        .setIndex(_systemCatalogCursor.getIndex())
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
      Column idCol = _systemCatalog.getColumn(CAT_COL_ID);
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
      Column idCol = _systemCatalog.getColumn(CAT_COL_ID);
      return (_systemCatalogCursor.findFirstRow(idCol, objectId) ?
              _systemCatalogCursor : null);
    }

    @Override
    public TableInfo lookupTable(String tableName) throws IOException {

      for(Map<String,Object> row : _systemCatalogCursor.iterable(
              SYSTEM_CATALOG_TABLE_NAME_COLUMNS)) {

        Short type = (Short)row.get(CAT_COL_TYPE);
        if(!isTableType(type)) {
          continue;
        }

        int parentId = (Integer)row.get(CAT_COL_PARENT_ID);
        if(parentId != _tableParentId) {
          continue;
        }

        String realName = (String)row.get(CAT_COL_NAME);
        if(!tableName.equalsIgnoreCase(realName)) {
          continue;
        }

        Integer pageNumber = (Integer)row.get(CAT_COL_ID);
        int flags = (Integer)row.get(CAT_COL_FLAGS);
        String linkedDbName = (String)row.get(CAT_COL_DATABASE);
        String linkedTableName = (String)row.get(CAT_COL_FOREIGN_NAME);

        return createTableInfo(realName, pageNumber, flags, type, linkedDbName,
                               linkedTableName);
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
      Column idCol = _systemCatalog.getColumn(CAT_COL_ID);
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
  private static final class WeakTableReference extends WeakReference<Table>
  {
    private final Integer _pageNumber;

    private WeakTableReference(Integer pageNumber, Table table, 
                               ReferenceQueue<Table> queue) {
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
    private final ReferenceQueue<Table> _queue = new ReferenceQueue<Table>();

    public Table get(Integer pageNumber) {
      WeakTableReference ref = _tables.get(pageNumber);
      return ((ref != null) ? ref.get() : null);
    }

    public Table put(Table table) {
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
}
