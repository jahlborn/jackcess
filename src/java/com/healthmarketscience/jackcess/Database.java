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
import java.io.FileReader;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An Access database.
 * <p>
 * There is now experimental, optional support for large indexes (disabled by
 * default).  This optional support can be enabled via a few different means:
 * <ul>
 * <li>Setting the system property {@value #USE_BIG_INDEX_PROPERTY} to
 *     {@code "true"} will enable "large" index support accross the jvm</li>
 * <li>Calling {@link #setUseBigIndex} with {@code true} on a Database
 *     instance will enable "large" index support for all tables subsequently
 *     created from that instance</li>
 * <li>Calling {@link #getTable(String,boolean)} can selectively
 *     enable/disable "large" index support on a per-table basis (overriding
 *     any Database or system property setting)</li>
 * </ul>
 *
 * @author Tim McCune
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
      but leaves more chance of a useable database in the face of failures. */
  public static final boolean DEFAULT_AUTO_SYNC = true;

  /** system property which can be used to make big index support the
      default. */
  public static final String USE_BIG_INDEX_PROPERTY =
    "com.healthmarketscience.jackcess.bigIndex";
  
  /** Batch commit size for copying other result sets into this database */
  private static final int COPY_TABLE_BATCH_SIZE = 200;
  
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
  
  /** Empty database template for creating new databases */
  private static final String EMPTY_MDB = "com/healthmarketscience/jackcess/empty.mdb";
  /** Prefix for column or table names that are reserved words */
  private static final String ESCAPE_PREFIX = "x";
  /** Prefix that flags system tables */
  private static final String PREFIX_SYSTEM = "MSys";
  /** Name of the system object that is the parent of all tables */
  private static final String SYSTEM_OBJECT_NAME_TABLES = "Tables";
  /** Name of the table that contains system access control entries */
  private static final String TABLE_SYSTEM_ACES = "MSysACEs";
  /** Name of the table that contains table relationships */
  private static final String TABLE_SYSTEM_RELATIONSHIPS = "MSysRelationships";
  /** System object type for table definitions */
  private static final Short TYPE_TABLE = (short) 1;

  /** the columns to read when reading system catalog initially */
  private static Collection<String> SYSTEM_CATALOG_COLUMNS =
    new HashSet<String>(Arrays.asList(CAT_COL_NAME, CAT_COL_TYPE, CAT_COL_ID));
  
  
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
  
  /** Buffer to hold database pages */
  private ByteBuffer _buffer;
  /** ID of the Tables system object */
  private Integer _tableParentId;
  /** Format that the containing database is in */
  private final JetFormat _format;
  /**
   * Map of UPPERCASE table names to page numbers containing their definition
   * and their stored table name.
   */
  private Map<String, TableInfo> _tableLookup =
    new HashMap<String, TableInfo>();
  /** set of table names as stored in the mdb file, created on demand */
  private Set<String> _tableNames;
  /** Reads and writes database pages */
  private final PageChannel _pageChannel;
  /** System catalog table */
  private Table _systemCatalog;
  /** System access control entries table */
  private Table _accessControlEntries;
  /** page number of the system relationships table */
  private Integer _relationshipsPageNumber;
  /** System relationships table (initialized on first use) */
  private Table _relationships;
  /** SIDs to use for the ACEs added for new tables */
  private final List<byte[]> _newTableSIDs = new ArrayList<byte[]>();
  /** for now, "big index support" is optional */
  private boolean _useBigIndex;
  
  /**
   * Open an existing Database.  If the existing file is not writeable, the
   * file will be opened read-only.  Auto-syncing is enabled for the returned
   * Database.
   * @param mdbFile File containing the database
   */
  public static Database open(File mdbFile) throws IOException {
    return open(mdbFile, false);
  }
  
  /**
   * Open an existing Database.  If the existing file is not writeable or the
   * readOnly flag is <code>true</code>, the file will be opened read-only.
   * Auto-syncing is enabled for the returned Database.
   * @param mdbFile File containing the database
   * @param readOnly iff <code>true</code>, force opening file in read-only
   *                 mode
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
   */
  public static Database open(File mdbFile, boolean readOnly, boolean autoSync)
    throws IOException
  {    
    if(!mdbFile.exists() || !mdbFile.canRead()) {
      throw new FileNotFoundException("given file does not exist: " + mdbFile);
    }
    return new Database(openChannel(mdbFile,
                                    (!mdbFile.canWrite() || readOnly)),
                        autoSync);
  }
  
  /**
   * Create a new Database
   * @param mdbFile Location to write the new database to.  <b>If this file
   *    already exists, it will be overwritten.</b>
   */
  public static Database create(File mdbFile) throws IOException {
    return create(mdbFile, DEFAULT_AUTO_SYNC);
  }
  
  /**
   * Create a new Database
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
   */
  public static Database create(File mdbFile, boolean autoSync)
    throws IOException
  {    
    FileChannel channel = openChannel(mdbFile, false);
    channel.truncate(0);
    channel.transferFrom(Channels.newChannel(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(
            EMPTY_MDB)), 0, Integer.MAX_VALUE);
    return new Database(channel, autoSync);
  }
  
  private static FileChannel openChannel(File mdbFile, boolean readOnly)
    throws FileNotFoundException
  {
    String mode = (readOnly ? "r" : "rw");
    return new RandomAccessFile(mdbFile, mode).getChannel();
  }
  
  /**
   * Create a new database by reading it in from a FileChannel.
   * @param channel File channel of the database.  This needs to be a
   *    FileChannel instead of a ReadableByteChannel because we need to
   *    randomly jump around to various points in the file.
   */
  protected Database(FileChannel channel, boolean autoSync) throws IOException
  {
    _format = JetFormat.getFormat(channel);
    _pageChannel = new PageChannel(channel, _format, autoSync);
    // note, it's slighly sketchy to pass ourselves along partially
    // constructed, but only our _format and _pageChannel refs should be
    // needed
    _pageChannel.initialize(this);
    _buffer = _pageChannel.createPageBuffer();
    readSystemCatalog();
  }
  
  public PageChannel getPageChannel() {
    return _pageChannel;
  }

  public JetFormat getFormat() {
    return _format;
  }
  
  /**
   * @return The system catalog table
   */
  public Table getSystemCatalog() {
    return _systemCatalog;
  }
  
  /**
   * @return The system Access Control Entries table
   */
  public Table getAccessControlEntries() {
    return _accessControlEntries;
  }

  /**
   * Whether or not big index support is enabled for tables.
   */
  public boolean doUseBigIndex() {
    return _useBigIndex;
  }

  /**
   * Set whether or not big index support is enabled for tables.
   */
  public void setUseBigIndex(boolean useBigIndex) {
    _useBigIndex = useBigIndex;
  }
  
  /**
   * Read the system catalog
   */
  private void readSystemCatalog() throws IOException {
    _systemCatalog = readTable(TABLE_SYSTEM_CATALOG, PAGE_SYSTEM_CATALOG,
                               defaultUseBigIndex());
    for(Map<String,Object> row :
          Cursor.createCursor(_systemCatalog).iterable(
              SYSTEM_CATALOG_COLUMNS))
    {
      String name = (String) row.get(CAT_COL_NAME);
      if (name != null && TYPE_TABLE.equals(row.get(CAT_COL_TYPE))) {
        if (!name.startsWith(PREFIX_SYSTEM)) {
          addTable((String) row.get(CAT_COL_NAME), (Integer) row.get(CAT_COL_ID));
        } else if(TABLE_SYSTEM_ACES.equals(name)) {
          int pageNumber = (Integer)row.get(CAT_COL_ID);
          _accessControlEntries = readTable(TABLE_SYSTEM_ACES, pageNumber,
                                            defaultUseBigIndex());
        } else if(TABLE_SYSTEM_RELATIONSHIPS.equals(name)) {
          _relationshipsPageNumber = (Integer)row.get(CAT_COL_ID);
        }
      } else if (SYSTEM_OBJECT_NAME_TABLES.equals(name)) {
        _tableParentId = (Integer) row.get(CAT_COL_ID);
      }
    }

    // check for required system values
    if(_accessControlEntries == null) {
      throw new IOException("Did not find required " + TABLE_SYSTEM_ACES +
                            " table");
    }
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
   */
  public Set<String> getTableNames() {
    if(_tableNames == null) {
      _tableNames = new HashSet<String>();
      for(TableInfo tableInfo : _tableLookup.values()) {
        _tableNames.add(tableInfo.tableName);
      }
    }
    return _tableNames;
  }

  /**
   * @return an unmodifiable Iterator of the user Tables in this Database.
   * @throws IllegalStateException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   * @throws ConcurrentModificationException if a table is added to the
   *         database while an Iterator is in use.
   */
  public Iterator<Table> iterator() {
    return new TableIterator();
  }

  /**
   * @param name Table name
   * @return The table, or null if it doesn't exist
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
   */
  public Table getTable(String name, boolean useBigIndex) throws IOException {

    TableInfo tableInfo = lookupTable(name);
    
    if ((tableInfo == null) || (tableInfo.pageNumber == null)) {
      return null;
    }

    return readTable(tableInfo.tableName, tableInfo.pageNumber, useBigIndex);
  }
  
  /**
   * Create a new table in this database
   * @param name Name of the table to create
   * @param columns List of Columns in the table
   */
  public void createTable(String name, List<Column> columns)
    throws IOException
  {
    validateIdentifierName(name, _format.MAX_TABLE_NAME_LENGTH, "table");
    
    if(getTable(name) != null) {
      throw new IllegalArgumentException(
          "Cannot create table with name of existing table");
    }
    
    if(columns.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot create table with no columns");
    }
    if(columns.size() > _format.MAX_COLUMNS_PER_TABLE) {
      throw new IllegalArgumentException(
          "Cannot create table with more than " +
          _format.MAX_COLUMNS_PER_TABLE + " columns");
    }
    
    Set<String> colNames = new HashSet<String>();
    // next, validate the column definitions
    for(Column column : columns) {
      column.validate(_format);
      if(!colNames.add(column.getName().toUpperCase())) {
        throw new IllegalArgumentException("duplicate column name: " +
                                           column.getName());
      }
    }

    if(Table.countAutoNumberColumns(columns) > 1) {
      throw new IllegalArgumentException(
          "Can have at most one AutoNumber column per table");
    }
    
    //Write the tdef page to disk.
    int tdefPageNumber = Table.writeTableDefinition(columns, _pageChannel,
                                                    _format);
    
    //Add this table to our internal list.
    addTable(name, Integer.valueOf(tdefPageNumber));
    
    //Add this table to system tables
    addToSystemCatalog(name, tdefPageNumber);
    addToAccessControlEntries(tdefPageNumber);    
  }

  /**
   * Finds all the relationships in the database between the given tables.
   */
  public List<Relationship> getRelationships(Table table1, Table table2)
    throws IOException
  {
    // the relationships table does not get loaded until first accessed
    if(_relationships == null) {
      if(_relationshipsPageNumber == null) {
        throw new IOException("Could not find system relationships table");
      }
      _relationships = readTable(TABLE_SYSTEM_RELATIONSHIPS,
                                 _relationshipsPageNumber,
                                 defaultUseBigIndex());
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
   * Finds the relationships matching the given from and to tables from the
   * given cursor and adds them to the given list.
   */
  private void collectRelationships(
      Cursor cursor, Table fromTable, Table toTable,
      List<Relationship> relationships)
  {
    for(Map<String,Object> row : cursor) {
      String fromName = (String)row.get(REL_COL_FROM_TABLE);
      String toName = (String)row.get(REL_COL_TO_TABLE);
      
      if(fromTable.getName().equals(fromName) &&
         toTable.getName().equals(toName))
      {

        String relName = (String)row.get(REL_COL_NAME);
        
        // found more info for a relationship.  see if we already have some
        // info for this relationship
        Relationship rel = null;
        for(Relationship tmp : relationships) {
          if(tmp.getName().equals(relName)) {
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
  private void addToSystemCatalog(String name, int pageNumber)
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
        catalogRow[idx] = TYPE_TABLE;
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

    Column acmCol = _accessControlEntries.getColumn(ACE_COL_ACM);
    Column inheritCol = _accessControlEntries.getColumn(ACE_COL_F_INHERITABLE);
    Column objIdCol = _accessControlEntries.getColumn(ACE_COL_OBJECT_ID);
    Column sidCol = _accessControlEntries.getColumn(ACE_COL_SID);

    // construct a collection of ACE entries mimicing those of our parent, the
    // "Tables" system object
    List<Object[]> aceRows = new ArrayList<Object[]>(_newTableSIDs.size());
    for(byte[] sid : _newTableSIDs) {
      Object[] aceRow = new Object[_accessControlEntries.getColumnCount()];
      aceRow[acmCol.getColumnIndex()] = SYS_FULL_ACCESS_ACM;
      aceRow[inheritCol.getColumnIndex()] = Boolean.FALSE;
      aceRow[objIdCol.getColumnIndex()] = Integer.valueOf(pageNumber);
      aceRow[sidCol.getColumnIndex()] = sid;
      aceRows.add(aceRow);
    }
    _accessControlEntries.addRows(aceRows);  
  }

  /**
   * Determines the collection of SIDs which need to be added to new tables.
   */
  private void initNewTableSIDs() throws IOException
  {
    // search for ACEs matching the tableParentId.  use the index on the
    // objectId column if found (should be there)
    Cursor cursor = createCursorWithOptionalIndex(
        _accessControlEntries, ACE_COL_OBJECT_ID, _tableParentId);
    
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
  private Table readTable(String name, int pageNumber, boolean useBigIndex)
    throws IOException
  {
    _pageChannel.readPage(_buffer, pageNumber);
    byte pageType = _buffer.get(0);
    if (pageType != PageTypes.TABLE_DEF) {
      throw new IOException("Looking for " + name + " at page " + pageNumber +
                            ", but page type is " + pageType);
    }
    return new Table(this, _buffer, pageNumber, name, useBigIndex);
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
   * @param name Name of the new table to create
   * @param source ResultSet to copy from
   */
  public void copyTable(String name, ResultSet source)
    throws SQLException, IOException
  {
    copyTable(name, source, SimpleImportFilter.INSTANCE);
  }
  
  /**
   * Copy an existing JDBC ResultSet into a new table in this database
   * @param name Name of the new table to create
   * @param source ResultSet to copy from
   * @param filter valid import filter
   */
  public void copyTable(String name, ResultSet source, ImportFilter filter)
    throws SQLException, IOException
  {
    ResultSetMetaData md = source.getMetaData();
    List<Column> columns = new LinkedList<Column>();
    for (int i = 1; i <= md.getColumnCount(); i++) {
      Column column = new Column();
      column.setName(escape(md.getColumnName(i)));
      int lengthInUnits = md.getColumnDisplaySize(i);
      column.setSQLType(md.getColumnType(i), lengthInUnits);
      DataType type = column.getType();
      // we check for isTrueVariableLength here to avoid setting the length
      // for a NUMERIC column, which pretends to be var-len, even though it
      // isn't
      if(type.isTrueVariableLength() && !type.isLongValue()) {
        column.setLengthInUnits((short)lengthInUnits);
      }
      if(type.getHasScalePrecision()) {
        int scale = md.getScale(i);
        int precision = md.getPrecision(i);
        if(type.isValidScale(scale)) {
          column.setScale((byte)scale);
        }
        if(type.isValidPrecision(precision)) {
          column.setPrecision((byte)precision);
        }
      }
      columns.add(column);
    }
    createTable(escape(name), filter.filterColumns(columns, md));
    Table table = getTable(escape(name));
    List<Object[]> rows = new ArrayList<Object[]>(COPY_TABLE_BATCH_SIZE);
    while (source.next()) {
      Object[] row = new Object[md.getColumnCount()];
      for (int i = 0; i < row.length; i++) {
        row[i] = source.getObject(i + 1);
      }
      rows.add(filter.filterRow(row));
      if (rows.size() == COPY_TABLE_BATCH_SIZE) {
        table.addRows(rows);
        rows.clear();
      }
    }
    if (rows.size() > 0) {
      table.addRows(rows);
    }
  }
  
  /**
   * Copy a delimited text file into a new table in this database
   * @param name Name of the new table to create
   * @param f Source file to import
   * @param delim Regular expression representing the delimiter string.
   */
  public void importFile(String name, File f, String delim)
    throws IOException
  {
    importFile(name, f, delim, SimpleImportFilter.INSTANCE);
  }

  /**
   * Copy a delimited text file into a new table in this database
   * @param name Name of the new table to create
   * @param f Source file to import
   * @param delim Regular expression representing the delimiter string.
   * @param filter valid import filter
   */
  public void importFile(String name, File f, String delim,
                         ImportFilter filter)
    throws IOException
  {
    BufferedReader in = null;
    try {
      in = new BufferedReader(new FileReader(f));
      importReader(name, in, delim, filter);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException ex) {
          LOG.warn("Could not close file " + f.getAbsolutePath(), ex);
        }
      }
    }
  }

  /**
   * Copy a delimited text file into a new table in this database
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   */
  public void importReader(String name, BufferedReader in, String delim)
    throws IOException
  {
    importReader(name, in, delim, SimpleImportFilter.INSTANCE);
  }
  
  /**
   * Copy a delimited text file into a new table in this database
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   * @param filter valid import filter
   */
  public void importReader(String name, BufferedReader in, String delim,
                           ImportFilter filter)
    throws IOException
  {
    String line = in.readLine();
    if (line == null || line.trim().length() == 0) {
      return;
    }

    String tableName = escape(name);
    int counter = 0;
    while(getTable(tableName) != null) {
      tableName = escape(name + (counter++));
    }

    List<Column> columns = new LinkedList<Column>();
    String[] columnNames = line.split(delim);
      
    for (int i = 0; i < columnNames.length; i++) {
      columns.add(new ColumnBuilder(escape(columnNames[i]), DataType.TEXT)
                  .setLength((short)DataType.TEXT.getMaxSize())
                  .toColumn());
    }

    try {
      createTable(tableName, filter.filterColumns(columns, null));
      Table table = getTable(tableName);
      List<Object[]> rows = new ArrayList<Object[]>(COPY_TABLE_BATCH_SIZE);
      
      while ((line = in.readLine()) != null)
      {
        // 
        // Handle the situation where the end of the line
        // may have null fields.  We always want to add the
        // same number of columns to the table each time.
        //
        Object[] data = Table.dupeRow(line.split(delim), columnNames.length);
        rows.add(filter.filterRow(data));
        if (rows.size() == COPY_TABLE_BATCH_SIZE) {
          table.addRows(rows);
          rows.clear();
        }
      }
      if (rows.size() > 0) {
        table.addRows(rows);
      }
    } catch(SQLException e) {
      throw (IOException)new IOException(e.getMessage()).initCause(e);
    }
  }

  /**
   * Flushes any current changes to the database file to disk.
   */
  public void flush() throws IOException {
    _pageChannel.flush();
  }
  
  /**
   * Close the database file
   */
  public void close() throws IOException {
    _pageChannel.close();
  }
  
  /**
   * @return A table or column name escaped for Access
   */
  private String escape(String s) {
    if (isReservedWord(s)) {
      return ESCAPE_PREFIX + s; 
    }
    return s;
  }

  /**
   * @return {@code true} if the given string is a reserved word,
   *         {@code false} otherwise
   */
  public static boolean isReservedWord(String s) {
    return RESERVED_WORDS.contains(s.toLowerCase());
  }

  /**
   * Validates an identifier name.
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
  private void addTable(String tableName, Integer pageNumber)
  {
    _tableLookup.put(toLookupTableName(tableName),
                     new TableInfo(pageNumber, tableName));
    // clear this, will be created next time needed
    _tableNames = null;
  }

  /**
   * @returns the tableInfo of the given table, if any
   */
  private TableInfo lookupTable(String tableName) {
    return _tableLookup.get(toLookupTableName(tableName));
  }

  /**
   * @return a string usable in the _tableLookup map.
   */
  private String toLookupTableName(String tableName) {
    return ((tableName != null) ? tableName.toUpperCase() : null);
  }

  /**
   * Returns {@code true} if "big index support" has been enabled explicity on
   * the this Database or via a system property, {@code false} otherwise.
   */
  public boolean defaultUseBigIndex() {
    return doUseBigIndex() || Boolean.getBoolean(USE_BIG_INDEX_PROPERTY);
  }
  
  /**
   * Utility class for storing table page number and actual name.
   */
  private static class TableInfo
  {
    public final Integer pageNumber;
    public final String tableName;

    private TableInfo(Integer newPageNumber,
                      String newTableName) {
      pageNumber = newPageNumber;
      tableName = newTableName;
    }
  }

  /**
   * Table iterator for this database, unmodifiable.
   */
  private class TableIterator implements Iterator<Table>
  {
    private Iterator<String> _tableNameIter;

    private TableIterator() {
      _tableNameIter = getTableNames().iterator();
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
  
}
