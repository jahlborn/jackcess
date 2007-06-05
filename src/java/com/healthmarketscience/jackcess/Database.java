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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
 *
 * @author Tim McCune
 */
public class Database
  implements Iterable<Table>
{
  
  private static final Log LOG = LogFactory.getLog(Database.class);
  
  private static final byte[] SID = new byte[2];
  static {
    SID[0] = (byte) 0xA6;
    SID[1] = (byte) 0x33;
  }

  /** default value for the auto-sync value ({@code true}).  this is slower,
      but leaves more chance of a useable database in the face of failures. */
  public static final boolean DEFAULT_AUTO_SYNC = true;
  
  /** Batch commit size for copying other result sets into this database */
  private static final int COPY_TABLE_BATCH_SIZE = 200;
  
  /** System catalog always lives on page 2 */
  private static final int PAGE_SYSTEM_CATALOG = 2;
  
  private static final int ACM = 1048319;
  
  /** Free space left in page for new usage map definition pages */
  private static final short USAGE_MAP_DEF_FREE_SPACE = 3940;
  
  private static final String COL_ACM = "ACM";
  /** System catalog column name of the date a system object was created */
  private static final String COL_DATE_CREATE = "DateCreate";
  /** System catalog column name of the date a system object was updated */
  private static final String COL_DATE_UPDATE = "DateUpdate";
  private static final String COL_F_INHERITABLE = "FInheritable";
  private static final String COL_FLAGS = "Flags";
  /**
   * System catalog column name of the page on which system object definitions
   * are stored
   */
  private static final String COL_ID = "Id";
  /** System catalog column name of the name of a system object */
  private static final String COL_NAME = "Name";
  private static final String COL_OBJECT_ID = "ObjectId";
  private static final String COL_OWNER = "Owner";
  /** System catalog column name of a system object's parent's id */
  private static final String COL_PARENT_ID = "ParentId";
  private static final String COL_SID = "SID";
  /** System catalog column name of the type of a system object */
  private static final String COL_TYPE = "Type";
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
  /** System object type for table definitions */
  private static final Short TYPE_TABLE = (short) 1;

  /** the columns to read when reading system catalog initially */
  private static Collection<String> SYSTEM_CATALOG_COLUMNS =
    new HashSet<String>(Arrays.asList(COL_NAME, COL_TYPE, COL_ID));
  
  
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
  private JetFormat _format;
  /**
   * Map of UPPERCASE table names to page numbers containing their definition
   * and their stored table name.
   */
  private Map<String, TableInfo> _tableLookup =
    new HashMap<String, TableInfo>();
  /** set of table names as stored in the mdb file, created on demand */
  private Set<String> _tableNames;
  /** Reads and writes database pages */
  private PageChannel _pageChannel;
  /** System catalog table */
  private Table _systemCatalog;
  /** System access control entries table */
  private Table _accessControlEntries;
  
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
    channel.transferFrom(Channels.newChannel(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(
        EMPTY_MDB)), 0, (long) Integer.MAX_VALUE);
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
    _buffer = _pageChannel.createPageBuffer();
    readSystemCatalog();
  }
  
  public PageChannel getPageChannel() {
    return _pageChannel;
  }
  
  /**
   * @return The system catalog table
   */
  public Table getSystemCatalog() {
    return _systemCatalog;
  }
  
  public Table getAccessControlEntries() {
    return _accessControlEntries;
  }
  
  /**
   * Read the system catalog
   */
  private void readSystemCatalog() throws IOException {
    _pageChannel.readPage(_buffer, PAGE_SYSTEM_CATALOG);
    byte pageType = _buffer.get();
    if (pageType != PageTypes.TABLE_DEF) {
      throw new IOException("Looking for system catalog at page " +
          PAGE_SYSTEM_CATALOG + ", but page type is " + pageType);
    }
    _systemCatalog = new Table(_buffer, _pageChannel, _format, PAGE_SYSTEM_CATALOG, "System Catalog");
    Map row;
    while ( (row = _systemCatalog.getNextRow(SYSTEM_CATALOG_COLUMNS)) != null)
    {
      String name = (String) row.get(COL_NAME);
      if (name != null && TYPE_TABLE.equals(row.get(COL_TYPE))) {
        if (!name.startsWith(PREFIX_SYSTEM)) {
          addTable((String) row.get(COL_NAME), (Integer) row.get(COL_ID));
        } else if (TABLE_SYSTEM_ACES.equals(name)) {
          readAccessControlEntries(((Integer) row.get(COL_ID)).intValue());
        }
      } else if (SYSTEM_OBJECT_NAME_TABLES.equals(name)) {
        _tableParentId = (Integer) row.get(COL_ID);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished reading system catalog.  Tables: " +
                getTableNames());
    }
  }
  
  /**
   * Read the system access control entries table
   * @param pageNum Page number of the table def
   */
  private void readAccessControlEntries(int pageNum) throws IOException {
    ByteBuffer buffer = _pageChannel.createPageBuffer();
    _pageChannel.readPage(buffer, pageNum);
    byte pageType = buffer.get();
    if (pageType != PageTypes.TABLE_DEF) {
      throw new IOException("Looking for MSysACEs at page " + pageNum +
          ", but page type is " + pageType);
    }
    _accessControlEntries = new Table(buffer, _pageChannel, _format, pageNum, "Access Control Entries");
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

    TableInfo tableInfo = lookupTable(name);
    
    if ((tableInfo == null) || (tableInfo.pageNumber == null)) {
      return null;
    } else {
      int pageNumber = tableInfo.pageNumber.intValue();
      _pageChannel.readPage(_buffer, pageNumber);
      return new Table(_buffer, _pageChannel, _format, pageNumber,
                       tableInfo.tableName);
    }
  }
  
  /**
   * Create a new table in this database
   * @param name Name of the table to create
   * @param columns List of Columns in the table
   */
   //XXX Set up 1-page rollback buffer?
  public void createTable(String name, List<Column> columns)
    throws IOException
  {
    if(getTable(name) != null) {
      throw new IllegalArgumentException(
          "Cannot create table with name of existing table");
    }
    if(columns.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot create table with no columns");
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
    
    //We are creating a new page at the end of the db for the tdef.
    int pageNumber = _pageChannel.getPageCount();
    
    ByteBuffer buffer = _pageChannel.createPageBuffer();
    
    writeTableDefinition(buffer, columns, pageNumber);
    
    writeColumnDefinitions(buffer, columns); 
    
    //End of tabledef
    buffer.put((byte) 0xff);
    buffer.put((byte) 0xff);

    int tableDefLen = buffer.position();
    buffer.putShort(2, (short)(_format.PAGE_SIZE - tableDefLen - 8)); // overwrite page free space
    buffer.putInt(8, tableDefLen);  //Overwrite length of data for this page
       
    //Write the tdef and usage map pages to disk.
    _pageChannel.writeNewPage(buffer);
    _pageChannel.writeNewPage(createUsageMapDefinitionBuffer(pageNumber));
    _pageChannel.writeNewPage(createUsageMapDataBuffer()); //Usage map
    
    //Add this table to our internal list.
    addTable(name, new Integer(pageNumber));
    
    //Add this table to system tables
    addToSystemCatalog(name, pageNumber);
    addToAccessControlEntries(pageNumber);    
  }
  
  /**
   * @param buffer Buffer to write to
   * @param columns List of Columns in the table
   * @param pageNumber Page number that this table definition will be written to
   */
  private void writeTableDefinition(ByteBuffer buffer, List<Column> columns,
      int pageNumber)
  throws IOException {
    //Start writing the tdef
    buffer.put(PageTypes.TABLE_DEF);  //Page type
    buffer.put((byte) 0x01); //Unknown
    buffer.put((byte) 0); //Unknown
    buffer.put((byte) 0); //Unknown
    buffer.putInt(0);  //Next TDEF page pointer
    buffer.putInt(0);  //Length of data for this page
    buffer.put((byte) 0x59);  //Unknown
    buffer.put((byte) 0x06);  //Unknown
    buffer.putShort((short) 0); //Unknown
    buffer.putInt(0);  //Number of rows
    buffer.putInt(0); //Autonumber
    for (int i = 0; i < 16; i++) {  //Unknown
      buffer.put((byte) 0);
    }
    buffer.put(Table.TYPE_USER); //Table type
    buffer.putShort((short) columns.size()); //Max columns a row will have
    buffer.putShort(Column.countVariableLength(columns));  //Number of variable columns in table
    buffer.putShort((short) columns.size()); //Number of columns in table
    buffer.putInt(0);  //Number of indexes in table
    buffer.putInt(0);  //Number of indexes in table
    buffer.put((byte) 0); //Usage map row number
    int usageMapPage = pageNumber + 1;
    ByteUtil.put3ByteInt(buffer, usageMapPage);  //Usage map page number
    buffer.put((byte) 1); //Free map row number
    ByteUtil.put3ByteInt(buffer, usageMapPage);  //Free map page number
    if (LOG.isDebugEnabled()) {
      int position = buffer.position();
      buffer.rewind();
      LOG.debug("Creating new table def block:\n" + ByteUtil.toHexString(
          buffer, _format.SIZE_TDEF_BLOCK));
      buffer.position(position);
    }
  }
  
  /**
   * @param buffer Buffer to write to
   * @param columns List of Columns to write definitions for
   */
  private void writeColumnDefinitions(ByteBuffer buffer, List<Column> columns)
  throws IOException {
    short columnNumber = (short) 0;
    short fixedOffset = (short) 0;
    short variableOffset = (short) 0;
    // we specifically put the "long variable" values after the normal
    // variable length values so that we have a better chance of fitting it
    // all (because "long variable" values can go in separate pages)
    short longVariableOffset =
      (short) Column.countNonLongVariableLength(columns);
    for (Column col : columns) {
      int position = buffer.position();
      buffer.put(col.getType().getValue());
      buffer.put((byte) 0x59);  //Unknown
      buffer.put((byte) 0x06);  //Unknown
      buffer.putShort((short) 0); //Unknown
      buffer.putShort(columnNumber);  //Column Number
      if (col.isVariableLength()) {
        if(!col.getType().isLongValue()) {
          buffer.putShort(variableOffset++);
        } else {
          buffer.putShort(longVariableOffset++);
        }          
      } else {
        buffer.putShort((short) 0);
      }
      buffer.putShort(columnNumber); //Column Number again
      if(col.getType().getHasScalePrecision()) {
        buffer.put((byte) col.getPrecision());  // numeric precision
        buffer.put((byte) col.getScale());  // numeric scale
      } else {
        buffer.put((byte) 0x00); //unused
        buffer.put((byte) 0x00); //unused
      }
      buffer.putShort((short) 0); //Unknown
      if (col.isVariableLength()) { //Variable length
        buffer.put((byte) 0x2);
      } else {
        buffer.put((byte) 0x3);
      }
      if (col.isCompressedUnicode()) {  //Compressed
        buffer.put((byte) 1);
      } else {
        buffer.put((byte) 0);
      }
      buffer.putInt(0); //Unknown, but always 0.
      //Offset for fixed length columns
      if (col.isVariableLength()) {
        buffer.putShort((short) 0);
      } else {
        buffer.putShort(fixedOffset);
        fixedOffset += col.getType().getFixedSize();
      }
      if(!col.getType().isLongValue()) {
        buffer.putShort(col.getLength()); //Column length
      } else {
        buffer.putShort((short)0x0000); // unused
      }
      columnNumber++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating new column def block\n" + ByteUtil.toHexString(
            buffer, position, _format.SIZE_COLUMN_DEF_BLOCK));
      }
    }
    for (Column col : columns) {
      ByteBuffer colName = _format.CHARSET.encode(col.getName());
      buffer.putShort((short) colName.remaining());
      buffer.put(colName);
    }
  }
  
  /**
   * Create the usage map definition page buffer.  It will be stored on the page
   * immediately after the tdef page.
   * @param pageNumber Page number that the corresponding table definition will
   *    be written to
   */
  private ByteBuffer createUsageMapDefinitionBuffer(int pageNumber) throws IOException {
    ByteBuffer rtn = _pageChannel.createPageBuffer();
    rtn.put(PageTypes.DATA);
    rtn.put((byte) 0x1);  //Unknown
    rtn.putShort(USAGE_MAP_DEF_FREE_SPACE);  //Free space in page
    rtn.putInt(0); //Table definition
    rtn.putInt(0); //Unknown
    rtn.putShort((short) 2); //Number of records on this page
    rtn.putShort((short) _format.OFFSET_USED_PAGES_USAGE_MAP_DEF);  //First location
    rtn.putShort((short) _format.OFFSET_FREE_PAGES_USAGE_MAP_DEF);  //Second location
    rtn.position(_format.OFFSET_USED_PAGES_USAGE_MAP_DEF);
    rtn.put((byte) UsageMap.MAP_TYPE_REFERENCE);
    rtn.putInt(pageNumber + 2);  //First referenced page number
    rtn.position(_format.OFFSET_FREE_PAGES_USAGE_MAP_DEF);
    rtn.put((byte) UsageMap.MAP_TYPE_INLINE);
    return rtn;
  }
  
  /**
   * Create a usage map data page buffer.
   */
  private ByteBuffer createUsageMapDataBuffer() throws IOException {
    ByteBuffer rtn = _pageChannel.createPageBuffer();
    rtn.put(PageTypes.USAGE_MAP);
    rtn.put((byte) 0x01); //Unknown
    rtn.putShort((short) 0);  //Unknown
    return rtn;
  }
  
  /**
   * Add a new table to the system catalog
   * @param name Table name
   * @param pageNumber Page number that contains the table definition
   */
  private void addToSystemCatalog(String name, int pageNumber) throws IOException {
    Object[] catalogRow = new Object[_systemCatalog.getColumns().size()];
    int idx = 0;
    Iterator iter;
    for (iter = _systemCatalog.getColumns().iterator(); iter.hasNext(); idx++) {
      Column col = (Column) iter.next();
      if (COL_ID.equals(col.getName())) {
        catalogRow[idx] = new Integer(pageNumber);
      } else if (COL_NAME.equals(col.getName())) {
        catalogRow[idx] = name;
      } else if (COL_TYPE.equals(col.getName())) {
        catalogRow[idx] = TYPE_TABLE;
      } else if (COL_DATE_CREATE.equals(col.getName()) ||
          COL_DATE_UPDATE.equals(col.getName()))
      {
        catalogRow[idx] = new Date();
      } else if (COL_PARENT_ID.equals(col.getName())) {
        catalogRow[idx] = _tableParentId;
      } else if (COL_FLAGS.equals(col.getName())) {
        catalogRow[idx] = new Integer(0);
      } else if (COL_OWNER.equals(col.getName())) {
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
    Object[] aceRow = new Object[_accessControlEntries.getColumns().size()];
    int idx = 0;
    Iterator iter;
    for (iter = _accessControlEntries.getColumns().iterator(); iter.hasNext(); idx++) {
      Column col = (Column) iter.next();
      if (col.getName().equals(COL_ACM)) {
        aceRow[idx] = ACM;
      } else if (col.getName().equals(COL_F_INHERITABLE)) {
        aceRow[idx] = Boolean.FALSE;
      } else if (col.getName().equals(COL_OBJECT_ID)) {
        aceRow[idx] = new Integer(pageNumber);
      } else if (col.getName().equals(COL_SID)) {
        aceRow[idx] = SID;
      }
    }
    _accessControlEntries.addRow(aceRow);  
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
      Column column = new Column();
      column.setName(escape(columnNames[i]));
      column.setType(DataType.TEXT);
      column.setLength((short)DataType.TEXT.getMaxSize());
      columns.add(column);
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
        String[] data = new String[columnNames.length];
        String[] splitData = line.split(delim);
        System.arraycopy(splitData, 0, data, 0, splitData.length);
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
   * Close the database file
   */
  public void close() throws IOException {
    _pageChannel.close();
  }
  
  /**
   * @return A table or column name escaped for Access
   */
  private String escape(String s) {
    if (RESERVED_WORDS.contains(s.toLowerCase())) {
      return ESCAPE_PREFIX + s; 
    } else {
      return s;
    }
  }
  
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
   * Utility class for storing table page number and actual name.
   */
  private static class TableInfo
  {
    public Integer pageNumber;
    public String tableName;

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
