/*
Copyright (c) 2013 James Ahlborn

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

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import com.healthmarketscience.jackcess.query.Query;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.util.ColumnValidatorFactory;
import com.healthmarketscience.jackcess.util.ErrorHandler;
import com.healthmarketscience.jackcess.util.LinkResolver;
import com.healthmarketscience.jackcess.util.TableIterableBuilder;

/**
 * An Access database instance.  A new instance can be instantiated by opening
 * an existing database file ({@link DatabaseBuilder#open(File)}) or creating
 * a new database file ({@link DatabaseBuilder#create(Database.FileFormat,File)}) (for
 * more advanced opening/creating use {@link DatabaseBuilder}).  Once a
 * Database has been opened, you can interact with the data via the relevant
 * {@link Table}.  When a Database instance is no longer useful, it should
 * <b>always</b> be closed ({@link #close}) to avoid corruption.
 * <p/>
 * Database instances (and all the related objects) are <i>not</i>
 * thread-safe.  However, separate Database instances (and their respective
 * objects) can be used by separate threads without a problem.
 * <p/>
 * Database instances do not implement any "transactional" support, and
 * therefore concurrent editing of the same database file by multiple Database
 * instances (or with outside programs such as MS Access) <i>will generally
 * result in database file corruption</i>.
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public interface Database extends Iterable<Table>, Closeable, Flushable
{
  /** default value for the auto-sync value ({@code true}).  this is slower,
   *  but leaves more chance of a useable database in the face of failures.
   * @usage _general_field_
   */
  public static final boolean DEFAULT_AUTO_SYNC = true;

  /**
   * the default sort order for table columns.
   * @usage _intermediate_field_
   */
  public static final Table.ColumnOrder DEFAULT_COLUMN_ORDER = 
    Table.ColumnOrder.DATA;

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
   *  is {@value com.healthmarketscience.jackcess.impl.DatabaseImpl#DEFAULT_RESOURCE_PATH}
   *  if unspecified.
   * @usage _general_field_
   */
  public static final String RESOURCE_PATH_PROPERTY = 
    "com.healthmarketscience.jackcess.resourcePath";

  /** (boolean) system property which can be used to indicate that the current
   *  vm has a poor nio implementation (specifically for
   *  {@code FileChannel.transferFrom})
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

  /** system property which can be used to set the default enforcement of
   * foreign-key relationships.  Defaults to {@code true}.
   * @usage _general_field_
   */
  public static final String FK_ENFORCE_PROPERTY = 
    "com.healthmarketscience.jackcess.enforceForeignKeys";

  /**
   * Enum which indicates which version of Access created the database.
   * @usage _general_class_
   */
  public enum FileFormat {

    V1997(".mdb"),
    V2000(".mdb"),
    V2003(".mdb"),
    V2007(".accdb"),
    V2010(".accdb"),
    MSISAM(".mny");

    private final String _ext;

    private FileFormat(String ext) {
      _ext = ext;
    }

    /**
     * @return the file extension used for database files with this format.
     */
    public String getFileExtension() { return _ext; }

    @Override
    public String toString() { 
      return name() + " [" + DatabaseImpl.getFileFormatDetails(this).getFormat() + "]";
    }
  }

  /**
   * Returns the File underlying this Database
   */
  public File getFile();

  /**
   * @return The names of all of the user tables
   * @usage _general_method_
   */
  public Set<String> getTableNames() throws IOException;

  /**
   * @return The names of all of the system tables (String).  Note, in order
   *         to read these tables, you must use {@link #getSystemTable}.
   *         <i>Extreme care should be taken if modifying these tables
   *         directly!</i>.
   * @usage _intermediate_method_
   */
  public Set<String> getSystemTableNames() throws IOException;

  /**
   * @return an unmodifiable Iterator of the user Tables in this Database.
   * @throws RuntimeIOException if an IOException is thrown by one of the
   *         operations, the actual exception will be contained within
   * @throws ConcurrentModificationException if a table is added to the
   *         database while an Iterator is in use.
   * @usage _general_method_
   */
  public Iterator<Table> iterator();

  /**
   * Convenience method for constructing a new TableIterableBuilder for this
   * cursor.  A TableIterableBuilder provides a variety of options for more
   * flexible iteration of Tables.
   */
  public TableIterableBuilder newIterable();
  
  /**
   * @param name Table name (case-insensitive)
   * @return The table, or null if it doesn't exist
   * @usage _general_method_
   */
  public Table getTable(String name) throws IOException;

  /**
   * Finds all the relationships in the database between the given tables.
   * @usage _intermediate_method_
   */
  public List<Relationship> getRelationships(Table table1, Table table2)
    throws IOException;

  /**
   * Finds all the relationships in the database for the given table.
   * @usage _intermediate_method_
   */
  public List<Relationship> getRelationships(Table table) throws IOException;

  /**
   * Finds all the relationships in the database in <i>non-system</i> tables.
   * </p>
   * Warning, this may load <i>all</i> the Tables (metadata, not data) in the
   * database which could cause memory issues.
   * @usage _intermediate_method_
   */
  public List<Relationship> getRelationships() throws IOException;

  /**
   * Finds <i>all</i> the relationships in the database, <i>including system
   * tables</i>.
   * </p>
   * Warning, this may load <i>all</i> the Tables (metadata, not data) in the
   * database which could cause memory issues.
   * @usage _intermediate_method_
   */
  public List<Relationship> getSystemRelationships()
    throws IOException;

  /**
   * Finds all the queries in the database.
   * @usage _intermediate_method_
   */
  public List<Query> getQueries() throws IOException;

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
  public Table getSystemTable(String tableName) throws IOException;

  /**
   * @return the core properties for the database
   * @usage _general_method_
   */
  public PropertyMap getDatabaseProperties() throws IOException;

  /**
   * @return the summary properties for the database
   * @usage _general_method_
   */
  public PropertyMap getSummaryProperties() throws IOException;

  /**
   * @return the user-defined properties for the database
   * @usage _general_method_
   */
  public PropertyMap getUserDefinedProperties() throws IOException;

  /**
   * @return the current database password, or {@code null} if none set.
   * @usage _general_method_
   */
  public String getDatabasePassword() throws IOException;

  /**
   * Create a new table in this database
   * @param name Name of the table to create in this database
   * @param linkedDbName path to the linked database
   * @param linkedTableName name of the table in the linked database
   * @usage _general_method_
   */
  public void createLinkedTable(String name, String linkedDbName,
                                String linkedTableName)
    throws IOException;

  /**
   * Flushes any current changes to the database file (and any linked
   * databases) to disk.
   * @usage _general_method_
   */
  public void flush() throws IOException;

  /**
   * Close the database file (and any linked databases).  A Database
   * <b>must</b> be closed after use or changes could be lost and the Database
   * file corrupted.  A Database instance should be treated like any other
   * external resource which would be closed in a finally block (e.g. an
   * OutputStream or jdbc Connection).
   * @usage _general_method_
   */
  public void close() throws IOException;

  /**
   * Gets the currently configured ErrorHandler (always non-{@code null}).
   * This will be used to handle all errors unless overridden at the Table or
   * Cursor level.
   * @usage _intermediate_method_
   */
  public ErrorHandler getErrorHandler();

  /**
   * Sets a new ErrorHandler.  If {@code null}, resets to the
   * {@link ErrorHandler#DEFAULT}.
   * @usage _intermediate_method_
   */
  public void setErrorHandler(ErrorHandler newErrorHandler);

  /**
   * Gets the currently configured LinkResolver (always non-{@code null}).
   * This will be used to handle all linked database loading.
   * @usage _intermediate_method_
   */
  public LinkResolver getLinkResolver();

  /**
   * Sets a new LinkResolver.  If {@code null}, resets to the
   * {@link LinkResolver#DEFAULT}.
   * @usage _intermediate_method_
   */
  public void setLinkResolver(LinkResolver newLinkResolver);

  /**
   * Returns an unmodifiable view of the currently loaded linked databases,
   * mapped from the linked database file name to the linked database.  This
   * information may be useful for implementing a LinkResolver.
   * @usage _intermediate_method_
   */
  public Map<String,Database> getLinkedDatabases();

  
  /**
   * Returns {@code true} if this Database links to the given Table, {@code
   * false} otherwise.
   * @usage _general_method_
   */
  public boolean isLinkedTable(Table table) throws IOException;
  
  /**
   * Gets currently configured TimeZone (always non-{@code null}).
   * @usage _intermediate_method_
   */
  public TimeZone getTimeZone();

  /**
   * Sets a new TimeZone.  If {@code null}, resets to the default value.
   * @usage _intermediate_method_
   */
  public void setTimeZone(TimeZone newTimeZone);

  /**
   * Gets currently configured Charset (always non-{@code null}).
   * @usage _intermediate_method_
   */
  public Charset getCharset();

  /**
   * Sets a new Charset.  If {@code null}, resets to the default value.
   * @usage _intermediate_method_
   */
  public void setCharset(Charset newCharset);

  /**
   * Gets currently configured {@link Table.ColumnOrder} (always non-{@code
   * null}).
   * @usage _intermediate_method_
   */
  public Table.ColumnOrder getColumnOrder();

  /**
   * Sets a new Table.ColumnOrder.  If {@code null}, resets to the default value.
   * @usage _intermediate_method_
   */
  public void setColumnOrder(Table.ColumnOrder newColumnOrder);

  /**
   * Gets current foreign-key enforcement policy.
   * @usage _intermediate_method_
   */
  public boolean isEnforceForeignKeys();

  /**
   * Sets a new foreign-key enforcement policy.  If {@code null}, resets to
   * the default value.
   * @usage _intermediate_method_
   */
  public void setEnforceForeignKeys(Boolean newEnforceForeignKeys);

  /**
   * Gets currently configured ColumnValidatorFactory (always non-{@code null}).
   * @usage _intermediate_method_
   */
  public ColumnValidatorFactory getColumnValidatorFactory();

  /**
   * Sets a new ColumnValidatorFactory.  If {@code null}, resets to the
   * default value.  The configured ColumnValidatorFactory will be used to
   * create ColumnValidator instances on any <i>user</i> tables loaded from
   * this point onward (this will not be used for system tables).
   * @usage _intermediate_method_
   */
  public void setColumnValidatorFactory(ColumnValidatorFactory newFactory);
  
  /**
   * Returns the FileFormat of this database (which may involve inspecting the
   * database itself).
   * @throws IllegalStateException if the file format cannot be determined
   * @usage _general_method_
   */
  public FileFormat getFileFormat() throws IOException;

}
