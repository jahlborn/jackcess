// Copyright (c) 2009 Boomi, Inc.

package com.healthmarketscience.jackcess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author James Ahlborn
 */
public class ImportUtil 
{

  private static final Log LOG = LogFactory.getLog(ImportUtil.class);

  /** Batch commit size for copying other result sets into this database */
  private static final int COPY_TABLE_BATCH_SIZE = 200;
  
  private ImportUtil() {}

  /**
   * Copy an existing JDBC ResultSet into a new table in this database
   * <p>
   * Equivalent to:
   * {@code  importResultSet(source, db, name, SimpleImportFilter.INSTANCE);}
   * 
   * @param name Name of the new table to create
   * @param source ResultSet to copy from
   *
   * @return the name of the copied table
   *
   * @see #importResultSet(ResultSet,Database,String,ImportFilter)
   */
  public static String importResultSet(ResultSet source, Database db,
                                       String name)
    throws SQLException, IOException
  {
    return importResultSet(source, db, name, SimpleImportFilter.INSTANCE);
  }
  
  /**
   * Copy an existing JDBC ResultSet into a new table in this database
   * <p>
   * Equivalent to:
   * {@code  importResultSet(source, db, name, filter, false);}
   * 
   * @param name Name of the new table to create
   * @param source ResultSet to copy from
   * @param filter valid import filter
   *
   * @return the name of the imported table
   *
   * @see #importResultSet(ResultSet,Database,String,ImportFilter,boolean)
   */
  public static String importResultSet(ResultSet source, Database db,
                                       String name, ImportFilter filter)
    throws SQLException, IOException
  {
    return importResultSet(source, db, name, filter, false);
  }
   
  /**
   * Copy an existing JDBC ResultSet into a new (or optionally existing) table
   * in this database
   * @param name Name of the new table to create
   * @param source ResultSet to copy from
   * @param filter valid import filter
   * @param useExistingTable if {@code true} use current table if it already
   *                         exists, otherwise, create new table with unique
   *                         name
   *
   * @return the name of the imported table
   */
  public static String importResultSet(ResultSet source, Database db,
                                       String name, ImportFilter filter,
                                       boolean useExistingTable)
    throws SQLException, IOException
  {
    ResultSetMetaData md = source.getMetaData();

    name = Database.escapeIdentifier(name);
    Table table = null;
    if(!useExistingTable || ((table = db.getTable(name)) == null)) {
      

      List<Column> columns = new LinkedList<Column>();
      for (int i = 1; i <= md.getColumnCount(); i++) {
        Column column = new Column();
        column.setName(Database.escapeIdentifier(md.getColumnName(i)));
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

      table = createUniqueTable(db, name, columns, md, filter);
    }

    List<Object[]> rows = new ArrayList<Object[]>(COPY_TABLE_BATCH_SIZE);
    int numColumns = md.getColumnCount();

    while (source.next()) {
      Object[] row = new Object[numColumns];
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

    return table.getName();
  }
  
  /**
   * Copy a delimited text file into a new table in this database
   * <p>
   * Equivalent to:
   * {@code  importFile(f, name, db, delim, SimpleImportFilter.INSTANCE);}
   * 
   * @param name Name of the new table to create
   * @param f Source file to import
   * @param delim Regular expression representing the delimiter string.
   *
   * @return the name of the imported table
   *
   * @see #importFile(File,Database,String,String,ImportFilter)
   */
  public static String importFile(File f, Database db, String name,
                                  String delim)
    throws IOException
  {
    return importFile(f, db, name, delim, SimpleImportFilter.INSTANCE);
  }

  /**
   * Copy a delimited text file into a new table in this database
   * <p>
   * Equivalent to:
   * {@code  importReader(new BufferedReader(new FileReader(f)), db, name, delim, filter);}
   * 
   * @param name Name of the new table to create
   * @param f Source file to import
   * @param delim Regular expression representing the delimiter string.
   * @param filter valid import filter
   *
   * @return the name of the imported table
   *
   * @see #importReader(BufferedReader,Database,String,String,ImportFilter)
   */
  public static String importFile(File f, Database db, String name,
                                  String delim, ImportFilter filter)
    throws IOException
  {
    BufferedReader in = null;
    try {
      in = new BufferedReader(new FileReader(f));
      return importReader(in, db, name, delim, filter);
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
   * <p>
   * Equivalent to:
   * {@code  importReader(in, db, name, delim, SimpleImportFilter.INSTANCE);}
   * 
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   *
   * @return the name of the imported table
   *
   * @see #importReader(BufferedReader,Database,String,String,ImportFilter)
   */
  public static String importReader(BufferedReader in, Database db, 
                                    String name, String delim)
    throws IOException
  {
    return importReader(in, db, name, delim, SimpleImportFilter.INSTANCE);
  }
  
  /**
   * Copy a delimited text file into a new table in this database
   * <p>
   * Equivalent to:
   * {@code  importReader(in, db, name, delim, filter, false);}
   * 
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   * @param filter valid import filter
   *
   * @return the name of the imported table
   *
   * @see #importReader(BufferedReader,Database,String,String,ImportFilter,boolean)
   */
  public static String importReader(BufferedReader in, Database db, 
                                    String name, String delim,
                                    ImportFilter filter)
    throws IOException
  {
    return importReader(in, db, name, delim, filter, false);
  }

  /**
   * Copy a delimited text file into a new (or optionally exixsting) table in
   * this database
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   * @param filter valid import filter
   * @param useExistingTable if {@code true} use current table if it already
   *                         exists, otherwise, create new table with unique
   *                         name
   *
   * @return the name of the imported table
   */
  public static String importReader(BufferedReader in, Database db, 
                                    String name, String delim,
                                    ImportFilter filter, 
                                    boolean useExistingTable)
    throws IOException
  {
    String line = in.readLine();
    if (line == null || line.trim().length() == 0) {
      return null;
    }

    try {
      name = Database.escapeIdentifier(name);
      Table table = null;
      if(!useExistingTable || ((table = db.getTable(name)) == null)) {

        List<Column> columns = new LinkedList<Column>();
        String[] columnNames = line.split(delim);
      
        for (int i = 0; i < columnNames.length; i++) {
          columns.add(new ColumnBuilder(columnNames[i], DataType.TEXT)
                      .escapeName()
                      .setLength((short)DataType.TEXT.getMaxSize())
                      .toColumn());
        }

        table = createUniqueTable(db, name, columns, null, filter);
      }

      List<Object[]> rows = new ArrayList<Object[]>(COPY_TABLE_BATCH_SIZE);
      int numColumns = table.getColumnCount();
      
      while ((line = in.readLine()) != null)
      {
        // 
        // Handle the situation where the end of the line
        // may have null fields.  We always want to add the
        // same number of columns to the table each time.
        //
        Object[] data = Table.dupeRow(line.split(delim), numColumns);
        rows.add(filter.filterRow(data));
        if (rows.size() == COPY_TABLE_BATCH_SIZE) {
          table.addRows(rows);
          rows.clear();
        }
      }
      if (rows.size() > 0) {
        table.addRows(rows);
      }

      return table.getName();

    } catch(SQLException e) {
      throw (IOException)new IOException(e.getMessage()).initCause(e);
    }
  }

  /**
   * Returns a new table with a unique name and the given table definition.
   */
  private static Table createUniqueTable(Database db, String name,
                                         List<Column> columns,
                                         ResultSetMetaData md, 
                                         ImportFilter filter)
    throws IOException, SQLException
  {
    // otherwise, find unique name and create new table
    String baseName = name;
    int counter = 2;
    while(db.getTable(name) != null) {
      name = baseName + (counter++);
    }
    
    db.createTable(name, filter.filterColumns(columns, md));

    return db.getTable(name);
  }

  

}
