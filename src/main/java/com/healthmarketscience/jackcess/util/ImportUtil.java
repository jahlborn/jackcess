/*
Copyright (c) 2007 Health Market Science, Inc.

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

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.healthmarketscience.jackcess.impl.ByteUtil;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for importing tables to an Access database from other
 * sources.  See the {@link Builder} for convenient configuration of the
 * import functionality.  Note that most scenarios for customizing input data
 * can be handled by implementing a custom {@link ImportFilter}.
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public class ImportUtil
{
  /** Batch commit size for copying other result sets into this database */
  private static final int COPY_TABLE_BATCH_SIZE = 200;

  /** the platform line separator */
  static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private ImportUtil() {}

  /**
   * Returns a List of Column instances converted from the given
   * ResultSetMetaData (this is the same method used by the various {@code
   * importResultSet()} methods).
   *
   * @return a List of Columns
   */
  public static List<ColumnBuilder> toColumns(ResultSetMetaData md)
      throws SQLException, IOException
  {
      List<ColumnBuilder> columns = new ArrayList<ColumnBuilder>();
      for (int i = 1; i <= md.getColumnCount(); i++) {
        ColumnBuilder column = new ColumnBuilder(md.getColumnLabel(i))
          .escapeName();
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
      return columns;
  }

  /**
   * Copy an existing JDBC ResultSet into a new table in this database.
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
   * @see Builder
   */
  public static String importResultSet(ResultSet source, Database db,
                                       String name)
    throws SQLException, IOException
  {
    return importResultSet(source, db, name, SimpleImportFilter.INSTANCE);
  }

  /**
   * Copy an existing JDBC ResultSet into a new table in this database.
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
   * @see Builder
   */
  public static String importResultSet(ResultSet source, Database db,
                                       String name, ImportFilter filter)
    throws SQLException, IOException
  {
    return importResultSet(source, db, name, filter, false);
  }

  /**
   * Copy an existing JDBC ResultSet into a new (or optionally existing) table
   * in this database.
   *
   * @param name Name of the new table to create
   * @param source ResultSet to copy from
   * @param filter valid import filter
   * @param useExistingTable if {@code true} use current table if it already
   *                         exists, otherwise, create new table with unique
   *                         name
   *
   * @return the name of the imported table
   *
   * @see Builder
   */
  public static String importResultSet(ResultSet source, Database db,
                                       String name, ImportFilter filter,
                                       boolean useExistingTable)
    throws SQLException, IOException
  {
    ResultSetMetaData md = source.getMetaData();

    name = TableBuilder.escapeIdentifier(name);
    Table table = null;
    if(!useExistingTable || ((table = db.getTable(name)) == null)) {
      List<ColumnBuilder> columns = toColumns(md);
      table = createUniqueTable(db, name, columns, md, filter);
    }

    List<Object[]> rows = new ArrayList<Object[]>(COPY_TABLE_BATCH_SIZE);
    int numColumns = md.getColumnCount();

    while (source.next()) {
      Object[] row = new Object[numColumns];
      for (int i = 0; i < row.length; i++) {
        row[i] = source.getObject(i + 1);
      }
      row = filter.filterRow(row);
      if(row == null) {
        continue;
      }
      rows.add(row);
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
   * Copy a delimited text file into a new table in this database.
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
   * @see Builder
   */
  public static String importFile(File f, Database db, String name,
                                  String delim)
    throws IOException
  {
    return importFile(f, db, name, delim, SimpleImportFilter.INSTANCE);
  }

  /**
   * Copy a delimited text file into a new table in this database.
   * <p>
   * Equivalent to:
   * {@code  importFile(f, name, db, delim, "'", filter, false);}
   *
   * @param name Name of the new table to create
   * @param f Source file to import
   * @param delim Regular expression representing the delimiter string.
   * @param filter valid import filter
   *
   * @return the name of the imported table
   *
   * @see #importReader(BufferedReader,Database,String,String,ImportFilter)
   * @see Builder
   */
  public static String importFile(File f, Database db, String name,
                                  String delim, ImportFilter filter)
    throws IOException
  {
    return importFile(f, db, name, delim, ExportUtil.DEFAULT_QUOTE_CHAR,
                      filter, false);
  }

  /**
   * Copy a delimited text file into a new table in this database.
   * <p>
   * Equivalent to:
   * {@code  importReader(new BufferedReader(new FileReader(f)), db, name, delim, "'", filter, useExistingTable, true);}
   *
   * @param name Name of the new table to create
   * @param f Source file to import
   * @param delim Regular expression representing the delimiter string.
   * @param quote the quote character
   * @param filter valid import filter
   * @param useExistingTable if {@code true} use current table if it already
   *                         exists, otherwise, create new table with unique
   *                         name
   *
   * @return the name of the imported table
   *
   * @see #importReader(BufferedReader,Database,String,String,ImportFilter,boolean)
   * @see Builder
   */
  public static String importFile(File f, Database db, String name,
                                  String delim, char quote,
                                  ImportFilter filter,
                                  boolean useExistingTable)
    throws IOException
  {
    return importFile(f, db, name, delim, quote, filter, useExistingTable, true);
  }

  /**
   * Copy a delimited text file into a new table in this database.
   * <p>
   * Equivalent to:
   * {@code  importReader(new BufferedReader(new FileReader(f)), db, name, delim, "'", filter, useExistingTable, header);}
   *
   * @param name Name of the new table to create
   * @param f Source file to import
   * @param delim Regular expression representing the delimiter string.
   * @param quote the quote character
   * @param filter valid import filter
   * @param useExistingTable if {@code true} use current table if it already
   *                         exists, otherwise, create new table with unique
   *                         name
   * @param header if {@code false} the first line is not a header row, only
   *               valid if useExistingTable is {@code true}
   * @return the name of the imported table
   *
   * @see #importReader(BufferedReader,Database,String,String,char,ImportFilter,boolean,boolean)
   * @see Builder
   */
  public static String importFile(File f, Database db, String name,
                                  String delim, char quote,
                                  ImportFilter filter,
                                  boolean useExistingTable,
                                  boolean header)
    throws IOException
  {
    BufferedReader in = null;
    try {
      in = new BufferedReader(new FileReader(f));
      return importReader(in, db, name, delim, quote, filter,
                          useExistingTable, header);
    } finally {
      ByteUtil.closeQuietly(in);
    }
  }

  /**
   * Copy a delimited text file into a new table in this database.
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
   * @see Builder
   */
  public static String importReader(BufferedReader in, Database db,
                                    String name, String delim)
    throws IOException
  {
    return importReader(in, db, name, delim, SimpleImportFilter.INSTANCE);
  }

  /**
   * Copy a delimited text file into a new table in this database.
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
   * @see Builder
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
   * this database.
   * <p>
   * Equivalent to:
   * {@code  importReader(in, db, name, delim, '"', filter, false);}
   *
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   * @param filter valid import filter
   * @param useExistingTable if {@code true} use current table if it already
   *                         exists, otherwise, create new table with unique
   *                         name
   *
   * @return the name of the imported table
   *
   * @see Builder
   */
  public static String importReader(BufferedReader in, Database db,
                                    String name, String delim,
                                    ImportFilter filter,
                                    boolean useExistingTable)
    throws IOException
  {
    return importReader(in, db, name, delim, ExportUtil.DEFAULT_QUOTE_CHAR,
                        filter, useExistingTable);
  }

  /**
   * Copy a delimited text file into a new (or optionally exixsting) table in
   * this database.
   * <p>
   * Equivalent to:
   * {@code  importReader(in, db, name, delim, '"', filter, useExistingTable, true);}
   *
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   * @param quote the quote character
   * @param filter valid import filter
   * @param useExistingTable if {@code true} use current table if it already
   *                         exists, otherwise, create new table with unique
   *                         name
   *
   * @return the name of the imported table
   *
   * @see Builder
   */
  public static String importReader(BufferedReader in, Database db,
                                    String name, String delim, char quote,
                                    ImportFilter filter,
                                    boolean useExistingTable)
    throws IOException
  {
    return importReader(in, db, name, delim, quote, filter, useExistingTable,
                        true);
  }

  /**
   * Copy a delimited text file into a new (or optionally exixsting) table in
   * this database.
   *
   * @param name Name of the new table to create
   * @param in Source reader to import
   * @param delim Regular expression representing the delimiter string.
   * @param quote the quote character
   * @param filter valid import filter
   * @param useExistingTable if {@code true} use current table if it already
   *                         exists, otherwise, create new table with unique
   *                         name
   * @param header if {@code false} the first line is not a header row, only
   *               valid if useExistingTable is {@code true}
   *
   * @return the name of the imported table
   *
   * @see Builder
   */
  public static String importReader(BufferedReader in, Database db,
                                    String name, String delim, char quote,
                                    ImportFilter filter,
                                    boolean useExistingTable, boolean header)
    throws IOException
  {
    String line = in.readLine();
    if(StringUtil.isBlank(line)) {
      return null;
    }

    Pattern delimPat = Pattern.compile(delim);

    try {
      name = TableBuilder.escapeIdentifier(name);
      Table table = null;
      if(!useExistingTable || ((table = db.getTable(name)) == null)) {

        List<ColumnBuilder> columns = new ArrayList<ColumnBuilder>();
        Object[] columnNames = splitLine(line, delimPat, quote, in, 0);

        for (int i = 0; i < columnNames.length; i++) {
          columns.add(new ColumnBuilder((String)columnNames[i], DataType.TEXT)
                      .escapeName()
                      .setLength((short)DataType.TEXT.getMaxSize())
                      .toColumn());
        }

        table = createUniqueTable(db, name, columns, null, filter);

        // the first row was a header row
        header = true;
      }

      List<Object[]> rows = new ArrayList<Object[]>(COPY_TABLE_BATCH_SIZE);
      int numColumns = table.getColumnCount();

      if(!header) {
        // first line is _not_ a header line
        Object[] data = splitLine(line, delimPat, quote, in, numColumns);
        data = filter.filterRow(data);
        if(data != null) {
          rows.add(data);
        }
      }

      while ((line = in.readLine()) != null)
      {
        Object[] data = splitLine(line, delimPat, quote, in, numColumns);
        data = filter.filterRow(data);
        if(data == null) {
          continue;
        }
        rows.add(data);
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
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * Splits the given line using the given delimiter pattern and quote
   * character.  May read additional lines for quotes spanning newlines.
   */
  private static Object[] splitLine(String line, Pattern delim, char quote,
                                    BufferedReader in, int numColumns)
    throws IOException
  {
    List<String> tokens = new ArrayList<String>();
    StringBuilder sb = new StringBuilder();
    Matcher m = delim.matcher(line);
    int idx = 0;

    while(idx < line.length()) {

      if(line.charAt(idx) == quote) {

        // find quoted value
        sb.setLength(0);
        ++idx;
        while(true) {

          int endIdx = line.indexOf(quote, idx);

          if(endIdx >= 0) {

            sb.append(line, idx, endIdx);
            ++endIdx;
            if((endIdx < line.length()) && (line.charAt(endIdx) == quote)) {

              // embedded quote
              sb.append(quote);
              // keep searching
              idx = endIdx + 1;

            } else {

              // done
              idx = endIdx;
              break;
            }

          } else {

            // line wrap
            sb.append(line, idx, line.length());
            sb.append(LINE_SEPARATOR);

            idx = 0;
            line = in.readLine();
            if(line == null) {
              throw new EOFException("Missing end of quoted value " + sb);
            }
          }
        }

        tokens.add(sb.toString());

        // skip next delim
        idx = (m.find(idx) ? m.end() : line.length());

      } else if(m.find(idx)) {

        // next unquoted value
        tokens.add(line.substring(idx, m.start()));
        idx = m.end();

      } else {

        // trailing token
        tokens.add(line.substring(idx));
        idx = line.length();
      }
    }

    return tokens.toArray(new Object[Math.max(tokens.size(), numColumns)]);
  }

  /**
   * Returns a new table with a unique name and the given table definition.
   */
  private static Table createUniqueTable(Database db, String name,
                                         List<ColumnBuilder> columns,
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

    return new TableBuilder(name)
      .addColumns(filter.filterColumns(columns, md))
      .toTable(db);
  }

  /**
   * Builder which simplifies configuration of an import operation.
   */
  public static class Builder
  {
    private Database _db;
    private String _tableName;
    private String _delim = ExportUtil.DEFAULT_DELIMITER;
    private char _quote = ExportUtil.DEFAULT_QUOTE_CHAR;
    private ImportFilter _filter = SimpleImportFilter.INSTANCE;
    private boolean _useExistingTable;
    private boolean _header = true;

    public Builder(Database db) {
      this(db, null);
    }

    public Builder(Database db, String tableName) {
      _db = db;
      _tableName = tableName;
    }

    public Builder setDatabase(Database db) {
      _db = db;
      return this;
    }

    public Builder setTableName(String tableName) {
      _tableName = tableName;
      return this;
    }

    public Builder setDelimiter(String delim) {
      _delim = delim;
      return this;
    }

    public Builder setQuote(char quote) {
      _quote = quote;
      return this;
    }

    public Builder setFilter(ImportFilter filter) {
      _filter = filter;
      return this;
    }

    public Builder setUseExistingTable(boolean useExistingTable) {
      _useExistingTable = useExistingTable;
      return this;
    }

    public Builder setHeader(boolean header) {
      _header = header;
      return this;
    }

    /**
     * @see ImportUtil#importResultSet(ResultSet,Database,String,ImportFilter,boolean)
     */
    public String importResultSet(ResultSet source)
      throws SQLException, IOException
    {
      return ImportUtil.importResultSet(source, _db, _tableName, _filter,
                                        _useExistingTable);
    }

    /**
     * @see ImportUtil#importFile(File,Database,String,String,char,ImportFilter,boolean,boolean)
     */
    public String importFile(File f) throws IOException {
      return ImportUtil.importFile(f, _db, _tableName, _delim, _quote, _filter,
                                   _useExistingTable, _header);
    }

    /**
     * @see ImportUtil#importReader(BufferedReader,Database,String,String,char,ImportFilter,boolean,boolean)
     */
    public String importReader(BufferedReader reader) throws IOException {
      return ImportUtil.importReader(reader, _db, _tableName, _delim, _quote,
                                     _filter, _useExistingTable, _header);
    }
  }

}
