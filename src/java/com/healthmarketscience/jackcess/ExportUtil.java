/*
Copyright (c) 2007 Health Market Science, Inc.

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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author Frank Gerbig
 */
public class ExportUtil {

  private static final Log LOG = LogFactory.getLog(ExportUtil.class);

  public static final String DEFAULT_DELIMITER = ",";
  public static final char DEFAULT_QUOTE_CHAR = '"';
  public static final String DEFAULT_FILE_EXT = "csv";


  private ExportUtil() {
  }

  /**
   * Copy all tables into new delimited text files <br>
   * Equivalent to: {@code exportAll(db, dir, "csv");}
   * 
   * @param db
   *          Database the table to export belongs to
   * @param dir
   *          The directory where the new files will be created
   * 
   * @see #exportAll(Database,File,String)
   */
  public static void exportAll(Database db, File dir)
      throws IOException {
    exportAll(db, dir, DEFAULT_FILE_EXT);
  }

  /**
   * Copy all tables into new delimited text files <br>
   * Equivalent to: {@code exportFile(db, name, f, false, null, '"',
   * SimpleExportFilter.INSTANCE);}
   * 
   * @param db
   *          Database the table to export belongs to
   * @param dir
   *          The directory where the new files will be created
   * @param ext
   *          The file extension of the new files
   * 
   * @see #exportFile(Database,String,File,boolean,String,char,ExportFilter)
   */
  public static void exportAll(Database db, File dir,
      String ext) throws IOException {
    for (String tableName : db.getTableNames()) {
      exportFile(db, tableName, new File(dir, tableName + "." + ext), false,
          DEFAULT_DELIMITER, DEFAULT_QUOTE_CHAR, SimpleExportFilter.INSTANCE);
    }
  }

  /**
   * Copy all tables into new delimited text files <br>
   * Equivalent to: {@code exportFile(db, name, f, false, null, '"',
   * SimpleExportFilter.INSTANCE);}
   * 
   * @param db
   *          Database the table to export belongs to
   * @param dir
   *          The directory where the new files will be created
   * @param ext
   *          The file extension of the new files
   * @param header
   *          If <code>true</code> the first line contains the column names
   * 
   * @see #exportFile(Database,String,File,boolean,String,char,ExportFilter)
   */
  public static void exportAll(Database db, File dir,
      String ext, boolean header)
      throws IOException {
    for (String tableName : db.getTableNames()) {
      exportFile(db, tableName, new File(dir, tableName + "." + ext), header,
          DEFAULT_DELIMITER, DEFAULT_QUOTE_CHAR, SimpleExportFilter.INSTANCE);
    }
  }

  /**
   * Copy all tables into new delimited text files <br>
   * Equivalent to: {@code exportFile(db, name, f, false, null, '"',
   * SimpleExportFilter.INSTANCE);}
   * 
   * @param db
   *          Database the table to export belongs to
   * @param dir
   *          The directory where the new files will be created
   * @param ext
   *          The file extension of the new files
   * @param header
   *          If <code>true</code> the first line contains the column names
   * @param delim
   *          The column delimiter, <code>null</code> for default (comma)
   * @param quote
   *          The quote character
   * @param filter
   *          valid export filter
   * 
   * @see #exportFile(Database,String,File,boolean,String,char,ExportFilter)
   */
  public static void exportAll(Database db, File dir,
      String ext, boolean header, String delim,
      char quote, ExportFilter filter)
      throws IOException {
    for (String tableName : db.getTableNames()) {
      exportFile(db, tableName, new File(dir, tableName + "." + ext), header,
          delim, quote, filter);
    }
  }

  /**
   * Copy a table into a new delimited text file <br>
   * Equivalent to: {@code exportFile(db, name, f, false, null, '"',
   * SimpleExportFilter.INSTANCE);}
   * 
   * @param db
   *          Database the table to export belongs to
   * @param tableName
   *          Name of the table to export
   * @param f
   *          New file to create
   * 
   * @see #exportFile(Database,String,File,boolean,String,char,ExportFilter)
   */
  public static void exportFile(Database db, String tableName,
      File f) throws IOException {
    exportFile(db, tableName, f, false, DEFAULT_DELIMITER, DEFAULT_QUOTE_CHAR, 
        SimpleExportFilter.INSTANCE);
  }

  /**
   * Copy a table into a new delimited text file <br>
   * Nearly equivalent to: {@code exportWriter(db, name, new BufferedWriter(f),
   * header, delim, quote, filter);}
   * 
   * @param db
   *          Database the table to export belongs to
   * @param tableName
   *          Name of the table to export
   * @param f
   *          New file to create
   * @param header
   *          If <code>true</code> the first line contains the column names
   * @param delim
   *          The column delimiter, <code>null</code> for default (comma)
   * @param quote
   *          The quote character
   * @param filter
   *          valid export filter
   * 
   * @see #exportWriter(Database,String,BufferedWriter,boolean,String,char,ExportFilter)
   */
  public static void exportFile(Database db, String tableName,
      File f, boolean header, String delim, char quote,
      ExportFilter filter) throws IOException {
    BufferedWriter out = null;
    try {
      out = new BufferedWriter(new FileWriter(f));
      exportWriter(db, tableName, out, header, delim, quote, filter);
      out.close();
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (Exception ex) {
          LOG.warn("Could not close file " + f.getAbsolutePath(), ex);
        }
      }
    }
  }

  /**
   * Copy a table in this database into a new delimited text file <br>
   * Equivalent to: {@code exportWriter(db, name, out, false, null, '"',
   * SimpleExportFilter.INSTANCE);}
   * 
   * @param db
   *          Database the table to export belongs to
   * @param tableName
   *          Name of the table to export
   * @param out
   *          Writer to export to
   * 
   * @see #exportWriter(Database,String,BufferedWriter,boolean,String,char,ExportFilter)
   */
  public static void exportWriter(Database db, String tableName,
      BufferedWriter out) throws IOException {
    exportWriter(db, tableName, out, false, DEFAULT_DELIMITER, 
                 DEFAULT_QUOTE_CHAR, SimpleExportFilter.INSTANCE);
  }

  /**
   * Copy a table in this database into a new delimited text file. <br>
   * Equivalent to: {@code exportWriter(Cursor.createCursor(db.getTable(tableName)), out, header, delim, quote, filter);}
   * 
   * @param db
   *          Database the table to export belongs to
   * @param tableName
   *          Name of the table to export
   * @param out
   *          Writer to export to
   * @param header
   *          If <code>true</code> the first line contains the column names
   * @param delim
   *          The column delimiter, <code>null</code> for default (comma)
   * @param quote
   *          The quote character
   * @param filter
   *          valid export filter
   * 
   * @see #exportWriter(Cursor,BufferedWriter,boolean,String,char,ExportFilter)
   */
  public static void exportWriter(Database db, String tableName,
      BufferedWriter out, boolean header, String delim,
      char quote, ExportFilter filter)
      throws IOException 
  {
    exportWriter(Cursor.createCursor(db.getTable(tableName)), out, header,
                 delim, quote, filter);
  }

  /**
   * Copy a table in this database into a new delimited text file.
   * 
   * @param cursor
   *          Cursor to export
   * @param out
   *          Writer to export to
   * @param header
   *          If <code>true</code> the first line contains the column names
   * @param delim
   *          The column delimiter, <code>null</code> for default (comma)
   * @param quote
   *          The quote character
   * @param filter
   *          valid export filter
   */
  public static void exportWriter(Cursor cursor,
      BufferedWriter out, boolean header, String delim,
      char quote, ExportFilter filter)
      throws IOException 
  {
    String delimiter = (delim == null) ? DEFAULT_DELIMITER : delim;

    // create pattern which will indicate whether or not a value needs to be
    // quoted or not (contains delimiter, separator, or newline)
    Pattern needsQuotePattern = Pattern.compile(
        "(?:" + Pattern.quote(delimiter) + ")|(?:" + 
        Pattern.quote("" + quote) + ")|(?:[\n\r])");

    List<Column> origCols = cursor.getTable().getColumns();
    List<Column> columns = new ArrayList<Column>(origCols);
    columns = filter.filterColumns(columns);

    Collection<String> columnNames = null;
    if(!origCols.equals(columns)) {

      // columns have been filtered
      columnNames = new HashSet<String>();
      for (Column c : columns) {
        columnNames.add(c.getName());
      }
    }

    // print the header row (if desired)
    if (header) {
      for (Iterator<Column> iter = columns.iterator(); iter.hasNext();) {

        writeValue(out, iter.next().getName(), quote, needsQuotePattern);

        if (iter.hasNext()) {
          out.write(delimiter);
        }
      }
      out.newLine();
    }

    // print the data rows
    Map<String, Object> row;
    Object[] unfilteredRowData = new Object[columns.size()];
    while ((row = cursor.getNextRow(columnNames)) != null) {

      // fill raw row data in array
      for (int i = 0; i < columns.size(); i++) {
        unfilteredRowData[i] = row.get(columns.get(i).getName());
      }

      // apply filter
      Object[] rowData = filter.filterRow(unfilteredRowData);

      // print row
      for (int i = 0; i < columns.size(); i++) {

        Object obj = rowData[i];
        if(obj != null) {

          String value = null;
          if(obj instanceof byte[]) {

            value = ByteUtil.toHexString((byte[])obj);

          } else {

            value = String.valueOf(rowData[i]);
          }

          writeValue(out, value, quote, needsQuotePattern);
        }

        if (i < columns.size() - 1) {
          out.write(delimiter);
        }
      }

      out.newLine();
    }

    out.flush();
  }

  private static void writeValue(BufferedWriter out, String value, char quote,
                                 Pattern needsQuotePattern) 
    throws IOException
  {
    if(!needsQuotePattern.matcher(value).find()) {

      // no quotes necessary
      out.write(value);
      return;
    }

    // wrap the value in quotes and handle internal quotes
    out.write(quote);
    for (int i = 0; i < value.length(); ++i) {
      char c = value.charAt(i);

      if (c == quote) {
        out.write(quote);
      }
      out.write(c);
    }
    out.write(quote);
  }

}
