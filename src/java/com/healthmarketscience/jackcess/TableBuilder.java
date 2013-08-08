/*
Copyright (c) 2008 Health Market Science, Inc.

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.jackcess.impl.DatabaseImpl;

/**
 * Builder style class for constructing a {@link Table}.
 * <p/>
 * Example:
 * <pre>
 *   Table table = new TableBuilder("Test")
 *     .addColumn(new ColumnBuilder("ID", DataType.LONG)
 *                .setAutoNumber(true))
 *     .addColumn(new ColumnBuilder("Name", DataType.TEXT))
 *     .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
 *               .addColumns("ID").setPrimaryKey())
 *     .toTable(db);
 * </pre>
 *
 * @author James Ahlborn
 */
public class TableBuilder {

  /** Prefix for column or table names that are reserved words */
  private static final String ESCAPE_PREFIX = "x";

  /* nested class for lazy loading */
  private static final class ReservedWords {
    /**
     * All of the reserved words in Access that should be escaped when creating
     * table or column names
     */
    private static final Set<String> VALUES = 
      new HashSet<String>(Arrays.asList(
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
       "yesno"));
  }


  /** name of the new table */
  private String _name;
  /** columns for the new table */
  private List<ColumnBuilder> _columns = new ArrayList<ColumnBuilder>();
  /** indexes for the new table */
  private List<IndexBuilder> _indexes = new ArrayList<IndexBuilder>();
  /** whether or not table/column/index names are automatically escaped */
  private boolean _escapeIdentifiers;
  

  public TableBuilder(String name) {
    this(name, false);
  }
  
  public TableBuilder(String name, boolean escapeIdentifiers) {
    _name = name;
    _escapeIdentifiers = escapeIdentifiers;
    if(_escapeIdentifiers) {
      _name = escapeIdentifier(_name);
    }
  }


  /**
   * Adds a Column to the new table.
   */
  public TableBuilder addColumn(ColumnBuilder column) {
    if(_escapeIdentifiers) {
      column.escapeName();
    }
    _columns.add(column);
    return this;
  }

  /**
   * Adds the Columns to the new table.
   */
  public TableBuilder addColumns(Collection<? extends ColumnBuilder> columns) {
    if(columns != null) {
      for(ColumnBuilder col : columns) {
        addColumn(col);
      }
    }
    return this;
  }

  /**
   * Adds an IndexBuilder to the new table.
   */
  public TableBuilder addIndex(IndexBuilder index) {
    if(_escapeIdentifiers) {
      index.setName(escapeIdentifier(index.getName()));
      for(IndexBuilder.Column col : index.getColumns()) {
        col.setName(escapeIdentifier(col.getName()));
      }
    }
    _indexes.add(index);
    return this;
  }

  /**
   * Sets whether or not subsequently added columns will have their names
   * automatically escaped
   */
  public TableBuilder setEscapeIdentifiers(boolean escapeIdentifiers) {
    _escapeIdentifiers = escapeIdentifiers;
    return this;
  }

  /**
   * Sets the names of the primary key columns for this table.  Convenience
   * method for creating a primary key index on a table.
   */
  public TableBuilder setPrimaryKey(String... colNames) {
    addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
             .addColumns(colNames).setPrimaryKey());
    return this;
  }
  
  /**
   * Escapes the new table's name using {@link TableBuilder#escapeIdentifier}.
   */
  public TableBuilder escapeName() {
    _name = escapeIdentifier(_name);
    return this;
  }

  /**
   * Creates a new Table in the given Database with the currently configured
   * attributes.
   */
  public Table toTable(Database db)
    throws IOException
  {
    ((DatabaseImpl)db).createTable(_name, _columns, _indexes);
    return db.getTable(_name);
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
    return ReservedWords.VALUES.contains(s.toLowerCase());
  }

  
}
