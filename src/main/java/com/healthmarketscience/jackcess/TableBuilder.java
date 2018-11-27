/*
Copyright (c) 2008 Health Market Science, Inc.

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

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.PropertyMapImpl;
import com.healthmarketscience.jackcess.impl.TableCreator;

/**
 * Builder style class for constructing a {@link Table}.
 * <br>
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
 * @see ColumnBuilder
 * @see IndexBuilder
 * @see RelationshipBuilder
 * @usage _general_class_
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
  /** table properties (if any) */
  private Map<String,PropertyMap.Property> _props;

  
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

  public String getName() {
    return _name;
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

  public List<ColumnBuilder> getColumns() {
    return _columns;
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
   * Adds the Indexes to the new table.
   */
  public TableBuilder addIndexes(Collection<? extends IndexBuilder> indexes) {
    if(indexes != null) {
      for(IndexBuilder col : indexes) {
        addIndex(col);
      }
    }
    return this;
  }

  public List<IndexBuilder> getIndexes() {
    return _indexes;
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
   * Sets the table property with the given name to the given value.  Attempts
   * to determine the type of the property (see
   * {@link PropertyMap#put(String,Object)} for details on determining the
   * property type).
   */
  public TableBuilder putProperty(String name, Object value) {
    return putProperty(name, null, value);
  }
  
  /**
   * Sets the table property with the given name and type to the given value.
   */
  public TableBuilder putProperty(String name, DataType type, Object value) {
    if(_props == null) {
      _props = new HashMap<String,PropertyMap.Property>();
    }
    _props.put(name, PropertyMapImpl.createProperty(name, type, value));
    return this;
  }

  public Map<String,PropertyMap.Property> getProperties() {
    return _props;
  }

  /**
   * Creates a new Table in the given Database with the currently configured
   * attributes.
   */
  public Table toTable(Database db) throws IOException {
    return new TableCreator(((DatabaseImpl)db)).createTable(this);
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
