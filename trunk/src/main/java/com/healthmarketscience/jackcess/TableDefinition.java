/*
Copyright (c) 2022 James Ahlborn

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
import java.time.LocalDateTime;
import java.util.List;

/**
 * The definition of a single database table.  A TableDefinition instance is
 * retrieved from a {@link TableMetaData} instance.  The TableDefinition
 * instance only provides access to the table metadata, but no table data.
 * <p>
 * A TableDefinition instance is not thread-safe (see {@link Database} for
 * more thread-safety details).
 *
 * @author James Ahlborn
 * @usage _intermediate_class_
 */
public interface TableDefinition
{
  /**
   * @return The name of the table
   * @usage _general_method_
   */
  public String getName();

  /**
   * Whether or not this table has been marked as hidden.
   * @usage _general_method_
   */
  public boolean isHidden();

  /**
   * Whether or not this table is a system (internal) table.
   * @usage _general_method_
   */
  public boolean isSystem();

  /**
   * @usage _general_method_
   */
  public int getColumnCount();

  /**
   * @usage _general_method_
   */
  public Database getDatabase();

  /**
   * @return All of the columns in this table (unmodifiable List)
   * @usage _general_method_
   */
  public List<? extends Column> getColumns();

  /**
   * @return the column with the given name
   * @usage _general_method_
   */
  public Column getColumn(String name);

  /**
   * @return the properties for this table
   * @usage _general_method_
   */
  public PropertyMap getProperties() throws IOException;

  /**
   * @return the created date for this table if available
   * @usage _general_method_
   */
  public LocalDateTime getCreatedDate() throws IOException;

  /**
   * Note: jackcess <i>does not automatically update the modified date of a
   * Table</i>.
   *
   * @return the last updated date for this table if available
   * @usage _general_method_
   */
  public LocalDateTime getUpdatedDate() throws IOException;

  /**
   * @return All of the Indexes on this table (unmodifiable List)
   * @usage _intermediate_method_
   */
  public List<? extends Index> getIndexes();

  /**
   * @return the index with the given name
   * @throws IllegalArgumentException if there is no index with the given name
   * @usage _intermediate_method_
   */
  public Index getIndex(String name);

  /**
   * @return the primary key index for this table
   * @throws IllegalArgumentException if there is no primary key index on this
   *         table
   * @usage _intermediate_method_
   */
  public Index getPrimaryKeyIndex();

  /**
   * @return the foreign key index joining this table to the given other table
   * @throws IllegalArgumentException if there is no relationship between this
   *         table and the given table
   * @usage _intermediate_method_
   */
  public Index getForeignKeyIndex(Table otherTable);
}
