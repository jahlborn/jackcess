/*
Copyright (c) 2013 James Ahlborn

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

package com.healthmarketscience.jackcess.query;

import java.util.List;


/**
 * Base interface for queries which represent some form of SELECT statement.
 * 
 * @author James Ahlborn
 */
public interface BaseSelectQuery extends Query 
{

  public String getSelectType();

  public List<String> getSelectColumns();

  public List<String> getFromTables();

  public String getFromRemoteDbPath();

  public String getFromRemoteDbType();

  public String getWhereExpression();

  public List<String> getGroupings();

  public String getHavingExpression();

  public List<String> getOrderings();
}
