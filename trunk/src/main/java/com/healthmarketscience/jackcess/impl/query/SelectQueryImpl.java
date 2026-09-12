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

package com.healthmarketscience.jackcess.impl.query;

import java.util.List;
import com.healthmarketscience.jackcess.query.SelectQuery;


/**
 * Concrete Query subclass which represents a select query, e.g.:
 * {@code SELECT <columns> FROM <tables> WHERE <expression>}
 * 
 * @author James Ahlborn
 */
public class SelectQueryImpl extends BaseSelectQueryImpl implements SelectQuery
{

  public SelectQueryImpl(String name, List<Row> rows, int objectId, 
                         int objectFlag) {
    super(name, rows, objectId, objectFlag, Type.SELECT);
  }

  @Override
  protected void toSQLString(StringBuilder builder)
  {
    toSQLSelectString(builder, true);
  }  

}
