/*
Copyright (c) 2016 James Ahlborn

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

import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.impl.TableMutator;

/**
 *
 * @author James Ahlborn
 */
public class TableModBuilder 
{
  private Table _table;

  public TableModBuilder(Table table) {
    _table = table;
  }

  public AddColumn addColumn(ColumnBuilder column) {
    return new AddColumn(column);
  }

  public AddIndex addIndex(IndexBuilder index) {
    return new AddIndex(index);
  }

  public class AddColumn
  {
    private ColumnBuilder _column;

    private AddColumn(ColumnBuilder column) {
      _column = column;
    }

    public Column add() throws IOException
    {
      return new TableMutator((TableImpl)_table).addColumn(_column);
    }
  }

  public class AddIndex
  {
    private IndexBuilder _index;

    private AddIndex(IndexBuilder index) {
      _index = index;
    }

    public Index add() throws IOException
    {
      return new TableMutator((TableImpl)_table).addIndex(_index);
    }
  }
}
