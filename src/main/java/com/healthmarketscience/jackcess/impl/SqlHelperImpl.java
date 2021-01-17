/*
Copyright (c) 2021 James Ahlborn

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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Implementation of SqlHelperImpl which works with the java.sql modules
 * classes.  This class is used if the java.sql module is enabled in the
 * application.
 *
 * @author James Ahlborn
 */
public class SqlHelperImpl extends SqlHelper {

  public SqlHelperImpl() {}

  @Override
  public boolean isBlob(Object value) {
    return (value instanceof Blob);
  }

  @Override
  public byte[] getBlobBytes(Object value) throws IOException {
    try {
      Blob b = (Blob)value;
      // note, start pos is 1-based
      return b.getBytes(1L, (int)b.length());
    } catch(SQLException e) {
      throw (IOException)(new IOException(e.getMessage())).initCause(e);
    }
  }

  @Override
  public boolean isClob(Object value) {
    return (value instanceof Clob);
  }

  @Override
  public CharSequence getClobString(Object value) throws IOException {
    try {
      Clob c = (Clob)value;
      // note, start pos is 1-based
      return c.getSubString(1L, (int)c.length());
    } catch(SQLException e) {
      throw (IOException)(new IOException(e.getMessage())).initCause(e);
    }
  }

  @Override
  public Integer getNewSqlType(String typeName) throws Exception {
    java.lang.reflect.Field sqlTypeField = Types.class.getField(typeName);
    return (Integer)sqlTypeField.get(null);
  }
}
