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

/**
 * Helper class to isolate the java.sql module interactions from the core of
 * jackcess (in java 9+ environments).  If the module is enabled (indicating
 * that the application is already using sql constructs), then jackcess will
 * seamlessly interact with sql types.  If the module is not enabled
 * (indicating that the application is not using any sql constructs), then
 * jackcess will not require the module in order to function otherwise
 * normally.
 *
 * This base class is the "fallback" class if the java.sql module is not
 * available.
 *
 * @author James Ahlborn
 */
public class SqlHelper {

  public static final SqlHelper INSTANCE = loadInstance();

  public SqlHelper() {}

  public boolean isBlob(Object value) {
    return false;
  }

  public byte[] getBlobBytes(Object value) throws IOException {
    throw new UnsupportedOperationException();
  }

  public boolean isClob(Object value) {
    return false;
  }

  public CharSequence getClobString(Object value) throws IOException {
    throw new UnsupportedOperationException();
  }

  public Integer getNewSqlType(String typeName) throws Exception {
    throw new UnsupportedOperationException();
  }

  private static final SqlHelper loadInstance() {
    // attempt to load the implementation of this class which works with
    // java.sql classes.  if that fails, use this fallback instance instead.
    try {
      return (SqlHelper)
          Class.forName("com.healthmarketscience.jackcess.impl.SqlHelperImpl")
          .newInstance();
    } catch(Throwable ignored) {}
    return new SqlHelper();
  }
}
