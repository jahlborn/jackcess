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

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.nio.charset.Charset;


/**
 * Common helper class used to maintain state during database mutation.
 *
 * @author James Ahlborn
 */
abstract class DBMutator 
{
  private final DatabaseImpl _database;

  protected DBMutator(DatabaseImpl database) {
    _database = database;
  }

  public DatabaseImpl getDatabase() {
    return _database;
  }

  public JetFormat getFormat() {
    return _database.getFormat();
  }

  public PageChannel getPageChannel() {
    return _database.getPageChannel();
  }

  public Charset getCharset() {
    return _database.getCharset();
  }

  public int reservePageNumber() throws IOException {
    return getPageChannel().allocateNewPage();
  }

  public static int calculateNameLength(String name) {
    return (name.length() * JetFormat.TEXT_FIELD_UNIT_SIZE) + 2;
  }

  protected ColumnImpl.SortOrder getDbSortOrder() {
    try {
      return _database.getDefaultSortOrder();
    } catch(IOException e) {
      // ignored, just use the jet format default
    }
    return null;
  }
}
