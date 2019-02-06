/*
Copyright (c) 2011 James Ahlborn

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

package com.healthmarketscience.jackcess.util;

import java.io.File;
import java.io.IOException;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;

/**
 * Resolver for linked databases.
 *
 * @author James Ahlborn
 * @usage _intermediate_class_
 */
public interface LinkResolver
{
  /**
   * default link resolver used if none provided
   * @usage _general_field_
   */
  public static final LinkResolver DEFAULT = new LinkResolver() {
      @Override
      public Database resolveLinkedDatabase(Database linkerDb,
                                            String linkeeFileName)
        throws IOException
      {
        // if linker is read-only, open linkee read-only
        boolean readOnly = ((linkerDb instanceof DatabaseImpl) ?
                            ((DatabaseImpl)linkerDb).isReadOnly() : false);
        return new DatabaseBuilder(new File(linkeeFileName))
          .setReadOnly(readOnly).open();
      }
    };

  /**
   * Returns the appropriate Database instance for the linkeeFileName from the
   * given linkerDb.
   */
  public Database resolveLinkedDatabase(Database linkerDb, String linkeeFileName)
    throws IOException;
}
