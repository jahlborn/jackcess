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
import java.nio.file.AccessDeniedException;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;

/**
 * Resolver for linked databases.
 *
 * @author James Ahlborn
 * @usage _intermediate_class_
 */
@FunctionalInterface
public interface LinkResolver
{
  /**
   * Link resolver which opens whatever file name the linking database
   * specifies, including absolute, network (UNC) and device paths.
   * <p>
   * Since the linkee file name comes from the database file itself, a
   * database from an untrusted source can use this resolver to make jackcess
   * read an arbitrary local file or open a network connection.  Only use this
   * resolver when the linked database file names can be trusted.
   * @usage _general_field_
   */
  public static final LinkResolver UNRESTRICTED = (linkerDb, linkeeFileName) -> {
    // if linker is read-only, open linkee read-only
    boolean readOnly = ((linkerDb instanceof DatabaseImpl) &&
                        ((DatabaseImpl)linkerDb).isReadOnly());
    return new DatabaseBuilder(new File(linkeeFileName))
      .setReadOnly(readOnly).open();
  };

  /**
   * Link resolver which refuses to open any linked database.
   * <p>
   * This is the resolver used if none is provided (unless the
   * {@value com.healthmarketscience.jackcess.Database#ALLOW_LINK_RESOLUTION_PROPERTY}
   * system property is enabled, in which case {@link #UNRESTRICTED} is used
   * instead).  An application which uses linked databases must configure
   * either {@link #UNRESTRICTED} or a resolver which enforces its own policy
   * for which file names are acceptable.
   * @usage _general_field_
   */
  public static final LinkResolver DEFAULT = (linkerDb, linkeeFileName) -> {
    throw new AccessDeniedException(
        linkeeFileName, null,
        "Linked database resolution is disabled.  Configure a LinkResolver " +
        "on the Database (e.g. LinkResolver.UNRESTRICTED for trusted file " +
        "names) to enable it");
  };

  /**
   * Returns the appropriate Database instance for the linkeeFileName from the
   * given linkerDb.
   */
  public Database resolveLinkedDatabase(Database linkerDb, String linkeeFileName)
    throws IOException;
}
