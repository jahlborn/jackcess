/*
Copyright (c) 2011 James Ahlborn

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA
*/

package com.healthmarketscience.jackcess.util;

import java.io.File;
import java.io.IOException;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

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
      public Database resolveLinkedDatabase(Database linkerDb,
                                            String linkeeFileName)
        throws IOException
      {
        return DatabaseBuilder.open(new File(linkeeFileName));
      }
    };

  /**
   * Returns the appropriate Database instance for the linkeeFileName from the
   * given linkerDb.
   */
  public Database resolveLinkedDatabase(Database linkerDb, String linkeeFileName)
    throws IOException;
}
