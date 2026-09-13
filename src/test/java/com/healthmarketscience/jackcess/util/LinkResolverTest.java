/*
Copyright (c) 2026 James Ahlborn

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
import java.nio.file.AccessDeniedException;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import static com.healthmarketscience.jackcess.TestUtil.*;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.SystemConfig;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author James Ahlborn
 */
public class LinkResolverTest
{
  /** the shapes of linkee file name which a linking database may specify */
  private static final String[] LINKEE_NAMES = {
    "linked.accdb",
    "../outside.accdb",
    "..\\outside.accdb",
    "C:\\data\\linked.accdb",
    "\\\\server\\share\\linked.accdb",
    "//server/share/linked.accdb",
    "\\\\?\\UNC\\server\\share\\linked.accdb",
    "\\\\.\\linked.accdb"
  };

  @Test
  public void testDefaultRejectsAllLinkees() throws Exception {
    try(Database db = createMem(FileFormat.V2010)) {
      for(String linkeeName : LINKEE_NAMES) {
        try {
          LinkResolver.DEFAULT.resolveLinkedDatabase(db, linkeeName);
          fail("AccessDeniedException should have been thrown for " +
               linkeeName);
        } catch(AccessDeniedException e) {
          assertEquals(linkeeName, e.getFile());
        }
      }
    }
  }

  @Test
  public void testLinkedTableDeniedByDefault() throws Exception {
    String linkeeName = "\\\\server\\share\\linked.accdb";

    try(Database db = createMem(FileFormat.V2010)) {
      db.createLinkedTable("RemoteTable", linkeeName, "Table1");

      // no LinkResolver has been configured, so loading the linked table must
      // not open the (network) path named by the database
      try {
        db.getTable("RemoteTable");
        fail("AccessDeniedException should have been thrown");
      } catch(AccessDeniedException e) {
        assertEquals(linkeeName, e.getFile());
      }

      assertTrue(db.getLinkedDatabases().isEmpty());
    }
  }

  @Test
  public void testUnrestrictedOpensLinkee() throws Exception {
    File linkeeFile = null;
    try(Database linkeeDb = createFile(FileFormat.V2010)) {
      linkeeFile = linkeeDb.getFile();
    }

    try(Database db = createMem(FileFormat.V2010);
        Database linkeeDb = LinkResolver.UNRESTRICTED.resolveLinkedDatabase(
            db, linkeeFile.getPath())) {
      assertEquals(linkeeFile.getCanonicalFile(),
                   linkeeDb.getFile().getCanonicalFile());
    }
  }

  @Test
  @SuppressWarnings("try")
  public void testAllowLinkResolutionProperty() throws Exception {
    assertSame(LinkResolver.DEFAULT, DatabaseImpl.getDefaultLinkResolver());

    try(AutoCloseable p = SystemConfig.withProperty(
            Database.ALLOW_LINK_RESOLUTION_PROPERTY, "true")) {
      assertSame(LinkResolver.UNRESTRICTED,
                 DatabaseImpl.getDefaultLinkResolver());
    }

    assertSame(LinkResolver.DEFAULT, DatabaseImpl.getDefaultLinkResolver());
  }
}
