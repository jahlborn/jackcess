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

package com.healthmarketscience.jackcess;

import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 * Tests the column flag bits which mark a column the engine maintains and
 * hides, and a column which holds a windows security identifier.
 *
 * @author James Ahlborn
 */
public class ColumnFlagsTest
{
  @Test
  public void testHiddenAndSecurityIdentifierColumns() throws Exception
  {
    int numDbs = 0;
    for(TestDB testDb : SUPPORTED_DBS_TEST_FOR_READ) {
      ++numDbs;

      try (Database db = openMem(testDb)) {

        // a user table has neither bit anywhere
        for(Column col : db.getTable("Table1").getColumns()) {
          assertFalse(col.isHidden(), col.getName());
          assertFalse(col.isSecurityIdentifier(), col.getName());
        }

        // the system catalog is entirely engine maintained, and the two
        // columns which hold a windows SID are the only ones marked as such
        checkSystemTable(db, "MSysObjects", "Owner");
        checkSystemTable(db, "MSysACEs", "SID");
      }
    }
    assertTrue(numDbs > 0);
  }

  private static void checkSystemTable(Database db, String tableName,
                                       String sidColName)
    throws Exception
  {
    Table t = db.getSystemTable(tableName);
    assertNotNull(t, tableName);
    for(Column col : t.getColumns()) {
      assertTrue(col.isHidden(), tableName + "." + col.getName());
      assertEquals(sidColName.equals(col.getName()),
                   col.isSecurityIdentifier(),
                   tableName + "." + col.getName());
    }
  }
}
