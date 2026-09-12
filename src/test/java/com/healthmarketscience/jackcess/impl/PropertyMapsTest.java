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

package com.healthmarketscience.jackcess.impl;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.PropertyMap;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import junit.framework.TestCase;

/**
 * Tests that a property map carries the whole flag byte, which Access uses as
 * a bit field.  See {@link PropertyMapImpl#DDL_FLAG} and
 * {@link PropertyMapImpl#SKIP_HANDLER_FLAG}.
 *
 * @author James Ahlborn
 */
public class PropertyMapsTest extends TestCase
{
  /** the one property in the test databases which carries more than the ddl
      bit.  every version from 1997 to 2010 writes it with the same flags */
  private static final String FLAGGED_TABLE = "Table4";
  private static final String FLAGGED_PROP = "ColIsGuid";
  private static final byte FLAGGED_FLAGS =
    (byte)(PropertyMapImpl.DDL_FLAG | PropertyMapImpl.SKIP_HANDLER_FLAG);

  public PropertyMapsTest(String name) throws Exception {
    super(name);
  }

  public void testReadFlagByte() throws Exception
  {
    for(TestDB testDb : SUPPORTED_DBS_TEST) {
      Database db = open(testDb);

      PropertyMap props = db.getTable(FLAGGED_TABLE).getProperties();
      PropertyMap.Property prop = props.get(FLAGGED_PROP);

      assertEquals(FLAGGED_FLAGS, getFlags(prop));
      // the ddl bit is set too, so the boolean view stays true
      assertTrue(prop.isDdl());

      db.close();
    }
  }

  public void testWriteKeepsFlagByte() throws Exception
  {
    for(TestDB testDb : SUPPORTED_DBS_TEST) {
      Database db = open(testDb);

      PropertyMapImpl props =
        (PropertyMapImpl)db.getTable(FLAGGED_TABLE).getProperties();
      PropertyMaps maps = props.getOwner();

      PropertyMaps maps2 = ((DatabaseImpl)db).readProperties(
          maps.write(), maps.getObjectId(), null);

      PropertyMap.Property prop2 = maps2.getDefault().get(FLAGGED_PROP);

      assertEquals(FLAGGED_FLAGS, getFlags(prop2));

      db.close();
    }
  }

  public void testNewPropertyGetsDdlBitOnly() throws Exception
  {
    PropertyMapImpl map = new PropertyMaps(10, null, null, null).getDefault();

    assertEquals((byte)0x00, getFlags(map.put("plain", "value")));
    assertEquals((byte)0x01,
                 getFlags(map.put("ddl", null, "value", true)));
  }

  /**
   * A property map is identified by its name and its block type together.
   * Access names an index after its column by default, so a table commonly
   * holds a column block and an index block with the same name.
   */
  public void testIndexBlockIsSeparateFromColumnBlock() throws Exception
  {
    for(TestDB testDb : SUPPORTED_DBS_TEST) {
      Database db = open(testDb);

      PropertyMaps maps =
        ((PropertyMapImpl)db.getTable("Table1").getProperties()).getOwner();
      int origSize = maps.getSize();

      maps.get("Shared").put("ColProp", DataType.TEXT, "column");
      maps.getIndex("Shared").put("IdxProp", DataType.TEXT, "index");

      assertEquals(origSize + 2, maps.getSize());

      PropertyMaps maps2 = ((DatabaseImpl)db).readProperties(
          maps.write(), maps.getObjectId(), null);

      assertEquals("column", maps2.get("Shared").getValue("ColProp"));
      assertNull(maps2.get("Shared").getValue("IdxProp"));
      assertEquals("index", maps2.getIndex("Shared").getValue("IdxProp"));
      assertNull(maps2.getIndex("Shared").getValue("ColProp"));

      db.close();
    }
  }

  private static byte getFlags(PropertyMap.Property prop) {
    return ((PropertyMapImpl.PropertyImpl)prop).getFlags();
  }
}
