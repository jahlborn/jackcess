package com.healthmarketscience.jackcess.impl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;

/**
 * @author Dan Rollo
 *         Date: Mar 5, 2010
 *         Time: 2:21:22 PM
 */
public final class UsageMapTest extends TestCase {

  public void testRead() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST) {
      final int expectedFirstPage;
      final int expectedLastPage;
      final Database.FileFormat expectedFileFormat = testDB.getExpectedFileFormat();
      if (Database.FileFormat.V2000.equals(expectedFileFormat)) {
        expectedFirstPage = 743;
        expectedLastPage = 767;
      } else if (Database.FileFormat.V2003.equals(expectedFileFormat)) {
        expectedFirstPage = 16;
        expectedLastPage = 799;
      } else if (Database.FileFormat.V2007.equals(expectedFileFormat)) {
        expectedFirstPage = 94;
        expectedLastPage = 511;
      } else if (Database.FileFormat.V2010.equals(expectedFileFormat)) {
        expectedFirstPage = 109;
        expectedLastPage = 511;
      } else {
        throw new IllegalAccessException("Unknown file format: " + expectedFileFormat);
      }
      checkUsageMapRead(testDB.getFile(), expectedFirstPage, expectedLastPage);
    }
  }

  private static void checkUsageMapRead(
      final File dbFile, final int expectedFirstPage, final int expectedLastPage)
    throws IOException {

    final Database db = DatabaseBuilder.open(dbFile);
    final UsageMap usageMap = UsageMap.read((DatabaseImpl)db,
                                            PageChannel.PAGE_GLOBAL_USAGE_MAP,
                                            PageChannel.ROW_GLOBAL_USAGE_MAP,
                                            true);
    assertEquals("Unexpected FirstPageNumber.", expectedFirstPage, 
                 usageMap.getFirstPageNumber());
    assertEquals("Unexpected LastPageNumber.", expectedLastPage, 
                 usageMap.getLastPageNumber());
  }

  public void testGobalReferenceUsageMap() throws Exception
  {
    Database db = openCopy(
        Database.FileFormat.V2000, 
        new File("src/test/data/V2000/testRefGlobalV2000.mdb"));

    Table t = new TableBuilder("Test2")
      .addColumn(new ColumnBuilder("id", DataType.LONG))
      .addColumn(new ColumnBuilder("data1", DataType.TEXT))
      .addColumn(new ColumnBuilder("data2", DataType.TEXT))
      .toTable(db);


    ((DatabaseImpl)db).getPageChannel().startWrite();
    try {
      List<Object[]> rows = new ArrayList<Object[]>();
      for(int i = 0; i < 300000; ++i) {
        String s1 = "r" + i + "-" + createString(100);
        String s2 = "r" + i + "-" + createString(200);

        rows.add(new Object[]{i, s1, s2});

        if((i % 2000) == 0) {
          t.addRows(rows);
          rows.clear();
        }
      }
    } finally {
      ((DatabaseImpl)db).getPageChannel().finishWrite();
    }

    db.close();
  }

  public void testPromoteGlobalUsageMapToReference() throws Exception
  {
    Database db = createFile(Database.FileFormat.V2003);
    File dbFile = db.getFile();

    Table t = new TableBuilder("Test")
      .addColumn(new ColumnBuilder("id", DataType.LONG))
      .addColumn(new ColumnBuilder("data1", DataType.TEXT))
      .addColumn(new ColumnBuilder("data2", DataType.TEXT))
      .toTable(db);

    // add enough rows to grow the database well beyond the ~512 page limit of
    // an inline global usage map, which should force the global usage map to
    // be promoted to a reference usage map
    int numRows = 20000;
    ((DatabaseImpl)db).getPageChannel().startWrite();
    try {
      List<Object[]> rows = new ArrayList<Object[]>();
      for(int i = 0; i < numRows; ++i) {
        rows.add(new Object[]{i, "r" + i + "-" + createString(100),
                              "r" + i + "-" + createString(200)});
        if((i % 2000) == 0) {
          t.addRows(rows);
          rows.clear();
        }
      }
      t.addRows(rows);
    } finally {
      ((DatabaseImpl)db).getPageChannel().finishWrite();
    }
    db.close();

    // reopen and verify the global usage map is now a reference map which
    // covers the entire database (starting from page 0), and that all the
    // data is still readable
    Database db2 = DatabaseBuilder.open(dbFile);
    try {
      UsageMap gmap = UsageMap.read((DatabaseImpl)db2,
                                    PageChannel.PAGE_GLOBAL_USAGE_MAP,
                                    PageChannel.ROW_GLOBAL_USAGE_MAP, true);
      assertEquals("global usage map should be promoted to a reference map",
                   "GlobalReferenceHandler", getHandlerName(gmap));
      assertEquals("global reference map should start at page 0",
                   0, gmap.getStartPage());

      int count = 0;
      for(Row r : db2.getTable("Test")) {
        ++count;
      }
      assertEquals(numRows, count);
    } finally {
      db2.close();
    }
  }

  private static String getHandlerName(UsageMap usageMap) throws Exception {
    Field f = UsageMap.class.getDeclaredField("_handler");
    f.setAccessible(true);
    return f.get(usageMap).getClass().getSimpleName();
  }
}
