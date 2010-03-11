package com.healthmarketscience.jackcess;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;

/**
 * @author Dan Rollo
 *         Date: Mar 5, 2010
 *         Time: 2:21:22 PM
 */
public final class UsageMapTest extends TestCase {

    public void testRead() throws Exception {
        try {
            Database.open(JetFormatTest.DB_1997);
            fail("mdb v97 usage map unsupported");
        } catch (IOException e) {
            assertEquals(UsageMap.MSG_PREFIX_UNRECOGNIZED_MAP + 2, e.getMessage());
        }

        checkUsageMapRead(JetFormatTest.DB_2000, 743, 767);
        checkUsageMapRead(JetFormatTest.DB_2007, 42, 511);
    }

    private static void checkUsageMapRead(final File dbFile,
                                          final int expectedFirstPage, final int expectedLastPage)
            throws IOException {

        final Database db = Database.open(dbFile);
        final UsageMap usageMap = UsageMap.read(db,
                1, //PageChannel.PAGE_GLOBAL_USAGE_MAP,
                0, //PageChannel.ROW_GLOBAL_USAGE_MAP,
                true);
        assertEquals("Unexpected FirstPageNumber.", expectedFirstPage, usageMap.getFirstPageNumber());
        assertEquals("Unexpected LastPageNumber.", expectedLastPage, usageMap.getLastPageNumber());
    }
}
