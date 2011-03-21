package com.healthmarketscience.jackcess;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;

import static com.healthmarketscience.jackcess.JetFormatTest.*;

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
            } else {
                throw new IllegalAccessException("Unknown file format: " + expectedFileFormat);
            }
            checkUsageMapRead(testDB.getFile(), expectedFirstPage, expectedLastPage);
        }
    }

    private static void checkUsageMapRead(final File dbFile,
                                          final int expectedFirstPage, final int expectedLastPage)
            throws IOException {

        final Database db = Database.open(dbFile);
        final UsageMap usageMap = UsageMap.read(db,
                PageChannel.PAGE_GLOBAL_USAGE_MAP,
                PageChannel.ROW_GLOBAL_USAGE_MAP,
                true);
        assertEquals("Unexpected FirstPageNumber.", expectedFirstPage, usageMap.getFirstPageNumber());
        assertEquals("Unexpected LastPageNumber.", expectedLastPage, usageMap.getLastPageNumber());
    }
}
