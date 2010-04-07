package com.healthmarketscience.jackcess;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author Dan Rollo
 *         Date: Mar 5, 2010
 *         Time: 12:44:21 PM
 */
public final class JetFormatTest extends TestCase {

    private static final File DIR_TEST_DATA = new File("test/data");

    /**
     * Defines known valid db test file base names.
     */
    public static enum Basename {

        BIG_INDEX("bigIndexTest"),
        COMP_INDEX("compIndexTest"),
        DEL_COL("delColTest"),
        DEL("delTest"),
        FIXED_NUMERIC("fixedNumericTest"),
        FIXED_TEXT("fixedTextTest"),
        INDEX_CURSOR("indexCursorTest"),
        INDEX("indexTest"),
        OVERFLOW("overflowTest"),
        QUERY("queryTest"),
        TEST("test"),
        TEST2("test2"),
        INDEX_CODES("testIndexCodes"),
        INDEX_PROPERTIES("testIndexProperties"),
        PROMOTION("testPromotion"),
        ;

        private final String basename;

        Basename(final String fileBasename) {
          basename = fileBasename;
        }

    }

    /** Defines currently supported db file formats. */
    final static Database.FileFormat[] SUPPORTED_FILEFORMATS = new Database.FileFormat[] {
            Database.FileFormat.V2000,
            Database.FileFormat.V2003,
            Database.FileFormat.V2007,
    };

    /**
     * Defines known valid test database files, and their jet format version.
     */
    public static final class TestDB {

        private final File dbFile;
        private final Database.FileFormat expectedFileFormat;

        private TestDB(final File databaseFile, final Database.FileFormat expectedDBFileFormat) {

            dbFile = databaseFile;
            expectedFileFormat = expectedDBFileFormat;
        }

        public final File getFile() { return dbFile; }
        public final Database.FileFormat  getExpectedFileFormat() { return expectedFileFormat; }
        public final JetFormat getExpectedFormat() { return expectedFileFormat.getJetFormat(); }

        public final String toString() {
            return "dbFile: " + dbFile.getAbsolutePath()
                    + "; expectedFileFormat: " + expectedFileFormat;
        }

        public static TestDB[] getSupportedForBasename(final Basename basename) {

            final TestDB[] supportedTestDBs = new TestDB[SUPPORTED_FILEFORMATS.length];
            int i = 0;
            for (final Database.FileFormat fileFormat: SUPPORTED_FILEFORMATS) {
                supportedTestDBs[i++] = new TestDB(
                        getFileForBasename(basename, fileFormat),
                        fileFormat);
            }
            return supportedTestDBs;
        }

        private static File getFileForBasename(Basename basename, Database.FileFormat fileFormat) {

            return new File(DIR_TEST_DATA, 
                            fileFormat.name() + "/" + basename.basename + fileFormat.name() + 
                            fileFormat.getFileExtension());
        }
    }

    static final TestDB UNSUPPORTED_TEST_V1997 = new TestDB(
            TestDB.getFileForBasename(Basename.TEST, Database.FileFormat.V1997), Database.FileFormat.V1997);

    static final TestDB[] SUPPORTED_DBS_TEST= TestDB.getSupportedForBasename(Basename.TEST);


    public void testGetFormat() throws Exception {
        try {
            JetFormat.getFormat(null);
            fail("npe");
        } catch (NullPointerException e) {
            assertNull(e.getMessage());
        }

        checkJetFormat(UNSUPPORTED_TEST_V1997);

        for (final TestDB testDB : SUPPORTED_DBS_TEST) {
          checkJetFormat(testDB);
        }
    }

    private static void checkJetFormat(final TestDB testDB)
            throws IOException {

        final FileChannel channel = Database.openChannel(testDB.dbFile, false);
        try {

            final JetFormat fmtActual = JetFormat.getFormat(channel);
            assertEquals("Unexpected JetFormat for dbFile: " + testDB.dbFile.getAbsolutePath(),
                    testDB.expectedFileFormat.getJetFormat(), fmtActual);

        } finally {
            channel.close();
        }
    }
}
