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
public final class JetFormatTest  extends TestCase {

    static final File DB_1997 = new File("test/data/mdb97/test97.mdb");
    static final File DB_2000 = new File("test/data/test.mdb");
    static final File DB_2003 = new File("test/data/mdb2003/test2003.mdb");
    static final File DB_2007 = new File("test/data/accdb/test.accdb");

    public void testGetFormat() throws Exception {
        try {
            JetFormat.getFormat(null);
            fail("npe");
        } catch (NullPointerException e) {
            assertNull(e.getMessage());
        }

        checkJetFormat(DB_1997, JetFormat.VERSION_3);
        checkJetFormat(DB_2000, JetFormat.VERSION_4);
        checkJetFormat(DB_2003, JetFormat.VERSION_4);
        checkJetFormat(DB_2007, JetFormat.VERSION_5);
    }

    private static void checkJetFormat(final File dbFile, final JetFormat fmtExpected)
            throws IOException {

        final FileChannel channel = Database.openChannel(dbFile, false);
        try {

            final JetFormat fmtActual = JetFormat.getFormat(channel);
            assertEquals("Unexpected JetFormat for dbFile: " + dbFile.getAbsolutePath(),
                    fmtExpected, fmtActual);

        } finally {
            channel.close();
        }
    }
}
