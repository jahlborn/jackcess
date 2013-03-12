package com.healthmarketscience.jackcess;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import static com.healthmarketscience.jackcess.Database.*;
import static com.healthmarketscience.jackcess.DatabaseTest.*;

/**
 * @author Dan Rollo
 *         Date: Mar 5, 2010
 *         Time: 12:44:21 PM
 */
public class JetFormatTest extends TestCase {

  static final File DIR_TEST_DATA = new File("test/data");

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
    COMPLEX("complexDataTest"),
    UNSUPPORTED("unsupportedFieldsTest"),
    LINKED("linkerTest");

    private final String _basename;

    Basename(String fileBasename) {
      _basename = fileBasename;
    }

    @Override
    public String toString() { return _basename; }
  }

  /** Defines currently supported db file formats.  (can be modified at
      runtime via the system property
      "com.healthmarketscience.jackcess.testFormats") */
  final static FileFormat[] SUPPORTED_FILEFORMATS;
  final static FileFormat[] SUPPORTED_FILEFORMATS_FOR_READ;

  static {
    String testFormatStr = System.getProperty("com.healthmarketscience.jackcess.testFormats");
    Set<FileFormat> testFormats = EnumSet.allOf(FileFormat.class);
    if((testFormatStr != null) && (testFormatStr.length() > 0)) {
      testFormats.clear();
      for(String tmp : testFormatStr.split(",")) {
        testFormats.add(FileFormat.valueOf(tmp.toUpperCase()));
      }
    }

    List<FileFormat> supported = new ArrayList<FileFormat>();
    List<FileFormat> supportedForRead = new ArrayList<FileFormat>();
    for(FileFormat ff : FileFormat.values()) {
      if(!testFormats.contains(ff)) {
        continue;
      }
      supportedForRead.add(ff);
      if(DatabaseImpl.getFileFormatDetails(ff).getFormat().READ_ONLY ||
         (ff == FileFormat.MSISAM)) {
        continue;
      }
      supported.add(ff);
    }

    SUPPORTED_FILEFORMATS = supported.toArray(new FileFormat[0]);
    SUPPORTED_FILEFORMATS_FOR_READ = 
      supportedForRead.toArray(new FileFormat[0]);
  }

  /**
   * Defines known valid test database files, and their jet format version.
   */
  public static final class TestDB {

    private final File dbFile;
    private final FileFormat expectedFileFormat;

    private TestDB(File databaseFile, 
                   FileFormat expectedDBFileFormat) {

      dbFile = databaseFile;
      expectedFileFormat = expectedDBFileFormat;
    }

    public final File getFile() { return dbFile; }

    public final FileFormat  getExpectedFileFormat() { 
      return expectedFileFormat; 
    }

    public final JetFormat getExpectedFormat() { 
      return DatabaseImpl.getFileFormatDetails(expectedFileFormat).getFormat(); 
    }

    @Override
    public final String toString() {
      return "dbFile: " + dbFile.getAbsolutePath()
        + "; expectedFileFormat: " + expectedFileFormat;
    }

    public static List<TestDB> getSupportedForBasename(Basename basename) {
      return getSupportedForBasename(basename, false);
    }

    public static List<TestDB> getSupportedForBasename(Basename basename,
                                                       boolean readOnly) {

      List<TestDB> supportedTestDBs = new ArrayList<TestDB>();
      for (FileFormat fileFormat : 
             (readOnly ? SUPPORTED_FILEFORMATS_FOR_READ :
              SUPPORTED_FILEFORMATS)) {
        File testFile = getFileForBasename(basename, fileFormat);
        if(!testFile.exists()) {
          continue;
        }
        
        // verify that the db is the file format expected
        try {
          Database db = new DatabaseBuilder(testFile).setReadOnly(true).open();
          FileFormat dbFileFormat = db.getFileFormat();
          db.close();
          if(dbFileFormat != fileFormat) {
            throw new IllegalStateException("Expected " + fileFormat +
                                            " was " + dbFileFormat);
          }
        } catch(Exception e) {
          throw new RuntimeException(e);
        }

        supportedTestDBs.add(new TestDB(testFile, fileFormat));
      }
      return supportedTestDBs;
    }

    private static File getFileForBasename(
        Basename basename, FileFormat fileFormat) {

      return new File(DIR_TEST_DATA, 
                      fileFormat.name() + File.separator +
                      basename + fileFormat.name() + 
                      fileFormat.getFileExtension());
    }
  }

  static final List<TestDB> SUPPORTED_DBS_TEST = 
    TestDB.getSupportedForBasename(Basename.TEST);
  static final List<TestDB> SUPPORTED_DBS_TEST_FOR_READ = 
    TestDB.getSupportedForBasename(Basename.TEST, true);


  public void testGetFormat() throws Exception {
    try {
      JetFormat.getFormat(null);
      fail("npe");
    } catch (NullPointerException e) {
      // success
    }

    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {

      final FileChannel channel = DatabaseImpl.openChannel(testDB.dbFile, false);
      try {

        JetFormat fmtActual = JetFormat.getFormat(channel);
        assertEquals("Unexpected JetFormat for dbFile: " + 
                     testDB.dbFile.getAbsolutePath(),
                     testDB.getExpectedFormat(), fmtActual);

      } finally {
        channel.close();
      }

    }
  }

  public void testReadOnlyFormat() throws Exception {

    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {

      Database db = null;
      IOException failure = null;
      try {
        db = openCopy(testDB);
      } catch(IOException e) {
        failure = e;
      } finally {
        if(db != null) {
          db.close();
        }
      }

      if(!testDB.getExpectedFormat().READ_ONLY) {
        assertNull(failure);
      } else {
        assertTrue(failure.getMessage().contains("does not support writing"));
      }

    }
  }

  public void testFileFormat() throws Exception {

    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {

      Database db = null;
      try {
        db = open(testDB);
        assertEquals(testDB.getExpectedFileFormat(), db.getFileFormat());
      } finally {
        if(db != null) {
          db.close();
        }
      }
    }
  }

}
