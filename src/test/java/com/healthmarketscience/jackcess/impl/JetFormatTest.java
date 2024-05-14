package com.healthmarketscience.jackcess.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.JackcessException;
import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.PropertyMap;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.TestUtil.*;


/**
 * @author Dan Rollo
 *         Date: Mar 5, 2010
 *         Time: 12:44:21 PM
 */
public class JetFormatTest extends TestCase {

  public static final File DIR_TEST_DATA = new File("src/test/data");

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
    LINKED("linkerTest"),
    LINKED_ODBC("odbcLinkerTest"),
    BLOB("testOle"),
    CALC_FIELD("calcFieldTest"),
    BINARY_INDEX("binIdxTest"),
    OLD_DATES("oldDates"),
    EXT_DATE("extDateTest"),
    EMOTICONS("testEmoticons");

    private final String _basename;

    Basename(String fileBasename) {
      _basename = fileBasename;
    }

    @Override
    public String toString() { return _basename; }
  }

  /** charset for access 97 dbs */
  public static final Charset A97_CHARSET = Charset.forName("windows-1252");

  /** Defines currently supported db file formats.  (can be modified at
      runtime via the system property
      "com.healthmarketscience.jackcess.testFormats") */
  public final static FileFormat[] SUPPORTED_FILEFORMATS;
  public final static FileFormat[] SUPPORTED_FILEFORMATS_FOR_READ;

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
    private final Charset _charset;

    private TestDB(File databaseFile,
                   FileFormat expectedDBFileFormat,
                   Charset charset) {

      dbFile = databaseFile;
      expectedFileFormat = expectedDBFileFormat;
      _charset = charset;
    }

    public final File getFile() { return dbFile; }

    public final FileFormat  getExpectedFileFormat() {
      return expectedFileFormat;
    }

    public final JetFormat getExpectedFormat() {
      return DatabaseImpl.getFileFormatDetails(expectedFileFormat).getFormat();
    }

    public final Charset getExpectedCharset() {
      return _charset;
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

        Charset charset = null;
        if(fileFormat == FileFormat.V1997) {
          charset = A97_CHARSET;
        }

        supportedTestDBs.add(new TestDB(testFile, fileFormat, charset));
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

  public static final List<TestDB> SUPPORTED_DBS_TEST =
    TestDB.getSupportedForBasename(Basename.TEST);
  public static final List<TestDB> SUPPORTED_DBS_TEST_FOR_READ =
    TestDB.getSupportedForBasename(Basename.TEST, true);


  public void testGetFormat() throws Exception {
    try {
      JetFormat.getFormat(null);
      fail("npe");
    } catch (NullPointerException e) {
      // success
    }

    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {

      final FileChannel channel = DatabaseImpl.openChannel(
          testDB.dbFile.toPath(), false, false);
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
      Exception failure = null;
      try {
        db = openCopy(testDB);

        if(testDB.getExpectedFormat().READ_ONLY) {
          PropertyMap props = db.getUserDefinedProperties();
          props.put("foo", "bar");
          props.save();
        }

      } catch(Exception e) {
        failure = e;
      } finally {
        if(db != null) {
          db.close();
        }
      }

      if(!testDB.getExpectedFormat().READ_ONLY) {
        assertNull(failure);
      } else {
        assertTrue(failure instanceof NonWritableChannelException);
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

    Database db = null;
    try {
      db = open(Database.FileFormat.GENERIC_JET4,
                new File(DIR_TEST_DATA, "adox_jet4.mdb"));
      assertEquals(Database.FileFormat.GENERIC_JET4, db.getFileFormat());
    } finally {
      if(db != null) {
        db.close();
      }
    }
  }

  public void testSqlTypes() throws Exception {

    JetFormat v2000 = JetFormat.VERSION_4;
    for(DataType dt : DataType.values()) {
      if(v2000.isSupportedDataType(dt)) {
        Integer sqlType = null;
        try {
          sqlType = dt.getSQLType();
        } catch(JackcessException ignored) {}

        if(sqlType != null) {
          assertEquals(dt, DataType.fromSQLType(sqlType));
        }
      }
    }

    assertEquals(DataType.LONG, DataType.fromSQLType(java.sql.Types.BIGINT));
    assertEquals(DataType.BIG_INT, DataType.fromSQLType(
                     java.sql.Types.BIGINT, 0, Database.FileFormat.V2016));
    assertEquals(java.sql.Types.BIGINT, DataType.BIG_INT.getSQLType());
    assertEquals(DataType.MEMO, DataType.fromSQLType(
                     java.sql.Types.VARCHAR, 1000));
  }

  public static void transferDbFrom(FileChannel channel, InputStream in)
    throws IOException
  {
    DatabaseImpl.transferDbFrom(channel, in);
  }
}
