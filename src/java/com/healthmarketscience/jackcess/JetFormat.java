/*
Copyright (c) 2005 Health Market Science, Inc.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Health Market Science at info@healthmarketscience.com
or at the following address:

Health Market Science
2700 Horizon Drive
Suite 200
King of Prussia, PA 19406
*/

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates constants describing a specific version of the Access Jet format
 * @author Tim McCune
 */
public abstract class JetFormat {
  
  /** Maximum size of a record minus OLE objects and Memo fields */
  public static final int MAX_RECORD_SIZE = 1900;  //2kb minus some overhead

  /** the "unit" size for text fields */
  public static final short TEXT_FIELD_UNIT_SIZE = 2;
  /** Maximum size of a text field */
  public static final short TEXT_FIELD_MAX_LENGTH = 255 * TEXT_FIELD_UNIT_SIZE;

  public enum CodecType {
    NONE, JET, MSISAM, OFFICE;
  }
  
  /** Offset in the file that holds the byte describing the Jet format
      version */
  private static final int OFFSET_VERSION = 20;
  /** Version code for Jet version 3 */
  private static final byte CODE_VERSION_3 = 0x0;
  /** Version code for Jet version 4 */
  private static final byte CODE_VERSION_4 = 0x1;
  /** Version code for Jet version 12 */
  private static final byte CODE_VERSION_12 = 0x2;
  /** Version code for Jet version 14 */
  private static final byte CODE_VERSION_14 = 0x3;

  /** location of the engine name in the header */
  static final int OFFSET_ENGINE_NAME = 0x4;
  /** length of the engine name in the header */
  static final int LENGTH_ENGINE_NAME = 0xF;
  /** amount of initial data to be read to determine database type */
  private static final int HEADER_LENGTH = 21;
  
  private final static byte[] MSISAM_ENGINE = new byte[] {
    'M', 'S', 'I', 'S', 'A', 'M', ' ', 'D', 'a', 't', 'a', 'b', 'a', 's', 'e'
  };

  /** mask used to obfuscate the db header */
  private static final byte[] BASE_HEADER_MASK = new byte[]{
    (byte)0xB5, (byte)0x6F, (byte)0x03, (byte)0x62, (byte)0x61, (byte)0x08,
    (byte)0xC2, (byte)0x55, (byte)0xEB, (byte)0xA9, (byte)0x67, (byte)0x72,
    (byte)0x43, (byte)0x3F, (byte)0x00, (byte)0x9C, (byte)0x7A, (byte)0x9F,
    (byte)0x90, (byte)0xFF, (byte)0x80, (byte)0x9A, (byte)0x31, (byte)0xC5,
    (byte)0x79, (byte)0xBA, (byte)0xED, (byte)0x30, (byte)0xBC, (byte)0xDF, 
    (byte)0xCC, (byte)0x9D, (byte)0x63, (byte)0xD9, (byte)0xE4, (byte)0xC3,
    (byte)0x7B, (byte)0x42, (byte)0xFB, (byte)0x8A, (byte)0xBC, (byte)0x4E,
    (byte)0x86, (byte)0xFB, (byte)0xEC, (byte)0x37, (byte)0x5D, (byte)0x44,
    (byte)0x9C, (byte)0xFA, (byte)0xC6, (byte)0x5E, (byte)0x28, (byte)0xE6, 
    (byte)0x13, (byte)0xB6, (byte)0x8A, (byte)0x60, (byte)0x54, (byte)0x94,
    (byte)0x7B, (byte)0x36, (byte)0xF5, (byte)0x72, (byte)0xDF, (byte)0xB1,
    (byte)0x77, (byte)0xF4, (byte)0x13, (byte)0x43, (byte)0xCF, (byte)0xAF,
    (byte)0xB1, (byte)0x33, (byte)0x34, (byte)0x61, (byte)0x79, (byte)0x5B,
    (byte)0x92, (byte)0xB5, (byte)0x7C, (byte)0x2A, (byte)0x05, (byte)0xF1,
    (byte)0x7C, (byte)0x99, (byte)0x01, (byte)0x1B, (byte)0x98, (byte)0xFD,
    (byte)0x12, (byte)0x4F, (byte)0x4A, (byte)0x94, (byte)0x6C, (byte)0x3E,
    (byte)0x60, (byte)0x26, (byte)0x5F, (byte)0x95, (byte)0xF8, (byte)0xD0,
    (byte)0x89, (byte)0x24, (byte)0x85, (byte)0x67, (byte)0xC6, (byte)0x1F,
    (byte)0x27, (byte)0x44, (byte)0xD2, (byte)0xEE, (byte)0xCF, (byte)0x65,
    (byte)0xED, (byte)0xFF, (byte)0x07, (byte)0xC7, (byte)0x46, (byte)0xA1,
    (byte)0x78, (byte)0x16, (byte)0x0C, (byte)0xED, (byte)0xE9, (byte)0x2D,
    (byte)0x62, (byte)0xD4};    

  /** value of the "AccessVersion" property for access 2000 dbs:
      {@code "08.50"} */
  private static final String ACCESS_VERSION_2000 = "08.50";
  /** value of the "AccessVersion" property for access 2002/2003 dbs
      {@code "09.50"}  */
  private static final String ACCESS_VERSION_2003 = "09.50";

  /** known intro bytes for property maps */
  static final byte[][] PROPERTY_MAP_TYPES = {
    new byte[]{'M', 'R', '2', '\0'}, // access 2000+
    new byte[]{'K', 'K', 'D', '\0'}};  // access 97

  // use nested inner class to avoid problematic static init loops
  private static final class PossibleFileFormats {
    private static final Map<String,Database.FileFormat> POSSIBLE_VERSION_3 = 
      Collections.singletonMap((String)null, Database.FileFormat.V1997);

    private static final Map<String,Database.FileFormat> POSSIBLE_VERSION_4 = 
      new HashMap<String,Database.FileFormat>();

    private static final Map<String,Database.FileFormat> POSSIBLE_VERSION_12 = 
      Collections.singletonMap((String)null, Database.FileFormat.V2007);

    private static final Map<String,Database.FileFormat> POSSIBLE_VERSION_14 = 
      Collections.singletonMap((String)null, Database.FileFormat.V2010);

    private static final Map<String,Database.FileFormat> POSSIBLE_VERSION_MSISAM = 
      Collections.singletonMap((String)null, Database.FileFormat.MSISAM);

    static {
      POSSIBLE_VERSION_4.put(ACCESS_VERSION_2000, Database.FileFormat.V2000);
      POSSIBLE_VERSION_4.put(ACCESS_VERSION_2003, Database.FileFormat.V2003);
    }
  }

  /** the JetFormat constants for the Jet database version "3" */
  public static final JetFormat VERSION_3 = new Jet3Format();
  /** the JetFormat constants for the Jet database version "4" */
  public static final JetFormat VERSION_4 = new Jet4Format();
  /** the JetFormat constants for the MSISAM database */
  public static final JetFormat VERSION_MSISAM = new MSISAMFormat();
  /** the JetFormat constants for the Jet database version "12" */
  public static final JetFormat VERSION_12 = new Jet12Format();
  /** the JetFormat constants for the Jet database version "14" */
  public static final JetFormat VERSION_14 = new Jet14Format();

  //These constants are populated by this class's constructor.  They can't be
  //populated by the subclass's constructor because they are final, and Java
  //doesn't allow this; hence all the abstract defineXXX() methods.

  /** the name of this format */
  private final String _name;
  
  /** the read/write mode of this format */
  public final boolean READ_ONLY;
  
  /** whether or not we can use indexes in this format */
  public final boolean INDEXES_SUPPORTED;

  /** type of page encoding supported */
  public final CodecType CODEC_TYPE;
  
  /** Database page size in bytes */
  public final int PAGE_SIZE;
  public final long MAX_DATABASE_SIZE;
  
  public final int MAX_ROW_SIZE;
  public final int DATA_PAGE_INITIAL_FREE_SPACE;

  public final int OFFSET_MASKED_HEADER;
  public final byte[] HEADER_MASK;
  public final int OFFSET_HEADER_DATE;
  public final int OFFSET_PASSWORD;
  public final int SIZE_PASSWORD;
  public final int OFFSET_SORT_ORDER;
  public final int SIZE_SORT_ORDER;
  public final int OFFSET_CODE_PAGE;
  public final int OFFSET_ENCODING_KEY;
  public final int OFFSET_NEXT_TABLE_DEF_PAGE;
  public final int OFFSET_NUM_ROWS;
  public final int OFFSET_NEXT_AUTO_NUMBER;
  public final int OFFSET_NEXT_COMPLEX_AUTO_NUMBER;
  public final int OFFSET_TABLE_TYPE;
  public final int OFFSET_MAX_COLS;
  public final int OFFSET_NUM_VAR_COLS;
  public final int OFFSET_NUM_COLS;
  public final int OFFSET_NUM_INDEX_SLOTS;
  public final int OFFSET_NUM_INDEXES;
  public final int OFFSET_OWNED_PAGES;
  public final int OFFSET_FREE_SPACE_PAGES;
  public final int OFFSET_INDEX_DEF_BLOCK;
  
  public final int SIZE_INDEX_COLUMN_BLOCK;
  public final int SIZE_INDEX_INFO_BLOCK;
  
  public final int OFFSET_COLUMN_TYPE;
  public final int OFFSET_COLUMN_NUMBER;
  public final int OFFSET_COLUMN_PRECISION;
  public final int OFFSET_COLUMN_SCALE;
  public final int OFFSET_COLUMN_SORT_ORDER;
  public final int OFFSET_COLUMN_CODE_PAGE;
  public final int OFFSET_COLUMN_COMPLEX_ID;
  public final int OFFSET_COLUMN_FLAGS;
  public final int OFFSET_COLUMN_COMPRESSED_UNICODE;
  public final int OFFSET_COLUMN_LENGTH;
  public final int OFFSET_COLUMN_VARIABLE_TABLE_INDEX;
  public final int OFFSET_COLUMN_FIXED_DATA_OFFSET;
  public final int OFFSET_COLUMN_FIXED_DATA_ROW_OFFSET;
  
  public final int OFFSET_TABLE_DEF_LOCATION;
  
  public final int OFFSET_ROW_START;
  public final int OFFSET_USAGE_MAP_START;
  
  public final int OFFSET_USAGE_MAP_PAGE_DATA;
  
  public final int OFFSET_REFERENCE_MAP_PAGE_NUMBERS;
  
  public final int OFFSET_FREE_SPACE;
  public final int OFFSET_NUM_ROWS_ON_DATA_PAGE;
  public final int MAX_NUM_ROWS_ON_DATA_PAGE;
  
  public final int OFFSET_INDEX_COMPRESSED_BYTE_COUNT;
  public final int OFFSET_INDEX_ENTRY_MASK;
  public final int OFFSET_PREV_INDEX_PAGE;
  public final int OFFSET_NEXT_INDEX_PAGE;
  public final int OFFSET_CHILD_TAIL_INDEX_PAGE;
  
  public final int SIZE_INDEX_DEFINITION;
  public final int SIZE_COLUMN_HEADER;
  public final int SIZE_ROW_LOCATION;
  public final int SIZE_LONG_VALUE_DEF;
  public final int MAX_INLINE_LONG_VALUE_SIZE;
  public final int MAX_LONG_VALUE_ROW_SIZE;
  public final int SIZE_TDEF_HEADER;
  public final int SIZE_TDEF_TRAILER;
  public final int SIZE_COLUMN_DEF_BLOCK;
  public final int SIZE_INDEX_ENTRY_MASK;
  public final int SKIP_BEFORE_INDEX_FLAGS;
  public final int SKIP_AFTER_INDEX_FLAGS;
  public final int SKIP_BEFORE_INDEX_SLOT;
  public final int SKIP_AFTER_INDEX_SLOT;
  public final int SKIP_BEFORE_INDEX;
  public final int SIZE_NAME_LENGTH;
  public final int SIZE_ROW_COLUMN_COUNT;
  public final int SIZE_ROW_VAR_COL_OFFSET;
  
  public final int USAGE_MAP_TABLE_BYTE_LENGTH;

  public final int MAX_COLUMNS_PER_TABLE;
  public final int MAX_TABLE_NAME_LENGTH;
  public final int MAX_COLUMN_NAME_LENGTH;
  public final int MAX_INDEX_NAME_LENGTH;

  public final boolean LEGACY_NUMERIC_INDEXES;
  
  public final Charset CHARSET;
  public final Column.SortOrder DEFAULT_SORT_ORDER;
  
  /**
   * @param channel the database file.
   * @return The Jet Format represented in the passed-in file
   * @throws IOException if the database file format is unsupported.
   */
  public static JetFormat getFormat(FileChannel channel) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH);
    int bytesRead = channel.read(buffer, 0L);
    if(bytesRead < HEADER_LENGTH) {
      throw new IOException("Empty database file");
    }
    buffer.flip();
    byte version = buffer.get(OFFSET_VERSION);
    if (version == CODE_VERSION_3) {
      return VERSION_3;
    } else if (version == CODE_VERSION_4) {
      if(ByteUtil.matchesRange(buffer, OFFSET_ENGINE_NAME, MSISAM_ENGINE)) {
        return VERSION_MSISAM;
      }
      return VERSION_4;
    } else if (version == CODE_VERSION_12) {
      return VERSION_12;
    } else if (version == CODE_VERSION_14) {
      return VERSION_14;
    }
    throw new IOException("Unsupported " +
                          ((version < CODE_VERSION_3) ? "older" : "newer") +
                          " version: " + version);
  }
  
  private JetFormat(String name) {
    _name = name;
    
    READ_ONLY = defineReadOnly();
    INDEXES_SUPPORTED = defineIndexesSupported();
    CODEC_TYPE = defineCodecType();
    
    PAGE_SIZE = definePageSize();
    MAX_DATABASE_SIZE = defineMaxDatabaseSize();
    
    MAX_ROW_SIZE = defineMaxRowSize();
    DATA_PAGE_INITIAL_FREE_SPACE = defineDataPageInitialFreeSpace();
    
    OFFSET_MASKED_HEADER = defineOffsetMaskedHeader();
    HEADER_MASK = defineHeaderMask();
    OFFSET_HEADER_DATE = defineOffsetHeaderDate();
    OFFSET_PASSWORD = defineOffsetPassword();
    SIZE_PASSWORD = defineSizePassword();
    OFFSET_SORT_ORDER = defineOffsetSortOrder();
    SIZE_SORT_ORDER = defineSizeSortOrder();
    OFFSET_CODE_PAGE = defineOffsetCodePage();
    OFFSET_ENCODING_KEY = defineOffsetEncodingKey();
    OFFSET_NEXT_TABLE_DEF_PAGE = defineOffsetNextTableDefPage();
    OFFSET_NUM_ROWS = defineOffsetNumRows();
    OFFSET_NEXT_AUTO_NUMBER = defineOffsetNextAutoNumber();
    OFFSET_NEXT_COMPLEX_AUTO_NUMBER = defineOffsetNextComplexAutoNumber();
    OFFSET_TABLE_TYPE = defineOffsetTableType();
    OFFSET_MAX_COLS = defineOffsetMaxCols();
    OFFSET_NUM_VAR_COLS = defineOffsetNumVarCols();
    OFFSET_NUM_COLS = defineOffsetNumCols();
    OFFSET_NUM_INDEX_SLOTS = defineOffsetNumIndexSlots();
    OFFSET_NUM_INDEXES = defineOffsetNumIndexes();
    OFFSET_OWNED_PAGES = defineOffsetOwnedPages();
    OFFSET_FREE_SPACE_PAGES = defineOffsetFreeSpacePages();
    OFFSET_INDEX_DEF_BLOCK = defineOffsetIndexDefBlock();
    
    SIZE_INDEX_COLUMN_BLOCK = defineSizeIndexColumnBlock();
    SIZE_INDEX_INFO_BLOCK = defineSizeIndexInfoBlock();
    
    OFFSET_COLUMN_TYPE = defineOffsetColumnType();
    OFFSET_COLUMN_NUMBER = defineOffsetColumnNumber();
    OFFSET_COLUMN_PRECISION = defineOffsetColumnPrecision();
    OFFSET_COLUMN_SCALE = defineOffsetColumnScale();
    OFFSET_COLUMN_SORT_ORDER = defineOffsetColumnSortOrder();
    OFFSET_COLUMN_CODE_PAGE = defineOffsetColumnCodePage();
    OFFSET_COLUMN_COMPLEX_ID = defineOffsetColumnComplexId();
    OFFSET_COLUMN_FLAGS = defineOffsetColumnFlags();
    OFFSET_COLUMN_COMPRESSED_UNICODE = defineOffsetColumnCompressedUnicode();
    OFFSET_COLUMN_LENGTH = defineOffsetColumnLength();
    OFFSET_COLUMN_VARIABLE_TABLE_INDEX = defineOffsetColumnVariableTableIndex();
    OFFSET_COLUMN_FIXED_DATA_OFFSET = defineOffsetColumnFixedDataOffset();
    OFFSET_COLUMN_FIXED_DATA_ROW_OFFSET = defineOffsetColumnFixedDataRowOffset();
    
    OFFSET_TABLE_DEF_LOCATION = defineOffsetTableDefLocation();
    
    OFFSET_ROW_START = defineOffsetRowStart();
    OFFSET_USAGE_MAP_START = defineOffsetUsageMapStart();
    
    OFFSET_USAGE_MAP_PAGE_DATA = defineOffsetUsageMapPageData();
    
    OFFSET_REFERENCE_MAP_PAGE_NUMBERS = defineOffsetReferenceMapPageNumbers();
    
    OFFSET_FREE_SPACE = defineOffsetFreeSpace();
    OFFSET_NUM_ROWS_ON_DATA_PAGE = defineOffsetNumRowsOnDataPage();
    MAX_NUM_ROWS_ON_DATA_PAGE = defineMaxNumRowsOnDataPage();
    
    OFFSET_INDEX_COMPRESSED_BYTE_COUNT = defineOffsetIndexCompressedByteCount();
    OFFSET_INDEX_ENTRY_MASK = defineOffsetIndexEntryMask();
    OFFSET_PREV_INDEX_PAGE = defineOffsetPrevIndexPage();
    OFFSET_NEXT_INDEX_PAGE = defineOffsetNextIndexPage();
    OFFSET_CHILD_TAIL_INDEX_PAGE = defineOffsetChildTailIndexPage();
    
    SIZE_INDEX_DEFINITION = defineSizeIndexDefinition();
    SIZE_COLUMN_HEADER = defineSizeColumnHeader();
    SIZE_ROW_LOCATION = defineSizeRowLocation();
    SIZE_LONG_VALUE_DEF = defineSizeLongValueDef();
    MAX_INLINE_LONG_VALUE_SIZE = defineMaxInlineLongValueSize();
    MAX_LONG_VALUE_ROW_SIZE = defineMaxLongValueRowSize();
    SIZE_TDEF_HEADER = defineSizeTdefHeader();
    SIZE_TDEF_TRAILER = defineSizeTdefTrailer();
    SIZE_COLUMN_DEF_BLOCK = defineSizeColumnDefBlock();
    SIZE_INDEX_ENTRY_MASK = defineSizeIndexEntryMask();
    SKIP_BEFORE_INDEX_FLAGS = defineSkipBeforeIndexFlags();
    SKIP_AFTER_INDEX_FLAGS = defineSkipAfterIndexFlags();
    SKIP_BEFORE_INDEX_SLOT = defineSkipBeforeIndexSlot();
    SKIP_AFTER_INDEX_SLOT = defineSkipAfterIndexSlot();
    SKIP_BEFORE_INDEX = defineSkipBeforeIndex();
    SIZE_NAME_LENGTH = defineSizeNameLength();
    SIZE_ROW_COLUMN_COUNT = defineSizeRowColumnCount();
    SIZE_ROW_VAR_COL_OFFSET = defineSizeRowVarColOffset();

    USAGE_MAP_TABLE_BYTE_LENGTH = defineUsageMapTableByteLength();

    MAX_COLUMNS_PER_TABLE = defineMaxColumnsPerTable();
    MAX_TABLE_NAME_LENGTH = defineMaxTableNameLength();
    MAX_COLUMN_NAME_LENGTH = defineMaxColumnNameLength();
    MAX_INDEX_NAME_LENGTH = defineMaxIndexNameLength();
    
    LEGACY_NUMERIC_INDEXES = defineLegacyNumericIndexes();
    
    CHARSET = defineCharset();
    DEFAULT_SORT_ORDER = defineDefaultSortOrder();
  }
  
  protected abstract boolean defineReadOnly();
  protected abstract boolean defineIndexesSupported();
  protected abstract CodecType defineCodecType();
  
  protected abstract int definePageSize();
  protected abstract long defineMaxDatabaseSize();
  
  protected abstract int defineMaxRowSize();
  protected abstract int defineDataPageInitialFreeSpace();
  
  protected abstract int defineOffsetMaskedHeader();
  protected abstract byte[] defineHeaderMask();
  protected abstract int defineOffsetHeaderDate();
  protected abstract int defineOffsetPassword();
  protected abstract int defineSizePassword();
  protected abstract int defineOffsetSortOrder();
  protected abstract int defineSizeSortOrder();
  protected abstract int defineOffsetCodePage();
  protected abstract int defineOffsetEncodingKey();
  protected abstract int defineOffsetNextTableDefPage();
  protected abstract int defineOffsetNumRows();
  protected abstract int defineOffsetNextAutoNumber();
  protected abstract int defineOffsetNextComplexAutoNumber();
  protected abstract int defineOffsetTableType();
  protected abstract int defineOffsetMaxCols();
  protected abstract int defineOffsetNumVarCols();
  protected abstract int defineOffsetNumCols();
  protected abstract int defineOffsetNumIndexSlots();
  protected abstract int defineOffsetNumIndexes();
  protected abstract int defineOffsetOwnedPages();
  protected abstract int defineOffsetFreeSpacePages();
  protected abstract int defineOffsetIndexDefBlock();
  
  protected abstract int defineSizeIndexColumnBlock();
  protected abstract int defineSizeIndexInfoBlock();
  
  protected abstract int defineOffsetColumnType();
  protected abstract int defineOffsetColumnNumber();
  protected abstract int defineOffsetColumnPrecision();
  protected abstract int defineOffsetColumnScale();
  protected abstract int defineOffsetColumnSortOrder();
  protected abstract int defineOffsetColumnCodePage();
  protected abstract int defineOffsetColumnComplexId();
  protected abstract int defineOffsetColumnFlags();
  protected abstract int defineOffsetColumnCompressedUnicode();
  protected abstract int defineOffsetColumnLength();
  protected abstract int defineOffsetColumnVariableTableIndex();
  protected abstract int defineOffsetColumnFixedDataOffset();
  protected abstract int defineOffsetColumnFixedDataRowOffset();
  
  protected abstract int defineOffsetTableDefLocation();
  
  protected abstract int defineOffsetRowStart();
  protected abstract int defineOffsetUsageMapStart();
  
  protected abstract int defineOffsetUsageMapPageData();
  
  protected abstract int defineOffsetReferenceMapPageNumbers();
  
  protected abstract int defineOffsetFreeSpace();
  protected abstract int defineOffsetNumRowsOnDataPage();
  protected abstract int defineMaxNumRowsOnDataPage();
  
  protected abstract int defineOffsetIndexCompressedByteCount();
  protected abstract int defineOffsetIndexEntryMask();
  protected abstract int defineOffsetPrevIndexPage();
  protected abstract int defineOffsetNextIndexPage();
  protected abstract int defineOffsetChildTailIndexPage();
  
  protected abstract int defineSizeIndexDefinition();
  protected abstract int defineSizeColumnHeader();
  protected abstract int defineSizeRowLocation();
  protected abstract int defineSizeLongValueDef();
  protected abstract int defineMaxInlineLongValueSize();
  protected abstract int defineMaxLongValueRowSize();
  protected abstract int defineSizeTdefHeader();
  protected abstract int defineSizeTdefTrailer();
  protected abstract int defineSizeColumnDefBlock();
  protected abstract int defineSizeIndexEntryMask();
  protected abstract int defineSkipBeforeIndexFlags();
  protected abstract int defineSkipAfterIndexFlags();
  protected abstract int defineSkipBeforeIndexSlot();
  protected abstract int defineSkipAfterIndexSlot();
  protected abstract int defineSkipBeforeIndex();
  protected abstract int defineSizeNameLength();
  protected abstract int defineSizeRowColumnCount();
  protected abstract int defineSizeRowVarColOffset();

  protected abstract int defineUsageMapTableByteLength();

  protected abstract int defineMaxColumnsPerTable();
  protected abstract int defineMaxTableNameLength();
  protected abstract int defineMaxColumnNameLength();
  protected abstract int defineMaxIndexNameLength();
  
  protected abstract Charset defineCharset();
  protected abstract Column.SortOrder defineDefaultSortOrder();

  protected abstract boolean defineLegacyNumericIndexes();

  protected abstract Map<String,Database.FileFormat> getPossibleFileFormats();

  protected abstract boolean isSupportedDataType(DataType type);

  @Override
  public String toString() {
    return _name;
  }
  
  private static class Jet3Format extends JetFormat {

    private Jet3Format() {
      super("VERSION_3");
    }

    @Override
    protected boolean defineReadOnly() { return true; }
	    
    @Override
    protected boolean defineIndexesSupported() { return false; }

    @Override
    protected CodecType defineCodecType() { 
      return CodecType.JET; 
    }
	    
    @Override
    protected int definePageSize() { return 2048; }
	    
    @Override
    protected long defineMaxDatabaseSize() {
      return (1L * 1024L * 1024L * 1024L);
    }
	    
    @Override
    protected int defineMaxRowSize() { return 2012; }
    @Override
    protected int defineDataPageInitialFreeSpace() { return PAGE_SIZE - 14; }
	    
    @Override
    protected int defineOffsetMaskedHeader() { return 24; }
    @Override
    protected byte[] defineHeaderMask() { 
      return ByteUtil.copyOf(BASE_HEADER_MASK, BASE_HEADER_MASK.length - 2); 
    }
    @Override
    protected int defineOffsetHeaderDate() { return -1; }
    @Override
    protected int defineOffsetPassword() { return 66; }
    @Override
    protected int defineSizePassword() { return 20; }
    @Override
    protected int defineOffsetSortOrder() { return 58; }
    @Override
    protected int defineSizeSortOrder() { return 2; }
    @Override
    protected int defineOffsetCodePage() { return 60; }
    @Override
    protected int defineOffsetEncodingKey() { return 62; }
    @Override
    protected int defineOffsetNextTableDefPage() { return 4; }
    @Override
    protected int defineOffsetNumRows() { return 12; }
    @Override
    protected int defineOffsetNextAutoNumber() { return 20; }
    @Override
    protected int defineOffsetNextComplexAutoNumber() { return -1; }
    @Override
    protected int defineOffsetTableType() { return 20; }
    @Override
    protected int defineOffsetMaxCols() { return 21; }
    @Override
    protected int defineOffsetNumVarCols() { return 23; }
    @Override
    protected int defineOffsetNumCols() { return 25; }
    @Override
    protected int defineOffsetNumIndexSlots() { return 27; }
    @Override
    protected int defineOffsetNumIndexes() { return 31; }
    @Override
    protected int defineOffsetOwnedPages() { return 35; }
    @Override
    protected int defineOffsetFreeSpacePages() { return 39; }
    @Override
    protected int defineOffsetIndexDefBlock() { return 43; }

    @Override
    protected int defineSizeIndexColumnBlock() { return 39; }
    @Override
    protected int defineSizeIndexInfoBlock() { return 20; }
	    
    @Override
    protected int defineOffsetColumnType() { return 0; }
    @Override
    protected int defineOffsetColumnNumber() { return 1; }
    @Override
    protected int defineOffsetColumnPrecision() { return 11; }
    @Override
    protected int defineOffsetColumnScale() { return 12; }
    @Override
    protected int defineOffsetColumnSortOrder() { return 9; }
    @Override
    protected int defineOffsetColumnCodePage() { return 11; }
    @Override
    protected int defineOffsetColumnComplexId() { return -1; }
    @Override
    protected int defineOffsetColumnFlags() { return 13; }
    @Override
    protected int defineOffsetColumnCompressedUnicode() { return 16; }
    @Override
    protected int defineOffsetColumnLength() { return 16; }
    @Override
    protected int defineOffsetColumnVariableTableIndex() { return 3; }
    @Override
    protected int defineOffsetColumnFixedDataOffset() { return 14; }
    @Override
    protected int defineOffsetColumnFixedDataRowOffset() { return 1; }
	  
    @Override
    protected int defineOffsetTableDefLocation() { return 4; }
	    
    @Override
    protected int defineOffsetRowStart() { return 10; }
    @Override
    protected int defineOffsetUsageMapStart() { return 5; }
	    
    @Override
    protected int defineOffsetUsageMapPageData() { return 4; }
	    
    @Override
    protected int defineOffsetReferenceMapPageNumbers() { return 1; }
	    
    @Override
    protected int defineOffsetFreeSpace() { return 2; }
    @Override
    protected int defineOffsetNumRowsOnDataPage() { return 8; }
    @Override
    protected int defineMaxNumRowsOnDataPage() { return 255; }
	    
    @Override
    protected int defineOffsetIndexCompressedByteCount() { return 20; }
    @Override
    protected int defineOffsetIndexEntryMask() { return 22; }
    @Override
    protected int defineOffsetPrevIndexPage() { return 8; }
    @Override
    protected int defineOffsetNextIndexPage() { return 12; }
    @Override
    protected int defineOffsetChildTailIndexPage() { return 16; }
	    
    @Override
    protected int defineSizeIndexDefinition() { return 8; }
    @Override
    protected int defineSizeColumnHeader() { return 18; }
    @Override
    protected int defineSizeRowLocation() { return 2; }
    @Override
    protected int defineSizeLongValueDef() { return 12; }
    @Override
    protected int defineMaxInlineLongValueSize() { return 64; }
    @Override
    protected int defineMaxLongValueRowSize() { return 2032; }
    @Override
    protected int defineSizeTdefHeader() { return 63; }
    @Override
    protected int defineSizeTdefTrailer() { return 2; }
    @Override
    protected int defineSizeColumnDefBlock() { return 25; }
    @Override
    protected int defineSizeIndexEntryMask() { return 226; }
    @Override
    protected int defineSkipBeforeIndexFlags() { return 0; }
    @Override
    protected int defineSkipAfterIndexFlags() { return 0; }
    @Override
    protected int defineSkipBeforeIndexSlot() { return 0; }
    @Override
    protected int defineSkipAfterIndexSlot() { return 0; }
    @Override
    protected int defineSkipBeforeIndex() { return 0; }
    @Override
    protected int defineSizeNameLength() { return 1; }
    @Override
    protected int defineSizeRowColumnCount() { return 1; }
    @Override
    protected int defineSizeRowVarColOffset() { return 1; }
	    
    @Override
    protected int defineUsageMapTableByteLength() { return 128; }
	      
    @Override
    protected int defineMaxColumnsPerTable() { return 255; }
	      
    @Override
    protected int defineMaxTableNameLength() { return 64; }
	      
    @Override
    protected int defineMaxColumnNameLength() { return 64; }
	      
    @Override
    protected int defineMaxIndexNameLength() { return 64; }
	      
    @Override
    protected boolean defineLegacyNumericIndexes() { return true; }

    @Override
    protected Charset defineCharset() { return Charset.defaultCharset(); }

    @Override
    protected Column.SortOrder defineDefaultSortOrder() {
      return Column.GENERAL_LEGACY_SORT_ORDER;
    }

    @Override
    protected Map<String,Database.FileFormat> getPossibleFileFormats()
    {
      return PossibleFileFormats.POSSIBLE_VERSION_3;
    }

    @Override
    protected boolean isSupportedDataType(DataType type) {
      return (type != DataType.COMPLEX_TYPE);
    }
  }
  
  private static class Jet4Format extends JetFormat {

    private Jet4Format() {
      this("VERSION_4");
    }

    private Jet4Format(String name) {
      super(name);
    }

    @Override
    protected boolean defineReadOnly() { return false; }
    
    @Override
    protected boolean defineIndexesSupported() { return true; }
	    
    @Override
    protected CodecType defineCodecType() { 
      return CodecType.JET; 
    }

    @Override
    protected int definePageSize() { return 4096; }
    
    @Override
    protected long defineMaxDatabaseSize() {
      return (2L * 1024L * 1024L * 1024L);
    }
    
    @Override
    protected int defineMaxRowSize() { return 4060; }
    @Override
    protected int defineDataPageInitialFreeSpace() { return PAGE_SIZE - 14; }
    
    @Override
    protected int defineOffsetMaskedHeader() { return 24; }
    @Override
    protected byte[] defineHeaderMask() { return BASE_HEADER_MASK; }
    @Override
    protected int defineOffsetHeaderDate() { return 114; }
    @Override
    protected int defineOffsetPassword() { return 66; }
    @Override
    protected int defineSizePassword() { return 40; }
    @Override
    protected int defineOffsetSortOrder() { return 110; }
    @Override
    protected int defineSizeSortOrder() { return 4; }
    @Override
    protected int defineOffsetCodePage() { return 60; }
    @Override
    protected int defineOffsetEncodingKey() { return 62; }
    @Override
    protected int defineOffsetNextTableDefPage() { return 4; }
    @Override
    protected int defineOffsetNumRows() { return 16; }
    @Override
    protected int defineOffsetNextAutoNumber() { return 20; }
    @Override
    protected int defineOffsetNextComplexAutoNumber() { return -1; }
    @Override
    protected int defineOffsetTableType() { return 40; }
    @Override
    protected int defineOffsetMaxCols() { return 41; }
    @Override
    protected int defineOffsetNumVarCols() { return 43; }
    @Override
    protected int defineOffsetNumCols() { return 45; }
    @Override
    protected int defineOffsetNumIndexSlots() { return 47; }
    @Override
    protected int defineOffsetNumIndexes() { return 51; }
    @Override
    protected int defineOffsetOwnedPages() { return 55; }
    @Override
    protected int defineOffsetFreeSpacePages() { return 59; }
    @Override
    protected int defineOffsetIndexDefBlock() { return 63; }

    @Override
    protected int defineSizeIndexColumnBlock() { return 52; }
    @Override
    protected int defineSizeIndexInfoBlock() { return 28; }
    
    @Override
    protected int defineOffsetColumnType() { return 0; }
    @Override
    protected int defineOffsetColumnNumber() { return 5; }
    @Override
    protected int defineOffsetColumnPrecision() { return 11; }
    @Override
    protected int defineOffsetColumnScale() { return 12; }
    @Override
    protected int defineOffsetColumnSortOrder() { return 11; }
    @Override
    protected int defineOffsetColumnCodePage() { return -1; }
    @Override
    protected int defineOffsetColumnComplexId() { return -1; }
    @Override
    protected int defineOffsetColumnFlags() { return 15; }
    @Override
    protected int defineOffsetColumnCompressedUnicode() { return 16; }
    @Override
    protected int defineOffsetColumnLength() { return 23; }
    @Override
    protected int defineOffsetColumnVariableTableIndex() { return 7; }
    @Override
    protected int defineOffsetColumnFixedDataOffset() { return 21; }
    @Override
    protected int defineOffsetColumnFixedDataRowOffset() { return 2; }
  
    @Override
    protected int defineOffsetTableDefLocation() { return 4; }
    
    @Override
    protected int defineOffsetRowStart() { return 14; }
    @Override
    protected int defineOffsetUsageMapStart() { return 5; }
    
    @Override
    protected int defineOffsetUsageMapPageData() { return 4; }
    
    @Override
    protected int defineOffsetReferenceMapPageNumbers() { return 1; }
    
    @Override
    protected int defineOffsetFreeSpace() { return 2; }
    @Override
    protected int defineOffsetNumRowsOnDataPage() { return 12; }
    @Override
    protected int defineMaxNumRowsOnDataPage() { return 255; }
    
    @Override
    protected int defineOffsetIndexCompressedByteCount() { return 24; }
    @Override
    protected int defineOffsetIndexEntryMask() { return 27; }
    @Override
    protected int defineOffsetPrevIndexPage() { return 12; }
    @Override
    protected int defineOffsetNextIndexPage() { return 16; }
    @Override
    protected int defineOffsetChildTailIndexPage() { return 20; }
    
    @Override
    protected int defineSizeIndexDefinition() { return 12; }
    @Override
    protected int defineSizeColumnHeader() { return 25; }
    @Override
    protected int defineSizeRowLocation() { return 2; }
    @Override
    protected int defineSizeLongValueDef() { return 12; }
    @Override
    protected int defineMaxInlineLongValueSize() { return 64; }
    @Override
    protected int defineMaxLongValueRowSize() { return 4076; }
    @Override
    protected int defineSizeTdefHeader() { return 63; }
    @Override
    protected int defineSizeTdefTrailer() { return 2; }
    @Override
    protected int defineSizeColumnDefBlock() { return 25; }
    @Override
    protected int defineSizeIndexEntryMask() { return 453; }
    @Override
    protected int defineSkipBeforeIndexFlags() { return 4; }
    @Override
    protected int defineSkipAfterIndexFlags() { return 5; }
    @Override
    protected int defineSkipBeforeIndexSlot() { return 4; }
    @Override
    protected int defineSkipAfterIndexSlot() { return 4; }
    @Override
    protected int defineSkipBeforeIndex() { return 4; }
    @Override
    protected int defineSizeNameLength() { return 2; }
    @Override
    protected int defineSizeRowColumnCount() { return 2; }
    @Override
    protected int defineSizeRowVarColOffset() { return 2; }
    
    @Override
    protected int defineUsageMapTableByteLength() { return 64; }
      
    @Override
    protected int defineMaxColumnsPerTable() { return 255; }
      
    @Override
    protected int defineMaxTableNameLength() { return 64; }
      
    @Override
    protected int defineMaxColumnNameLength() { return 64; }
      
    @Override
    protected int defineMaxIndexNameLength() { return 64; }
      
    @Override
    protected boolean defineLegacyNumericIndexes() { return true; }

    @Override
    protected Charset defineCharset() { return Charset.forName("UTF-16LE"); }

    @Override
    protected Column.SortOrder defineDefaultSortOrder() {
      return Column.GENERAL_LEGACY_SORT_ORDER;
    }

    @Override
    protected Map<String,Database.FileFormat> getPossibleFileFormats()
    {
      return PossibleFileFormats.POSSIBLE_VERSION_4;
    }

    @Override
    protected boolean isSupportedDataType(DataType type) {
      return (type != DataType.COMPLEX_TYPE);
    }
  }
  
  private static final class MSISAMFormat extends Jet4Format {
    private MSISAMFormat() {
      super("MSISAM");
    }

    @Override
    protected CodecType defineCodecType() { 
      return CodecType.MSISAM; 
    }

    @Override
    protected Map<String,Database.FileFormat> getPossibleFileFormats()
    {
      return PossibleFileFormats.POSSIBLE_VERSION_MSISAM;
    }
  }

  private static class Jet12Format extends Jet4Format {
    private Jet12Format() {
      super("VERSION_12");
    }


    private Jet12Format(String name) {
      super(name);
    }

    @Override
    protected CodecType defineCodecType() { 
      return CodecType.OFFICE; 
    }

    @Override
    protected boolean defineLegacyNumericIndexes() { return false; }

    @Override
    protected Map<String,Database.FileFormat> getPossibleFileFormats() {
      return PossibleFileFormats.POSSIBLE_VERSION_12;
    }

    @Override
    protected int defineOffsetNextComplexAutoNumber() { return 28; }

    @Override
    protected int defineOffsetColumnComplexId() { return 11; }
    
    @Override
    protected boolean isSupportedDataType(DataType type) {
      return true;
    }
  }

  private static final class Jet14Format extends Jet12Format {
      private Jet14Format() {
        super("VERSION_14");
      }

    @Override
    protected Column.SortOrder defineDefaultSortOrder() {
      return Column.GENERAL_SORT_ORDER;
    }

    @Override
    protected Map<String,Database.FileFormat> getPossibleFileFormats() {
      return PossibleFileFormats.POSSIBLE_VERSION_14;
    }
  }

}
