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
import java.util.EnumMap;
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
  
  /** Offset in the file that holds the byte describing the Jet format
      version */
  private static final long OFFSET_VERSION = 20L;
  /** Version code for Jet version 3 */
  private static final byte CODE_VERSION_3 = 0x0;
  /** Version code for Jet version 4 */
  private static final byte CODE_VERSION_4 = 0x1;
  /** Version code for Jet version 5 */
  private static final byte CODE_VERSION_5 = 0x2;

  /** value of the "AccessVersion" property for access 2000 dbs:
      {@code "08.50"} */
  private static final byte[] ACCESS_VERSION_2000 = new byte[] {
    '0', 0, '8', 0, '.', 0, '5', 0, '0', 0};
  /** value of the "AccessVersion" property for access 2002/2003 dbs
      {@code "09.50"}  */
  private static final byte[] ACCESS_VERSION_2003 = new byte[] {
    '0', 0, '9', 0, '.', 0, '5', 0, '0', 0};

  // use nested inner class to avoid problematic static init loops
  private static final class PossibleFileFormats {
    private static final Map<Database.FileFormat,byte[]> POSSIBLE_VERSION_3 = 
      Collections.singletonMap(Database.FileFormat.V1997, (byte[])null);

    private static final Map<Database.FileFormat,byte[]> POSSIBLE_VERSION_4 = 
      new EnumMap<Database.FileFormat,byte[]>(Database.FileFormat.class);

    private static final Map<Database.FileFormat,byte[]> POSSIBLE_VERSION_5 = 
      Collections.singletonMap(Database.FileFormat.V2007, (byte[])null);

    static {
      POSSIBLE_VERSION_4.put(Database.FileFormat.V2000, ACCESS_VERSION_2000);
      POSSIBLE_VERSION_4.put(Database.FileFormat.V2003, ACCESS_VERSION_2003);
    }
  }

  //These constants are populated by this class's constructor.  They can't be
  //populated by the subclass's constructor because they are final, and Java
  //doesn't allow this; hence all the abstract defineXXX() methods.

  /** the name of this format */
  private final String _name;
  
  /** the read/write mode of this format */
  public final boolean READ_ONLY;
  
  /** whether or not we can use indexes in this format */
  public final boolean INDEXES_SUPPORTED;
  
  /** Database page size in bytes */
  public final int PAGE_SIZE;
  public final long MAX_DATABASE_SIZE;
  
  public final int MAX_ROW_SIZE;
  public final int PAGE_INITIAL_FREE_SPACE;
  
  public final int OFFSET_NEXT_TABLE_DEF_PAGE;
  public final int OFFSET_NUM_ROWS;
  public final int OFFSET_NEXT_AUTO_NUMBER;
  public final int OFFSET_TABLE_TYPE;
  public final int OFFSET_MAX_COLS;
  public final int OFFSET_NUM_VAR_COLS;
  public final int OFFSET_NUM_COLS;
  public final int OFFSET_NUM_INDEX_SLOTS;
  public final int OFFSET_NUM_INDEXES;
  public final int OFFSET_OWNED_PAGES;
  public final int OFFSET_FREE_SPACE_PAGES;
  public final int OFFSET_INDEX_DEF_BLOCK;
  
  public final int OFFSET_INDEX_NUMBER_BLOCK;
  
  public final int OFFSET_COLUMN_TYPE;
  public final int OFFSET_COLUMN_NUMBER;
  public final int OFFSET_COLUMN_PRECISION;
  public final int OFFSET_COLUMN_SCALE;
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

  public final boolean REVERSE_FIRST_BYTE_IN_DESC_NUMERIC_INDEXES;
  
  public final Charset CHARSET;
  
  public static final JetFormat VERSION_3 = new Jet3Format();
  public static final JetFormat VERSION_4 = new Jet4Format();
  public static final JetFormat VERSION_5 = new Jet5Format();

  /**
   * @param channel the database file.
   * @return The Jet Format represented in the passed-in file
   * @throws IOException if the database file format is unsupported.
   */
  public static JetFormat getFormat(FileChannel channel) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(1);
    int bytesRead = channel.read(buffer, OFFSET_VERSION);
    if(bytesRead < 1) {
      throw new IOException("Empty database file");
    }
    buffer.flip();
    byte version = buffer.get();
    if (version == CODE_VERSION_3) {
      return VERSION_3;
    } else if (version == CODE_VERSION_4) {
      return VERSION_4;
    } else if (version == CODE_VERSION_5) {
      return VERSION_5;
    }
    throw new IOException("Unsupported " +
                          ((version < CODE_VERSION_3) ? "older" : "newer") +
                          " version: " + version);
  }
  
  private JetFormat(String name) {
    _name = name;
    
    READ_ONLY = defineReadOnly();
    INDEXES_SUPPORTED = defineIndexesSupported();
    
    PAGE_SIZE = definePageSize();
    MAX_DATABASE_SIZE = defineMaxDatabaseSize();
    
    MAX_ROW_SIZE = defineMaxRowSize();
    PAGE_INITIAL_FREE_SPACE = definePageInitialFreeSpace();
    
    OFFSET_NEXT_TABLE_DEF_PAGE = defineOffsetNextTableDefPage();
    OFFSET_NUM_ROWS = defineOffsetNumRows();
    OFFSET_NEXT_AUTO_NUMBER = defineOffsetNextAutoNumber();
    OFFSET_TABLE_TYPE = defineOffsetTableType();
    OFFSET_MAX_COLS = defineOffsetMaxCols();
    OFFSET_NUM_VAR_COLS = defineOffsetNumVarCols();
    OFFSET_NUM_COLS = defineOffsetNumCols();
    OFFSET_NUM_INDEX_SLOTS = defineOffsetNumIndexSlots();
    OFFSET_NUM_INDEXES = defineOffsetNumIndexes();
    OFFSET_OWNED_PAGES = defineOffsetOwnedPages();
    OFFSET_FREE_SPACE_PAGES = defineOffsetFreeSpacePages();
    OFFSET_INDEX_DEF_BLOCK = defineOffsetIndexDefBlock();
    
    OFFSET_INDEX_NUMBER_BLOCK = defineOffsetIndexNumberBlock();
    
    OFFSET_COLUMN_TYPE = defineOffsetColumnType();
    OFFSET_COLUMN_NUMBER = defineOffsetColumnNumber();
    OFFSET_COLUMN_PRECISION = defineOffsetColumnPrecision();
    OFFSET_COLUMN_SCALE = defineOffsetColumnScale();
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
    
    REVERSE_FIRST_BYTE_IN_DESC_NUMERIC_INDEXES = defineReverseFirstByteInDescNumericIndexes();
    
    CHARSET = defineCharset();
  }
  
  protected abstract boolean defineReadOnly();
  protected abstract boolean defineIndexesSupported();
  
  protected abstract int definePageSize();
  protected abstract long defineMaxDatabaseSize();
  
  protected abstract int defineMaxRowSize();
  protected abstract int definePageInitialFreeSpace();
  
  protected abstract int defineOffsetNextTableDefPage();
  protected abstract int defineOffsetNumRows();
  protected abstract int defineOffsetNextAutoNumber();
  protected abstract int defineOffsetTableType();
  protected abstract int defineOffsetMaxCols();
  protected abstract int defineOffsetNumVarCols();
  protected abstract int defineOffsetNumCols();
  protected abstract int defineOffsetNumIndexSlots();
  protected abstract int defineOffsetNumIndexes();
  protected abstract int defineOffsetOwnedPages();
  protected abstract int defineOffsetFreeSpacePages();
  protected abstract int defineOffsetIndexDefBlock();
  
  protected abstract int defineOffsetIndexNumberBlock();
  
  protected abstract int defineOffsetColumnType();
  protected abstract int defineOffsetColumnNumber();
  protected abstract int defineOffsetColumnPrecision();
  protected abstract int defineOffsetColumnScale();
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

  protected abstract boolean defineReverseFirstByteInDescNumericIndexes();

  protected abstract Map<Database.FileFormat,byte[]> getPossibleFileFormats();

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
    protected int definePageSize() { return 2048; }
	    
    @Override
    protected long defineMaxDatabaseSize() {
      return (1L * 1024L * 1024L * 1024L);
    }
	    
    @Override
    protected int defineMaxRowSize() { return 2012; }
    @Override
    protected int definePageInitialFreeSpace() { return PAGE_SIZE - 14; }
	    
    @Override
    protected int defineOffsetNextTableDefPage() { return 4; }
    @Override
    protected int defineOffsetNumRows() { return 12; }
    @Override
    protected int defineOffsetNextAutoNumber() { return 20; }
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
    protected int defineOffsetIndexNumberBlock() { return 39; }
	    
    @Override
    protected int defineOffsetColumnType() { return 0; }
    @Override
    protected int defineOffsetColumnNumber() { return 1; }
    @Override
    protected int defineOffsetColumnPrecision() { return 11; }
    @Override
    protected int defineOffsetColumnScale() { return 12; }
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
    protected boolean defineReverseFirstByteInDescNumericIndexes() { return false; }

    @Override
    protected Charset defineCharset() { return Charset.defaultCharset(); }

    @Override
    protected Map<Database.FileFormat,byte[]> getPossibleFileFormats()
    {
      return PossibleFileFormats.POSSIBLE_VERSION_3;
    }

  }
  
  private static class Jet4Format extends JetFormat {

    private Jet4Format() {
      this("VERSION_4");
    }

    private Jet4Format(final String name) {
      super(name);
    }

    @Override
    protected boolean defineReadOnly() { return false; }
    
    @Override
    protected boolean defineIndexesSupported() { return true; }
	    
    @Override
    protected int definePageSize() { return 4096; }
    
    @Override
    protected long defineMaxDatabaseSize() {
      return (2L * 1024L * 1024L * 1024L);
    }
    
    @Override
    protected int defineMaxRowSize() { return 4060; }
    @Override
    protected int definePageInitialFreeSpace() { return PAGE_SIZE - 14; }
    
    @Override
    protected int defineOffsetNextTableDefPage() { return 4; }
    @Override
    protected int defineOffsetNumRows() { return 16; }
    @Override
    protected int defineOffsetNextAutoNumber() { return 20; }
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
    protected int defineOffsetIndexNumberBlock() { return 52; }
    
    @Override
    protected int defineOffsetColumnType() { return 0; }
    @Override
    protected int defineOffsetColumnNumber() { return 5; }
    @Override
    protected int defineOffsetColumnPrecision() { return 11; }
    @Override
    protected int defineOffsetColumnScale() { return 12; }
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
    protected boolean defineReverseFirstByteInDescNumericIndexes() { return false; }

    @Override
    protected Charset defineCharset() { return Charset.forName("UTF-16LE"); }

    @Override
    protected Map<Database.FileFormat,byte[]> getPossibleFileFormats()
    {
      return PossibleFileFormats.POSSIBLE_VERSION_4;
    }

  }
  
  private static final class Jet5Format extends Jet4Format {
      private Jet5Format() {
        super("VERSION_5");
      }

    @Override
    protected boolean defineReverseFirstByteInDescNumericIndexes() { return true; }

    @Override
    protected Map<Database.FileFormat,byte[]> getPossibleFileFormats() {
      return PossibleFileFormats.POSSIBLE_VERSION_5;
    }
  }

}
