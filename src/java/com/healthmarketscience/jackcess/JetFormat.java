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

/**
 * Encapsulates constants describing a specific version of the Access Jet format
 * @author Tim McCune
 */
public abstract class JetFormat {
  
  /** Maximum size of a record minus OLE objects and Memo fields */
  public static final int MAX_RECORD_SIZE = 1900;  //2kb minus some overhead
  
  /** Maximum size of a text field */
  public static final short TEXT_FIELD_MAX_LENGTH = 255 * 2;
  
  /** Offset in the file that holds the byte describing the Jet format version */
  private static final long OFFSET_VERSION = 20L;
  /** Version code for Jet version 3 */
  private static final byte CODE_VERSION_3 = 0x0;
  /** Version code for Jet version 4 */
  private static final byte CODE_VERSION_4 = 0x1;

  //These constants are populated by this class's constructor.  They can't be
  //populated by the subclass's constructor because they are final, and Java
  //doesn't allow this; hence all the abstract defineXXX() methods.
  
  /** Database page size in bytes */
  public final int PAGE_SIZE;
  
  public final int MAX_ROW_SIZE;
  
  public final int OFFSET_NEXT_TABLE_DEF_PAGE;
  public final int OFFSET_NUM_ROWS;
  public final int OFFSET_TABLE_TYPE;
  public final int OFFSET_NUM_COLS;
  public final int OFFSET_NUM_INDEXES;
  public final int OFFSET_OWNED_PAGES;
  public final int OFFSET_FREE_SPACE_PAGES;
  public final int OFFSET_INDEX_DEF_BLOCK;
  
  public final int OFFSET_COLUMN_TYPE;
  public final int OFFSET_COLUMN_NUMBER;
  public final int OFFSET_COLUMN_PRECISION;
  public final int OFFSET_COLUMN_SCALE;
  public final int OFFSET_COLUMN_VARIABLE;
  public final int OFFSET_COLUMN_COMPRESSED_UNICODE;
  public final int OFFSET_COLUMN_LENGTH;
  
  public final int OFFSET_TABLE_DEF_LOCATION;
  public final int OFFSET_NUM_ROWS_ON_PAGE;
  public final int OFFSET_ROW_LOCATION_BLOCK;
  
  public final int OFFSET_ROW_START;
  public final int OFFSET_MAP_START;
  
  public final int OFFSET_USAGE_MAP_PAGE_DATA;
  
  public final int OFFSET_REFERENCE_MAP_PAGE_NUMBERS;
  
  public final int OFFSET_FREE_SPACE;
  public final int OFFSET_DATA_ROW_LOCATION_BLOCK;
  public final int OFFSET_NUM_ROWS_ON_DATA_PAGE;
  
  public final int OFFSET_LVAL_ROW_LOCATION_BLOCK;
  
  public final int OFFSET_USED_PAGES_USAGE_MAP_DEF;
  public final int OFFSET_FREE_PAGES_USAGE_MAP_DEF;
  
  public final int OFFSET_INDEX_ENTRY_MASK;
  
  public final int SIZE_INDEX_DEFINITION;
  public final int SIZE_COLUMN_HEADER;
  public final int SIZE_ROW_LOCATION;
  public final int SIZE_LONG_VALUE_DEF;
  public final int SIZE_TDEF_BLOCK;
  public final int SIZE_COLUMN_DEF_BLOCK;
  public final int SIZE_INDEX_ENTRY_MASK;
  
  public final int PAGES_PER_USAGE_MAP_PAGE;
  
  public final Charset CHARSET;
  
  public static final JetFormat VERSION_4 = new Jet4Format();
  
  /**
   * @return The Jet Format represented in the passed-in file
   */
  public static JetFormat getFormat(FileChannel channel) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(1);
    channel.read(buffer, OFFSET_VERSION);
    buffer.flip();
    byte version = buffer.get();
    if (version == CODE_VERSION_4) {
      return VERSION_4;
    } else {
      throw new IOException("Unsupported version: " + version);
    }
  }
  
  private JetFormat() {
    
    PAGE_SIZE = definePageSize();
    
    MAX_ROW_SIZE = defineMaxRowSize();
    
    OFFSET_NEXT_TABLE_DEF_PAGE = defineOffsetNextTableDefPage();
    OFFSET_NUM_ROWS = defineOffsetNumRows();
    OFFSET_TABLE_TYPE = defineOffsetTableType();
    OFFSET_NUM_COLS = defineOffsetNumCols();
    OFFSET_NUM_INDEXES = defineOffsetNumIndexes();
    OFFSET_OWNED_PAGES = defineOffsetOwnedPages();
    OFFSET_FREE_SPACE_PAGES = defineOffsetFreeSpacePages();
    OFFSET_INDEX_DEF_BLOCK = defineOffsetIndexDefBlock();
    
    OFFSET_COLUMN_TYPE = defineOffsetColumnType();
    OFFSET_COLUMN_NUMBER = defineOffsetColumnNumber();
    OFFSET_COLUMN_PRECISION = defineOffsetColumnPrecision();
    OFFSET_COLUMN_SCALE = defineOffsetColumnScale();
    OFFSET_COLUMN_VARIABLE = defineOffsetColumnVariable();
    OFFSET_COLUMN_COMPRESSED_UNICODE = defineOffsetColumnCompressedUnicode();
    OFFSET_COLUMN_LENGTH = defineOffsetColumnLength();
    
    OFFSET_TABLE_DEF_LOCATION = defineOffsetTableDefLocation();
    OFFSET_NUM_ROWS_ON_PAGE = defineOffsetNumRowsOnPage();
    OFFSET_ROW_LOCATION_BLOCK = defineOffsetRowLocationBlock();
    
    OFFSET_ROW_START = defineOffsetRowStart();
    OFFSET_MAP_START = defineOffsetMapStart();
    
    OFFSET_USAGE_MAP_PAGE_DATA = defineOffsetUsageMapPageData();
    
    OFFSET_REFERENCE_MAP_PAGE_NUMBERS = defineOffsetReferenceMapPageNumbers();
    
    OFFSET_FREE_SPACE = defineOffsetFreeSpace();
    OFFSET_DATA_ROW_LOCATION_BLOCK = defineOffsetDataRowLocationBlock();
    OFFSET_NUM_ROWS_ON_DATA_PAGE = defineOffsetNumRowsOnDataPage();
    
    OFFSET_LVAL_ROW_LOCATION_BLOCK = defineOffsetLvalRowLocationBlock();
    
    OFFSET_USED_PAGES_USAGE_MAP_DEF = defineOffsetUsedPagesUsageMapDef();
    OFFSET_FREE_PAGES_USAGE_MAP_DEF = defineOffsetFreePagesUsageMapDef();
    
    OFFSET_INDEX_ENTRY_MASK = defineOffsetIndexEntryMask();
    
    SIZE_INDEX_DEFINITION = defineSizeIndexDefinition();
    SIZE_COLUMN_HEADER = defineSizeColumnHeader();
    SIZE_ROW_LOCATION = defineSizeRowLocation();
    SIZE_LONG_VALUE_DEF = defineSizeLongValueDef();
    SIZE_TDEF_BLOCK = defineSizeTdefBlock();
    SIZE_COLUMN_DEF_BLOCK = defineSizeColumnDefBlock();
    SIZE_INDEX_ENTRY_MASK = defineSizeIndexEntryMask();
    
    PAGES_PER_USAGE_MAP_PAGE = definePagesPerUsageMapPage();
    
    CHARSET = defineCharset();
  }
  
  protected abstract int definePageSize();
  
  protected abstract int defineMaxRowSize();
  
  protected abstract int defineOffsetNextTableDefPage();
  protected abstract int defineOffsetNumRows();
  protected abstract int defineOffsetTableType();
  protected abstract int defineOffsetNumCols();
  protected abstract int defineOffsetNumIndexes();
  protected abstract int defineOffsetOwnedPages();
  protected abstract int defineOffsetFreeSpacePages();
  protected abstract int defineOffsetIndexDefBlock();
  
  protected abstract int defineOffsetColumnType();
  protected abstract int defineOffsetColumnNumber();
  protected abstract int defineOffsetColumnPrecision();
  protected abstract int defineOffsetColumnScale();
  protected abstract int defineOffsetColumnVariable();
  protected abstract int defineOffsetColumnCompressedUnicode();
  protected abstract int defineOffsetColumnLength();

  protected abstract int defineOffsetTableDefLocation();
  protected abstract int defineOffsetNumRowsOnPage();
  protected abstract int defineOffsetRowLocationBlock();
  
  protected abstract int defineOffsetRowStart();
  protected abstract int defineOffsetMapStart();
  
  protected abstract int defineOffsetUsageMapPageData();
  
  protected abstract int defineOffsetReferenceMapPageNumbers();
  
  protected abstract int defineOffsetFreeSpace();
  protected abstract int defineOffsetDataRowLocationBlock();
  protected abstract int defineOffsetNumRowsOnDataPage();
  
  protected abstract int defineOffsetLvalRowLocationBlock();
  
  protected abstract int defineOffsetUsedPagesUsageMapDef();
  protected abstract int defineOffsetFreePagesUsageMapDef();
  
  protected abstract int defineOffsetIndexEntryMask();
  
  protected abstract int defineSizeIndexDefinition();
  protected abstract int defineSizeColumnHeader();
  protected abstract int defineSizeRowLocation();
  protected abstract int defineSizeLongValueDef();
  protected abstract int defineSizeTdefBlock();
  protected abstract int defineSizeColumnDefBlock();
  protected abstract int defineSizeIndexEntryMask();
  
  protected abstract int definePagesPerUsageMapPage();
    
  protected abstract Charset defineCharset();
  
  private static final class Jet4Format extends JetFormat {
    
    protected int definePageSize() { return 4096; }
    
    protected int defineMaxRowSize() { return PAGE_SIZE - 18; }
    
    protected int defineOffsetNextTableDefPage() { return 4; }
    protected int defineOffsetNumRows() { return 16; }
    protected int defineOffsetTableType() { return 40; }
    protected int defineOffsetNumCols() { return 45; }
    protected int defineOffsetNumIndexes() { return 47; }
    protected int defineOffsetOwnedPages() { return 55; }
    protected int defineOffsetFreeSpacePages() { return 59; }
    protected int defineOffsetIndexDefBlock() { return 63; }
    
    protected int defineOffsetColumnType() { return 0; }
    protected int defineOffsetColumnNumber() { return 5; }
    protected int defineOffsetColumnPrecision() { return 11; }
    protected int defineOffsetColumnScale() { return 12; }
    protected int defineOffsetColumnVariable() { return 15; }
    protected int defineOffsetColumnCompressedUnicode() { return 16; }
    protected int defineOffsetColumnLength() { return 23; }
  
    protected int defineOffsetTableDefLocation() { return 4; }
    protected int defineOffsetNumRowsOnPage() { return 12; }
    protected int defineOffsetRowLocationBlock() { return 16; }
    
    protected int defineOffsetRowStart() { return 14; }
    protected int defineOffsetMapStart() { return 5; }
    
    protected int defineOffsetUsageMapPageData() { return 4; }
    
    protected int defineOffsetReferenceMapPageNumbers() { return 1; }
    
    protected int defineOffsetFreeSpace() { return 2; }
    protected int defineOffsetDataRowLocationBlock() { return 14; }
    protected int defineOffsetNumRowsOnDataPage() { return 12; }
    
    protected int defineOffsetLvalRowLocationBlock() { return 10; }
    
    protected int defineOffsetUsedPagesUsageMapDef() { return 4027; }
    protected int defineOffsetFreePagesUsageMapDef() { return 3958; }
    
    protected int defineOffsetIndexEntryMask() { return 27; }
    
    protected int defineSizeIndexDefinition() { return 12; }
    protected int defineSizeColumnHeader() { return 25; }
    protected int defineSizeRowLocation() { return 2; }
    protected int defineSizeLongValueDef() { return 12; }
    protected int defineSizeTdefBlock() { return 63; }
    protected int defineSizeColumnDefBlock() { return 25; }
    protected int defineSizeIndexEntryMask() { return 453; }
    
    protected int definePagesPerUsageMapPage() { return 4092 * 8; }
      
    protected Charset defineCharset() { return Charset.forName("UTF-16LE"); }
  }
  
}
