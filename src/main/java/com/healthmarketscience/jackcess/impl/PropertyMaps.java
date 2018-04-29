/*
Copyright (c) 2011 James Ahlborn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.healthmarketscience.jackcess.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.PropertyMap;

/**
 * Collection of PropertyMap instances read from a single property data block.
 *
 * @author James Ahlborn
 */
public class PropertyMaps implements Iterable<PropertyMapImpl>
{
  /** the name of the "default" properties for a PropertyMaps instance */
  public static final String DEFAULT_NAME = "";

  private static final short PROPERTY_NAME_LIST = 0x80;
  private static final short DEFAULT_PROPERTY_VALUE_LIST = 0x00;
  private static final short COLUMN_PROPERTY_VALUE_LIST = 0x01;

  /** maps the PropertyMap name (case-insensitive) to the PropertyMap
      instance */
  private final Map<String,PropertyMapImpl> _maps =
    new LinkedHashMap<String,PropertyMapImpl>();
  private final int _objectId;
  private final RowIdImpl _rowId;
  private final Handler _handler;
  private final Owner _owner;

  public PropertyMaps(int objectId, RowIdImpl rowId, Handler handler,
                      Owner owner) {
    _objectId = objectId;
    _rowId = rowId;
    _handler = handler;
    _owner = owner;
  }

  public int getObjectId() {
    return _objectId;
  }

  public int getSize() {
    return _maps.size();
  }

  public boolean isEmpty() {
    return _maps.isEmpty();
  }

  /**
   * @return the unnamed "default" PropertyMap in this group, creating if
   *         necessary.
   */
  public PropertyMapImpl getDefault() {
    return get(DEFAULT_NAME, DEFAULT_PROPERTY_VALUE_LIST);
  }

  /**
   * @return the PropertyMap with the given name in this group, creating if
   *         necessary
   */
  public PropertyMapImpl get(String name) {
    return get(name, COLUMN_PROPERTY_VALUE_LIST);
  }

  /**
   * @return the PropertyMap with the given name and type in this group,
   *         creating if necessary
   */
  private PropertyMapImpl get(String name, short type) {
    String lookupName = DatabaseImpl.toLookupName(name);
    PropertyMapImpl map = _maps.get(lookupName);
    if(map == null) {
      map = new PropertyMapImpl(name, type, this);
      _maps.put(lookupName, map);
    }
    return map;
  }

  public Iterator<PropertyMapImpl> iterator() {
    return _maps.values().iterator();
  }

  public byte[] write() throws IOException {
    return _handler.write(this);
  }

  public void save() throws IOException {
    _handler.save(this);
    if(_owner != null) {
      _owner.propertiesUpdated();
    }
  }

  @Override
  public String toString() {
    return CustomToStringStyle.builder(this)
      .append(null, _maps.values())
      .toString();
  }

  /**
   * Utility class for reading/writing property blocks.
   */
  static final class Handler
  {
    /** the current database */
    private final DatabaseImpl _database;
    /** the system table "property" column */
    private final ColumnImpl _propCol;
    /** cache of PropColumns used to read/write property values */
    private final Map<DataType,PropColumn> _columns =
      new HashMap<DataType,PropColumn>();

    Handler(DatabaseImpl database) {
      _database = database;
      _propCol = _database.getSystemCatalog().getColumn(
          DatabaseImpl.CAT_COL_PROPS);
    }

    /**
     * @return a PropertyMaps instance decoded from the given bytes (always
     *         returns non-{@code null} result).
     */
    public PropertyMaps read(byte[] propBytes, int objectId,
                             RowIdImpl rowId, Owner owner)
      throws IOException
    {
      PropertyMaps maps = new PropertyMaps(objectId, rowId, this, owner);
      if((propBytes == null) || (propBytes.length == 0)) {
        return maps;
      }

      ByteBuffer bb = PageChannel.wrap(propBytes);

      // check for known header
      boolean knownType = false;
      for(byte[] tmpType : JetFormat.PROPERTY_MAP_TYPES) {
        if(ByteUtil.matchesRange(bb, bb.position(), tmpType)) {
          ByteUtil.forward(bb, tmpType.length);
          knownType = true;
          break;
        }
      }

      if(!knownType) {
        throw new IOException("Unknown property map type " +
                              ByteUtil.toHexString(bb, 4));
      }

      // parse each data "chunk"
      List<String> propNames = null;
      while(bb.hasRemaining()) {

        int len = bb.getInt();
        short type = bb.getShort();
        int endPos = bb.position() + len - 6;

        ByteBuffer bbBlock = PageChannel.narrowBuffer(bb, bb.position(),
                                                      endPos);

        if(type == PROPERTY_NAME_LIST) {
          propNames = readPropertyNames(bbBlock);
        } else {
          readPropertyValues(bbBlock, propNames, type, maps);
        }

        bb.position(endPos);
      }

      return maps;
    }

    /**
     * @return a byte[] encoded from the given PropertyMaps instance
     */
    public byte[] write(PropertyMaps maps)
      throws IOException
    {
      if(maps == null) {
        return null;
      }

      ByteArrayBuilder bab = new ByteArrayBuilder();

      bab.put(_database.getFormat().PROPERTY_MAP_TYPE);

      // grab the property names from all the maps
      Set<String> propNames = new LinkedHashSet<String>();
      for(PropertyMapImpl propMap : maps) {
        for(PropertyMap.Property prop : propMap) {
          propNames.add(prop.getName());
        }
      }

      if(propNames.isEmpty()) {
        return null;
      }

      // write the full set of property names
      writeBlock(null, propNames, PROPERTY_NAME_LIST, bab);

      // write all the map values
      for(PropertyMapImpl propMap : maps) {
        if(!propMap.isEmpty()) {
          writeBlock(propMap, propNames, propMap.getType(), bab);
        }
      }

      return bab.toArray();
    }

    /**
     * Saves PropertyMaps instance to the db.
     */
    public void save(PropertyMaps maps) throws IOException
    {
      RowIdImpl rowId = maps._rowId;
      if(rowId == null) {
        throw new IllegalStateException(
            "PropertyMaps cannot be saved without a row id");
      }

      byte[] mapsBytes = write(maps);

      // for now assume all properties come from system catalog table
      _propCol.getTable().updateValue(_propCol, rowId, mapsBytes);
    }

    private void writeBlock(
        PropertyMapImpl propMap, Set<String> propNames,
        short blockType, ByteArrayBuilder bab)
      throws IOException
    {
      int blockStartPos = bab.position();
      bab.reserveInt()
        .putShort(blockType);

      if(blockType == PROPERTY_NAME_LIST) {
        writePropertyNames(propNames, bab);
      } else {
        writePropertyValues(propMap, propNames, bab);
      }

      int len = bab.position() - blockStartPos;
      bab.putInt(blockStartPos, len);
    }

    /**
     * @return the property names parsed from the given data chunk
     */
    private List<String> readPropertyNames(ByteBuffer bbBlock) {
      List<String> names = new ArrayList<String>();
      while(bbBlock.hasRemaining()) {
        names.add(readPropName(bbBlock));
      }
      return names;
    }

    private void writePropertyNames(Set<String> propNames,
                                    ByteArrayBuilder bab) {
      for(String propName : propNames) {
        writePropName(propName, bab);
      }
    }

    /**
     * @return the PropertyMap created from the values parsed from the given
     *         data chunk combined with the given property names
     */
    private PropertyMapImpl readPropertyValues(
        ByteBuffer bbBlock, List<String> propNames, short blockType,
        PropertyMaps maps)
      throws IOException
    {
      String mapName = DEFAULT_NAME;

      if(bbBlock.hasRemaining()) {

        // read the map name, if any
        int nameBlockLen = bbBlock.getInt();
        int endPos = bbBlock.position() + nameBlockLen - 4;
        if(nameBlockLen > 6) {
          mapName = readPropName(bbBlock);
        }
        bbBlock.position(endPos);
      }

      PropertyMapImpl map = maps.get(mapName, blockType);

      // read the values
      while(bbBlock.hasRemaining()) {

        int valLen = bbBlock.getShort();
        int endPos = bbBlock.position() + valLen - 2;
        boolean isDdl = (bbBlock.get() != 0);
        DataType dataType = DataType.fromByte(bbBlock.get());
        int nameIdx = bbBlock.getShort();
        int dataSize = bbBlock.getShort();

        String propName = propNames.get(nameIdx);
        PropColumn col = getColumn(dataType, propName, dataSize, null);

        byte[] data = ByteUtil.getBytes(bbBlock, dataSize);
        Object value = col.read(data);

        map.put(propName, dataType, value, isDdl);

        bbBlock.position(endPos);
      }

      return map;
    }

    private void writePropertyValues(
        PropertyMapImpl propMap, Set<String> propNames, ByteArrayBuilder bab)
      throws IOException
    {
      // write the map name, if any
      String mapName = propMap.getName();
      int blockStartPos = bab.position();
      bab.reserveInt();
      writePropName(mapName, bab);
      int len = bab.position() - blockStartPos;
      bab.putInt(blockStartPos, len);

      // write the map values
      int nameIdx = 0;
      for(String propName : propNames) {

        PropertyMapImpl.PropertyImpl prop = (PropertyMapImpl.PropertyImpl)
          propMap.get(propName);

        if(prop != null) {

          Object value = prop.getValue();
          if(value != null) {

            int valStartPos = bab.position();
            bab.reserveShort();

            byte ddlFlag = (byte)(prop.isDdl() ? 1 : 0);
            bab.put(ddlFlag);
            bab.put(prop.getType().getValue());
            bab.putShort((short)nameIdx);

            PropColumn col = getColumn(prop.getType(), propName, -1, value);

            ByteBuffer data = col.write(
                value, _database.getFormat().MAX_ROW_SIZE);

            bab.putShort((short)data.remaining());
            bab.put(data);

            len = bab.position() - valStartPos;
            bab.putShort(valStartPos, (short)len);
          }
        }

        ++nameIdx;
      }
    }

    /**
     * Reads a property name from the given data block
     */
    private String readPropName(ByteBuffer buffer) {
      int nameLength = buffer.getShort();
      byte[] nameBytes = ByteUtil.getBytes(buffer, nameLength);
      return ColumnImpl.decodeUncompressedText(nameBytes, _database.getCharset());
    }

    /**
     * Writes a property name to the given data block
     */
    private void writePropName(String propName, ByteArrayBuilder bab) {
      ByteBuffer textBuf = ColumnImpl.encodeUncompressedText(
          propName, _database.getCharset());
      bab.putShort((short)textBuf.remaining());
      bab.put(textBuf);
    }

    /**
     * Gets a PropColumn capable of reading/writing a property of the given
     * DataType
     */
    private PropColumn getColumn(DataType dataType, String propName,
                                 int dataSize, Object value)
      throws IOException
    {

      if(isPseudoGuidColumn(dataType, propName, dataSize, value)) {
        dataType = DataType.GUID;
      }

      PropColumn col = _columns.get(dataType);

      if(col == null) {

        // translate long value types into simple types
        DataType colType = dataType;
        if(dataType == DataType.MEMO) {
          colType = DataType.TEXT;
        } else if(dataType == DataType.OLE) {
          colType = DataType.BINARY;
        }

        // create column with ability to read/write the given data type
        col = ((colType == DataType.BOOLEAN) ?
               new BooleanPropColumn() : new PropColumn(colType));

        _columns.put(dataType, col);
      }

      return col;
    }

    private static boolean isPseudoGuidColumn(
        DataType dataType, String propName, int dataSize, Object value)
      throws IOException
    {
      // guids seem to be marked as "binary" fields
      return((dataType == DataType.BINARY) &&
             ((dataSize == DataType.GUID.getFixedSize()) ||
              ((dataSize == -1) && ColumnImpl.isGUIDValue(value))) &&
             PropertyMap.GUID_PROP.equalsIgnoreCase(propName));
    }

    /**
     * Column adapted to work w/out a Table.
     */
    private class PropColumn extends ColumnImpl
    {
      private PropColumn(DataType type) {
        super(null, null, type, 0, 0, 0);
      }

      @Override
      public DatabaseImpl getDatabase() {
        return _database;
      }
    }

    /**
     * Normal boolean columns do not write into the actual row data, so we
     * need to do a little extra work.
     */
    private final class BooleanPropColumn extends PropColumn
    {
      private BooleanPropColumn() {
        super(DataType.BOOLEAN);
      }

      @Override
      public Object read(byte[] data) throws IOException {
        return ((data[0] != 0) ? Boolean.TRUE : Boolean.FALSE);
      }

      @Override
      public ByteBuffer write(Object obj, int remainingRowLength)
        throws IOException
      {
        ByteBuffer buffer = PageChannel.createBuffer(1);
        buffer.put(((Number)booleanToInteger(obj)).byteValue());
        buffer.flip();
        return buffer;
      }
    }
  }

  /**
   * Utility interface for the object which owns the PropertyMaps
   */
  static interface Owner {

    /**
     * Invoked when new properties are saved.
     */
    public void propertiesUpdated() throws IOException;
  }
}
