/*
Copyright (c) 2011 James Ahlborn

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
*/

package com.healthmarketscience.jackcess;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Collection of PropertyMap instances read from a single property data block.
 *
 * @author James Ahlborn
 */
public class PropertyMaps implements Iterable<PropertyMap>
{
  /** the name of the "default" properties for a PropertyMaps instance */
  public static final String DEFAULT_NAME = "";

  private static final short PROPERTY_NAME_LIST = 0x80;
  private static final short DEFAULT_PROPERTY_VALUE_LIST = 0x00;
  private static final short COLUMN_PROPERTY_VALUE_LIST = 0x01;

  /** maps the PropertyMap name (case-insensitive) to the PropertyMap
      instance */
  private final Map<String,PropertyMap> _maps = 
    new LinkedHashMap<String,PropertyMap>();
  private final int _objectId;

  public PropertyMaps(int objectId) {
    _objectId = objectId;
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
  public PropertyMap getDefault() {
    return get(DEFAULT_NAME, DEFAULT_PROPERTY_VALUE_LIST);
  }

  /**
   * @return the PropertyMap with the given name in this group, creating if
   *         necessary
   */
  public PropertyMap get(String name) {
    return get(name, COLUMN_PROPERTY_VALUE_LIST);
  }

  /**
   * @return the PropertyMap with the given name and type in this group,
   *         creating if necessary
   */
  private PropertyMap get(String name, short type) {
    String lookupName = Database.toLookupName(name);
    PropertyMap map = _maps.get(lookupName);
    if(map == null) {
      map = new PropertyMap(name, type);
      _maps.put(lookupName, map);
    }
    return map;
  }

  /**
   * Adds the given PropertyMap to this group.
   */
  public void put(PropertyMap map) {
    _maps.put(Database.toLookupName(map.getName()), map);
  }

  public Iterator<PropertyMap> iterator() {
    return _maps.values().iterator();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for(Iterator<PropertyMap> iter = iterator(); iter.hasNext(); ) {
      sb.append(iter.next());
      if(iter.hasNext()) {
        sb.append("\n");
      }
    }
    return sb.toString();
  }

  /**
   * Utility class for reading/writing property blocks.
   */
  static final class Handler
  {
    /** the current database */
    private final Database _database;
    /** cache of PropColumns used to read/write property values */
    private final Map<DataType,PropColumn> _columns = 
      new HashMap<DataType,PropColumn>();

    Handler(Database database) {
      _database = database;
    }

    /**
     * @return a PropertyMaps instance decoded from the given bytes (always
     *         returns non-{@code null} result).
     */
    public PropertyMaps read(byte[] propBytes, int objectId) 
      throws IOException 
    {

      PropertyMaps maps = new PropertyMaps(objectId);
      if((propBytes == null) || (propBytes.length == 0)) {
        return maps;
      }

      ByteBuffer bb = ByteBuffer.wrap(propBytes)
        .order(PageChannel.DEFAULT_BYTE_ORDER);

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
        } else if((type == DEFAULT_PROPERTY_VALUE_LIST) ||
                  (type == COLUMN_PROPERTY_VALUE_LIST)) {
          maps.put(readPropertyValues(bbBlock, propNames, type));
        } else {
          throw new IOException("Unknown property block type " + type);
        }

        bb.position(endPos);
      }

      return maps;
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

    /**
     * @return the PropertyMap created from the values parsed from the given
     *         data chunk combined with the given property names
     */
    private PropertyMap readPropertyValues(
        ByteBuffer bbBlock, List<String> propNames, short blockType) 
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
      
      PropertyMap map = new PropertyMap(mapName, blockType);

      // read the values
      while(bbBlock.hasRemaining()) {

        int valLen = bbBlock.getShort();
        int endPos = bbBlock.position() + valLen - 2;
        byte flag = bbBlock.get();
        DataType dataType = DataType.fromByte(bbBlock.get());
        int nameIdx = bbBlock.getShort();
        int dataSize = bbBlock.getShort();

        String propName = propNames.get(nameIdx);
        PropColumn col = getColumn(dataType, propName, dataSize);

        byte[] data = ByteUtil.getBytes(bbBlock, dataSize);
        Object value = col.read(data);

        map.put(propName, dataType, flag, value);

        bbBlock.position(endPos);
      }

      return map;
    }

    /**
     * Reads a property name from the given data block
     */
    private String readPropName(ByteBuffer buffer) { 
      int nameLength = buffer.getShort();
      byte[] nameBytes = ByteUtil.getBytes(buffer, nameLength);
      return Column.decodeUncompressedText(nameBytes, _database.getCharset());
    }

    /**
     * Gets a PropColumn capable of reading/writing a property of the given
     * DataType
     */
    private PropColumn getColumn(DataType dataType, String propName, 
                                 int dataSize) {

      if(isPseudoGuidColumn(dataType, propName, dataSize)) {
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
               new BooleanPropColumn() : new PropColumn());
        col.setType(colType);
        if(col.isVariableLength()) {
          col.setLength((short)colType.getMaxSize());
        }
      }

      return col;
    }

    private boolean isPseudoGuidColumn(DataType dataType, String propName, 
                                       int dataSize) {
      // guids seem to be marked as "binary" fields
      return((dataType == DataType.BINARY) && 
             (dataSize == DataType.GUID.getFixedSize()) &&
             PropertyMap.GUID_PROP.equalsIgnoreCase(propName));
    }

    /**
     * Column adapted to work w/out a Table.
     */
    private class PropColumn extends Column
    {
      @Override
      public Database getDatabase() {
        return _database;
      }
    }

    /**
     * Normal boolean columns do not write into the actual row data, so we
     * need to do a little extra work.
     */
    private final class BooleanPropColumn extends PropColumn
    {
      @Override
      public Object read(byte[] data) throws IOException {
        return ((data[0] != 0) ? Boolean.TRUE : Boolean.FALSE);
      }

      @Override
      public ByteBuffer write(Object obj, int remainingRowLength)
        throws IOException
      {
        ByteBuffer buffer = getPageChannel().createBuffer(1);
        buffer.put(((Number)booleanToInteger(obj)).byteValue());
        buffer.flip();
        return buffer;
      }
    }
  }
}
