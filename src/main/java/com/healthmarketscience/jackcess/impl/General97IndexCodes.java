/*
Copyright (c) 2019 James Ahlborn

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static com.healthmarketscience.jackcess.impl.ByteUtil.ByteStream;

/**
 * Various constants used for creating "general" (access 1997) sort order
 * text index entries.
 *
 * @author James Ahlborn
 */
public class General97IndexCodes extends GeneralLegacyIndexCodes
{
  // stash the codes in some resource files
  private static final String CODES_FILE =
    DatabaseImpl.RESOURCE_PATH + "index_codes_gen_97.txt";
  private static final String EXT_MAPPINGS_FILE =
    DatabaseImpl.RESOURCE_PATH + "index_mappings_ext_gen_97.txt";

  private static final class Codes
  {
    /** handlers for the first 256 chars.  use nested class to lazy load the
        handlers */
    private static final CharHandler[] _values = loadCodes(
        CODES_FILE, FIRST_CHAR, LAST_CHAR);
  }

  private static final class ExtMappings
  {
    /** mappings for the rest of the chars in BMP 0.  use nested class to lazy
        load the handlers.  since these codes are for single byte encodings,
        you would think you wou;dn't need any ext codes.  however, some chars
        in the extended range have corollaries in the single byte range. this
        array holds the mappings from the ext range to the single byte range.
        chars without mappings go to 0. */
    private static final short[] _values = loadMappings(
        EXT_MAPPINGS_FILE, FIRST_EXT_CHAR, LAST_EXT_CHAR);
  }

  static final General97IndexCodes GEN_97_INSTANCE = new General97IndexCodes();

  General97IndexCodes() {}

  /**
   * Returns the CharHandler for the given character.
   */
  @Override
  CharHandler getCharHandler(char c)
  {
    if(c <= LAST_CHAR) {
      return Codes._values[c];
    }

    // some ext chars are equivalent to single byte chars.  most chars have no
    // equivalent, and they map to 0 (which is an "ignored" char, so it all
    // works out)
    int extOffset = asUnsignedChar(c) - asUnsignedChar(FIRST_EXT_CHAR);
    return Codes._values[ExtMappings._values[extOffset]];
  }

  @Override
  void writeNonNullIndexTextValue(
      Object value, ByteStream bout, boolean isAscending)
    throws IOException
  {
    // use simplified format for 97 encoding
    writeNonNull97IndexTextValue(value, bout, isAscending);
  }

  static short[] loadMappings(String mappingsFilePath,
                              char firstChar, char lastChar)
  {
    int firstCharCode = asUnsignedChar(firstChar);
    int numMappings = (asUnsignedChar(lastChar) - firstCharCode) + 1;
    short[] values = new short[numMappings];

    BufferedReader reader = null;
    try {

      reader = new BufferedReader(
          new InputStreamReader(
              DatabaseImpl.getResourceAsStream(mappingsFilePath), "US-ASCII"));

      // this is a sparse file with entries like <fromCode>,<toCode>
      String mappingLine = null;
      while((mappingLine = reader.readLine()) != null) {
        mappingLine = mappingLine.trim();
        if(mappingLine.length() == 0) {
          continue;
        }

        String[] mappings = mappingLine.split(",");
        int fromCode = Integer.parseInt(mappings[0]);
        int toCode = Integer.parseInt(mappings[1]);

        values[fromCode - firstCharCode] = (short)toCode;
      }

    } catch(IOException e) {
      throw new RuntimeException("failed loading index mappings file " +
                                 mappingsFilePath, e);
    } finally {
      ByteUtil.closeQuietly(reader);
    }

    return values;
  }
}
