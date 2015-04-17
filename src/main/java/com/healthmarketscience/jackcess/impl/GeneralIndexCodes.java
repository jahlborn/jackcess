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



/**
 * Various constants used for creating "general" (access 2010+) sort order
 * text index entries.
 *
 * @author James Ahlborn
 */
public class GeneralIndexCodes extends GeneralLegacyIndexCodes {

  // stash the codes in some resource files
  private static final String CODES_FILE = 
    DatabaseImpl.RESOURCE_PATH + "index_codes_gen.txt";
  private static final String EXT_CODES_FILE = 
    DatabaseImpl.RESOURCE_PATH + "index_codes_ext_gen.txt";

  private static final class Codes
  {
    /** handlers for the first 256 chars.  use nested class to lazy load the
        handlers */
    private static final CharHandler[] _values = loadCodes(
        CODES_FILE, FIRST_CHAR, LAST_CHAR);
  }
  
  private static final class ExtCodes
  {
    /** handlers for the rest of the chars in BMP 0.  use nested class to
        lazy load the handlers */
    private static final CharHandler[] _values = loadCodes(
        EXT_CODES_FILE, FIRST_EXT_CHAR, LAST_EXT_CHAR);
  }

  static final GeneralIndexCodes GEN_INSTANCE = new GeneralIndexCodes();

  GeneralIndexCodes() {
  }

  /**
   * Returns the CharHandler for the given character.
   */
  @Override
  CharHandler getCharHandler(char c)
  {
    if(c <= LAST_CHAR) {
      return Codes._values[c];
    }

    int extOffset = asUnsignedChar(c) - asUnsignedChar(FIRST_EXT_CHAR);
    return ExtCodes._values[extOffset];
  }

}
