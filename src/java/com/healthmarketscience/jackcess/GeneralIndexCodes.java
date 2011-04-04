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



/**
 * Various constants used for creating "general" (access 2010+) sort order
 * text index entries.
 *
 * @author James Ahlborn
 */
public class GeneralIndexCodes extends GeneralLegacyIndexCodes {

  // stash the codes in some resource files
  private static final String CODES_FILE = 
    Database.RESOURCE_PATH + "index_codes_gen.txt";
  private static final String EXT_CODES_FILE = 
    Database.RESOURCE_PATH + "index_codes_ext_gen.txt";

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
