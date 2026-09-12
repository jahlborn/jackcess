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

  /* the weight table gives the high surrogates four runs, and gives the 768
     of them from U+D880 to U+DB7F no weight at all */
  private static final char HIGH_SURROGATE_PLANE2_START = (char)0xD840;
  private static final char HIGH_SURROGATE_UNWEIGHTED_START = (char)0xD880;
  private static final char HIGH_SURROGATE_HIGH_PLANES_START = (char)0xDB80;
  private static final int HIGH_SURROGATE_PLANE1_OFFSET = -10238;
  private static final int HIGH_SURROGATE_PLANE2_OFFSET = 9666;
  private static final int HIGH_SURROGATE_HIGH_PLANES_OFFSET = 9090;
  private static final byte SURROGATE_EXTRA_BYTE = (byte)0x3f;
  private static final byte HIGH_SURROGATE_EXTRA_BYTE = (byte)0x3e;

  /** handler for a high surrogate in the first run, U+D800 to U+D83F */
  private static final CharHandler HIGH_SURROGATE_PLANE1_CHAR_HANDLER =
    new SurrogateCharHandler(SURROGATE_EXTRA_BYTE) {
      @Override public byte[] getInlineBytes(char c) {
        return toInlineBytes(asUnsignedChar(c) + HIGH_SURROGATE_PLANE1_OFFSET);
      }
    };

  /** handler for a high surrogate in the second run, U+D840 to U+D87F */
  private static final CharHandler HIGH_SURROGATE_PLANE2_CHAR_HANDLER =
    new SurrogateCharHandler(HIGH_SURROGATE_EXTRA_BYTE) {
      @Override public byte[] getInlineBytes(char c) {
        return toInlineBytes(asUnsignedChar(c) + HIGH_SURROGATE_PLANE2_OFFSET);
      }
    };

  /** handler for a high surrogate in the fourth run, U+DB80 to U+DBFF */
  private static final CharHandler HIGH_SURROGATE_HIGH_PLANES_CHAR_HANDLER =
    new SurrogateCharHandler(HIGH_SURROGATE_EXTRA_BYTE) {
      @Override public byte[] getInlineBytes(char c) {
        return toInlineBytes(
            asUnsignedChar(c) + HIGH_SURROGATE_HIGH_PLANES_OFFSET);
      }
    };

  /** handler for a low surrogate, whose weight is piecewise over its position
      in the 1024 char block */
  private static final CharHandler LOW_SURROGATE_CHAR_HANDLER =
    new SurrogateCharHandler(SURROGATE_EXTRA_BYTE) {
      @Override public byte[] getInlineBytes(char c) {
        int charOffset = (asUnsignedChar(c) - 0xdc00) % 1024;

        int idxOffset = 0;
        if(charOffset < 8) {
          idxOffset = 9992;
        } else if(charOffset < (8 + 254)) {
          idxOffset = 9990;
        } else if(charOffset < (8 + 254 + 254)) {
          idxOffset = 9988;
        } else if(charOffset < (8 + 254 + 254 + 254)) {
          idxOffset = 9986;
        } else  {
          idxOffset = 9984;
        }
        return toInlineBytes(asUnsignedChar(c) - idxOffset);
      }
    };

  /**
   * @return the handler for the given surrogate char.  The third run of the
   *         high surrogates has no weight in the table, so those are ignored.
   */
  private static CharHandler getSurrogateCharHandler(char c) {
    if(Character.isLowSurrogate(c)) {
      return LOW_SURROGATE_CHAR_HANDLER;
    }
    if(c < HIGH_SURROGATE_PLANE2_START) {
      return HIGH_SURROGATE_PLANE1_CHAR_HANDLER;
    }
    if(c < HIGH_SURROGATE_UNWEIGHTED_START) {
      return HIGH_SURROGATE_PLANE2_CHAR_HANDLER;
    }
    if(c < HIGH_SURROGATE_HIGH_PLANES_START) {
      return IGNORED_CHAR_HANDLER;
    }
    return HIGH_SURROGATE_HIGH_PLANES_CHAR_HANDLER;
  }

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
        EXT_CODES_FILE, FIRST_EXT_CHAR, LAST_EXT_CHAR,
        GeneralIndexCodes::getSurrogateCharHandler);
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
