/*
Copyright (c) 2008 Health Market Science, Inc.

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Various constants used for creating index entries.
 *
 * @author James Ahlborn
 */
public class IndexCodes {

  static final byte ASC_START_FLAG = (byte)0x7F;
  static final byte ASC_NULL_FLAG = (byte)0x00;
  static final byte DESC_START_FLAG = (byte)0x80;
  static final byte DESC_NULL_FLAG = (byte)0xFF;

  static final byte END_TEXT = (byte)0x01;
  static final byte END_EXTRA_TEXT = (byte)0x00;

  static final byte MID_GUID = (byte)0x09;
  static final byte ASC_END_GUID = (byte)0x08;
  static final byte DESC_END_GUID = (byte)0xF7;

  static final byte ASC_BOOLEAN_TRUE = (byte)0x00;
  static final byte ASC_BOOLEAN_FALSE = (byte)0xFF;
  
  static final byte DESC_BOOLEAN_TRUE = ASC_BOOLEAN_FALSE;
  static final byte DESC_BOOLEAN_FALSE = ASC_BOOLEAN_TRUE;


  // unprintable char is removed from normal text.
  // pattern for unprintable chars in the extra bytes:
  // 01 01 01 <pos> 06  <code> )
  // <pos> = 7 + (4 * char_pos) | 0x8000 (as short)
  // <code> = char code
  static final int UNPRINTABLE_COUNT_START = 7;
  static final int UNPRINTABLE_COUNT_MULTIPLIER = 4;
  static final int UNPRINTABLE_OFFSET_FLAGS = 0x8000;
  static final byte UNPRINTABLE_MIDFIX = (byte)0x06;

  // international char is replaced with ascii char.
  // pattern for international chars in the extra bytes:
  // [ 02 (for each normal char) ] [ <symbol_code> (for each inat char) ]
  static final byte INTERNATIONAL_EXTRA_PLACEHOLDER = (byte)0x02;  

  // see Index.writeCrazyCodes for details on writing crazy codes
  static final byte CRAZY_CODE_START = (byte)0x80;
  static final byte CRAZY_CODE_1 = (byte)0x02;
  static final byte CRAZY_CODE_2 = (byte)0x03;
  static final byte[] CRAZY_CODES_SUFFIX = 
    new byte[]{(byte)0xFF, (byte)0x02, (byte)0x80, (byte)0xFF, (byte)0x80};
  static final byte CRAZY_CODES_UNPRINT_SUFFIX = (byte)0xFF;

  // stash the codes in some resource files
  private static final String CODES_FILE = 
    "com/healthmarketscience/jackcess/index_codes.txt";
  private static final String EXT_CODES_FILE = 
    "com/healthmarketscience/jackcess/index_codes_ext.txt";

  /**
   * Enum which classifies the types of char encoding strategies used when
   * creating text index entries.
   */
  enum Type {
    SIMPLE("S") {
      @Override public CharHandler parseCodes(String[] codeStrings) {
        return parseSimpleCodes(codeStrings);
      }
    },
    INTERNATIONAL("I") {
      @Override public CharHandler parseCodes(String[] codeStrings) {
        return parseInternationalCodes(codeStrings);
      }
    },
    UNPRINTABLE("U") {
      @Override public CharHandler parseCodes(String[] codeStrings) {
        return parseUnprintableCodes(codeStrings);
      }
    },
    UNPRINTABLE_EXT("P") {
      @Override public CharHandler parseCodes(String[] codeStrings) {
        return parseUnprintableExtCodes(codeStrings);
      }
    },
    INTERNATIONAL_EXT("Z") {
      @Override public CharHandler parseCodes(String[] codeStrings) {
        return parseInternationalExtCodes(codeStrings);
      }
    },
    IGNORED("X") {
      @Override public CharHandler parseCodes(String[] codeStrings) {
        return IGNORED_CHAR_HANDLER;
      }
    };

    private final String _prefixCode;

    private Type(String prefixCode) {
      _prefixCode = prefixCode;
    }

    public String getPrefixCode() {
      return _prefixCode;
    }

    public abstract CharHandler parseCodes(String[] codeStrings);
  }

  /**
   * Base class for the handlers which hold thetext index character encoding
   * information.
   */
  abstract static class CharHandler {
    public abstract Type getType();
    public byte[] getInlineBytes() {
      return null;
    }
    public byte[] getExtraBytes() {
      return null;
    }
    public byte[] getUnprintableBytes() {
      return null;
    }
    public byte getExtraByteModifier() {
      return 0;
    }
    public byte getCrazyFlag() {
      return 0;
    }
  }

  /**
   * CharHandler for Type.SIMPLE
   */
  private static final class SimpleCharHandler extends CharHandler {
    private byte[] _bytes;
    private SimpleCharHandler(byte[] bytes) {
      _bytes = bytes;
    }
    @Override public Type getType() {
      return Type.SIMPLE;
    }
    @Override public byte[] getInlineBytes() {
      return _bytes;
    }
  }

  /**
   * CharHandler for Type.INTERNATIONAL
   */
  private static final class InternationalCharHandler extends CharHandler {
    private byte[] _bytes;
    private byte[] _extraBytes;
    private InternationalCharHandler(byte[] bytes, byte[] extraBytes) {
      _bytes = bytes;
      _extraBytes = extraBytes;
    }
    @Override public Type getType() {
      return Type.INTERNATIONAL;
    }
    @Override public byte[] getInlineBytes() {
      return _bytes;
    }
    @Override public byte[] getExtraBytes() {
      return _extraBytes;
    }
  }

  /**
   * CharHandler for Type.UNPRINTABLE
   */
  private static final class UnprintableCharHandler extends CharHandler {
    private byte[] _unprintBytes;
    private UnprintableCharHandler(byte[] unprintBytes) {
      _unprintBytes = unprintBytes;
    }
    @Override public Type getType() {
      return Type.UNPRINTABLE;
    }
    @Override public byte[] getUnprintableBytes() {
      return _unprintBytes;
    }
  }

  /**
   * CharHandler for Type.UNPRINTABLE_EXT
   */
  private static final class UnprintableExtCharHandler extends CharHandler {
    private byte _extraByteMod;
    private UnprintableExtCharHandler(Byte extraByteMod) {
      _extraByteMod = extraByteMod;
    }
    @Override public Type getType() {
      return Type.UNPRINTABLE_EXT;
    }
    @Override public byte getExtraByteModifier() {
      return _extraByteMod;
    }
  }

  /**
   * CharHandler for Type.INTERNATIONAL_EXT
   */
  private static final class InternationalExtCharHandler extends CharHandler {
    private byte[] _bytes;
    private byte[] _extraBytes;
    private byte _crazyFlag;
    private InternationalExtCharHandler(byte[] bytes, byte[] extraBytes,
                                        byte crazyFlag) {
      _bytes = bytes;
      _extraBytes = extraBytes;
      _crazyFlag = crazyFlag;
    }
    @Override public Type getType() {
      return Type.INTERNATIONAL_EXT;
    }
    @Override public byte[] getInlineBytes() {
      return _bytes;
    }
    @Override public byte[] getExtraBytes() {
      return _extraBytes;
    }
    @Override public byte getCrazyFlag() {
      return _crazyFlag;
    }
  }

  /** shared CharHandler instance for Type.IGNORED */
  static final CharHandler IGNORED_CHAR_HANDLER = new CharHandler() {
    @Override public Type getType() {
      return Type.IGNORED;
    }
  };

  /** alternate shared CharHandler instance for "surrogate" chars (which we do
      not handle) */
  static final CharHandler SURROGATE_CHAR_HANDLER = new CharHandler() {
    @Override public Type getType() {
      return Type.IGNORED;
    }
    @Override public byte[] getInlineBytes() {
      throw new IllegalStateException(
          "Surrogate pair chars are not handled");
    }
  };

  private static final char FIRST_CHAR = (char)0x0000;
  private static final char LAST_CHAR = (char)0x00FF;
  private static final char FIRST_EXT_CHAR = LAST_CHAR + 1;
  private static final char LAST_EXT_CHAR = (char)0xFFFF;

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
  
  private IndexCodes() {
  }

  /**
   * Returns the CharHandler for the given character.
   */
  static CharHandler getCharHandler(char c)
  {
    if(c <= LAST_CHAR) {
      return Codes._values[c];
    }

    int extOffset = asUnsignedChar(c) - asUnsignedChar(FIRST_EXT_CHAR);
    return ExtCodes._values[extOffset];
  }

  /**
   * Loads the CharHandlers for the given range of characters from the
   * resource file with the given name.
   */
  private static CharHandler[] loadCodes(String codesFilePath, 
                                         char firstChar, char lastChar)
  {
    int numCodes = (asUnsignedChar(lastChar) - asUnsignedChar(firstChar)) + 1;
    CharHandler[] values = new CharHandler[numCodes];

    Map<String,Type> prefixMap = new HashMap<String,Type>();
    for(Type type : Type.values()) {
      prefixMap.put(type.getPrefixCode(), type);
    }

    BufferedReader reader = null;
    try {

      reader = new BufferedReader(
          new InputStreamReader(
              Thread.currentThread().getContextClassLoader()
              .getResourceAsStream(codesFilePath), "US-ASCII"));
      
      int start = asUnsignedChar(firstChar);
      int end = asUnsignedChar(lastChar);
      for(int i = start; i <= end; ++i) {
        char c = (char)i;
        CharHandler ch = null;
        if(Character.isHighSurrogate(c) || Character.isLowSurrogate(c)) {
          // surrogate chars are not included in the codes files
          ch = SURROGATE_CHAR_HANDLER;
        } else {
          String codeLine = reader.readLine();
          ch = parseCodes(prefixMap, codeLine);
        }
        values[(i - start)] = ch;
      }

    } catch(IOException e) {
      throw new RuntimeException("failed loading index codes file " +
                                 codesFilePath, e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ex) {
          // ignored
        }
      }
    }

    return values;
  }

  /**
   * Returns a CharHandler parsed from the given line from an index codes
   * file.
   */
  private static CharHandler parseCodes(Map<String,Type> prefixMap,
                                        String codeLine)
  {
    String prefix = codeLine.substring(0, 1);
    String suffix = ((codeLine.length() > 1) ? codeLine.substring(2) : "");
    return prefixMap.get(prefix).parseCodes(suffix.split(",", -1));
  }

  /**
   * Returns a SimpleCharHandler parsed from the given index code strings.
   */
  private static CharHandler parseSimpleCodes(String[] codeStrings) 
  {
    if(codeStrings.length != 1) {
      throw new IllegalStateException("Unexpected code strings " +
                                      Arrays.asList(codeStrings));
    }
    return new SimpleCharHandler(codesToBytes(codeStrings[0], true));
  }

  /**
   * Returns an InternationalCharHandler parsed from the given index code
   * strings.
   */
  private static CharHandler parseInternationalCodes(String[] codeStrings)
  {
    if(codeStrings.length != 2) {
      throw new IllegalStateException("Unexpected code strings " +
                                      Arrays.asList(codeStrings));
    }
    return new InternationalCharHandler(codesToBytes(codeStrings[0], true),
                                        codesToBytes(codeStrings[1], true));
  }

  /**
   * Returns a UnprintableCharHandler parsed from the given index code
   * strings.
   */
  private static CharHandler parseUnprintableCodes(String[] codeStrings)
  {
    if(codeStrings.length != 1) {
      throw new IllegalStateException("Unexpected code strings " +
                                      Arrays.asList(codeStrings));
    }
    return new UnprintableCharHandler(codesToBytes(codeStrings[0], true));
  }

  /**
   * Returns a UnprintableExtCharHandler parsed from the given index code
   * strings.
   */
  private static CharHandler parseUnprintableExtCodes(String[] codeStrings) 
  {
    if(codeStrings.length != 1) {
      throw new IllegalStateException("Unexpected code strings " +
                                      Arrays.asList(codeStrings));
    }
    byte[] bytes = codesToBytes(codeStrings[0], true);
    if(bytes.length != 1) {
      throw new IllegalStateException("Unexpected code strings " +
                                      Arrays.asList(codeStrings));
    }
    return new UnprintableExtCharHandler(bytes[0]);
  }

  /**
   * Returns a InternationalExtCharHandler parsed from the given index code
   * strings.
   */
  private static CharHandler parseInternationalExtCodes(String[] codeStrings) 
  {
    if(codeStrings.length != 3) {
      throw new IllegalStateException("Unexpected code strings " +
                                      Arrays.asList(codeStrings));
    }

    byte crazyFlag = ("1".equals(codeStrings[2]) ?
                      CRAZY_CODE_1 : CRAZY_CODE_2);
    return new InternationalExtCharHandler(codesToBytes(codeStrings[0], true),
                                           codesToBytes(codeStrings[1], false),
                                           crazyFlag);
  }

  /**
   * Converts a string of hex encoded bytes to a byte[], optionally throwing
   * an exception if no codes are given.
   */
  private static byte[] codesToBytes(String codes, boolean required)
  {
    if(codes.length() == 0) {
      if(required) {
        throw new IllegalStateException("empty code bytes");
      }
      return null;
    }
    byte[] bytes = new byte[codes.length() / 2];
    for(int i = 0; i < bytes.length; ++i) {
      int charIdx = i*2;
      bytes[i] = (byte)(Integer.parseInt(codes.substring(charIdx, charIdx + 2),
                                         16));
    }
    return bytes;
  }

  /**
   * Returns an the char value converted to an unsigned char value.  Note, I
   * think this is unnecessary (I think java treats chars as unsigned), but I
   * did this just to be on the safe side.
   */
  private static int asUnsignedChar(char c)
  {
    return c & 0xFFFF;
  }

  static boolean isNullEntry(byte startEntryFlag) {
    return((startEntryFlag == ASC_NULL_FLAG) ||
           (startEntryFlag == DESC_NULL_FLAG));
  }
  
  static byte getNullEntryFlag(boolean isAscending) {
    return(isAscending ? ASC_NULL_FLAG : DESC_NULL_FLAG);
  }
  
  static byte getStartEntryFlag(boolean isAscending) {
    return(isAscending ? ASC_START_FLAG : DESC_START_FLAG);
  }
  
}
