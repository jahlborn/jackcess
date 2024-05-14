/*
Copyright (c) 2008 Health Market Science, Inc.

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.healthmarketscience.jackcess.impl.ByteUtil.ByteStream;

/**
 * Various constants used for creating "general legacy" (access 2000-2007)
 * sort order text index entries.
 *
 * @author James Ahlborn
 */
public class GeneralLegacyIndexCodes {

  static final int MAX_TEXT_INDEX_CHAR_LENGTH =
    (JetFormat.TEXT_FIELD_MAX_LENGTH / JetFormat.TEXT_FIELD_UNIT_SIZE);

  static final byte END_TEXT = (byte)0x01;
  static final byte END_EXTRA_TEXT = (byte)0x00;

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
    DatabaseImpl.RESOURCE_PATH + "index_codes_genleg.txt";
  private static final String EXT_CODES_FILE =
    DatabaseImpl.RESOURCE_PATH + "index_codes_ext_genleg.txt";

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
    SIGNIFICANT("G") {
      @Override public CharHandler parseCodes(String[] codeStrings) {
        return parseSignificantCodes(codeStrings);
      }
    },
    SURROGATE("Q") {
      @Override public CharHandler parseCodes(String[] codeStrings) {
        // these are not parsed from the codes files
        throw new UnsupportedOperationException();
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
   * Base class for the handlers which hold the text index character encoding
   * information.
   */
  abstract static class CharHandler {
    public abstract Type getType();
    public byte[] getInlineBytes(char c) {
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
    public boolean isSignificantChar() {
      return false;
    }
  }

  /**
   * CharHandler for Type.SIMPLE
   */
  private static final class SimpleCharHandler extends CharHandler {
    private final byte[] _bytes;
    private SimpleCharHandler(byte[] bytes) {
      _bytes = bytes;
    }
    @Override public Type getType() {
      return Type.SIMPLE;
    }
    @Override public byte[] getInlineBytes(char c) {
      return _bytes;
    }
  }

  /**
   * CharHandler for Type.INTERNATIONAL
   */
  private static final class InternationalCharHandler extends CharHandler {
    private final byte[] _bytes;
    private final byte[] _extraBytes;
    private InternationalCharHandler(byte[] bytes, byte[] extraBytes) {
      _bytes = bytes;
      _extraBytes = extraBytes;
    }
    @Override public Type getType() {
      return Type.INTERNATIONAL;
    }
    @Override public byte[] getInlineBytes(char c) {
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
    private final byte[] _unprintBytes;
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
    private final byte _extraByteMod;
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
    private final byte[] _bytes;
    private final byte[] _extraBytes;
    private final byte _crazyFlag;
    private InternationalExtCharHandler(byte[] bytes, byte[] extraBytes,
                                        byte crazyFlag) {
      _bytes = bytes;
      _extraBytes = extraBytes;
      _crazyFlag = crazyFlag;
    }
    @Override public Type getType() {
      return Type.INTERNATIONAL_EXT;
    }
    @Override public byte[] getInlineBytes(char c) {
      return _bytes;
    }
    @Override public byte[] getExtraBytes() {
      return _extraBytes;
    }
    @Override public byte getCrazyFlag() {
      return _crazyFlag;
    }
  }

  /**
   * CharHandler for Type.SIGNIFICANT
   */
  private static final class SignificantCharHandler extends CharHandler {
    private final byte[] _bytes;
    private SignificantCharHandler(byte[] bytes) {
      _bytes = bytes;
    }
    @Override public Type getType() {
      return Type.SIGNIFICANT;
    }
    @Override public byte[] getInlineBytes(char c) {
      return _bytes;
    }
    @Override public boolean isSignificantChar() {
      return true;
    }
  }

  /** shared CharHandler instance for Type.IGNORED */
  static final CharHandler IGNORED_CHAR_HANDLER = new CharHandler() {
    @Override public Type getType() {
      return Type.IGNORED;
    }
  };

  /** the surrogate char bufs are computed on the fly.  re-use a buffer for
      those */
  private static final ThreadLocal<byte[]> SURROGATE_CHAR_BUF =
    ThreadLocal.withInitial(() -> new byte[2]);
  private static final byte[] SURROGATE_EXTRA_BYTES = {0x3f};

  private static abstract class SurrogateCharHandler extends CharHandler {
    @Override public Type getType() {
      return Type.SURROGATE;
    }
    @Override public byte[] getExtraBytes() {
      return SURROGATE_EXTRA_BYTES;
    }
    protected static byte[] toInlineBytes(int idxC) {
      byte[] bytes = SURROGATE_CHAR_BUF.get();
      bytes[0] = (byte)((idxC >>> 8) & 0xFF);
      bytes[1] = (byte)(idxC & 0xFF);
      return bytes;
    }
  }

  /** shared CharHandler instance for "high surrogate" chars (which are
      computed) */
  static final CharHandler HIGH_SURROGATE_CHAR_HANDLER =
    new SurrogateCharHandler() {
      @Override public byte[] getInlineBytes(char c) {
        // the high sorrogate bytes seems to be computed from a fixed offset
        int idxC = asUnsignedChar(c) - 10238;
        return toInlineBytes(idxC);
      }
    };

  /** shared CharHandler instance for "low surrogate" chars (which are
      computed) */
  static final CharHandler LOW_SURROGATE_CHAR_HANDLER =
    new SurrogateCharHandler() {
      @Override public byte[] getInlineBytes(char c) {
        // the low surrogate bytes are computed with a specific value based in
        // its location in a 1024 character block.
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
        int idxC = asUnsignedChar(c) - idxOffset;
        return toInlineBytes(idxC);
      }
    };

  static final char FIRST_CHAR = (char)0x0000;
  static final char LAST_CHAR = (char)0x00FF;
  static final char FIRST_EXT_CHAR = LAST_CHAR + 1;
  static final char LAST_EXT_CHAR = (char)0xFFFF;

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

  static final GeneralLegacyIndexCodes GEN_LEG_INSTANCE =
    new GeneralLegacyIndexCodes();

  GeneralLegacyIndexCodes() {
  }

  /**
   * Returns the CharHandler for the given character.
   */
  CharHandler getCharHandler(char c)
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
  static CharHandler[] loadCodes(String codesFilePath,
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
              DatabaseImpl.getResourceAsStream(codesFilePath), "US-ASCII"));

      int start = asUnsignedChar(firstChar);
      int end = asUnsignedChar(lastChar);
      for(int i = start; i <= end; ++i) {
        char c = (char)i;
        CharHandler ch = null;
        if(Character.isHighSurrogate(c)) {
          // surrogate chars are not included in the codes files
          ch = HIGH_SURROGATE_CHAR_HANDLER;
        } else if(Character.isLowSurrogate(c)) {
          // surrogate chars are not included in the codes files
          ch = LOW_SURROGATE_CHAR_HANDLER;
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
      ByteUtil.closeQuietly(reader);
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
    String suffix = ((codeLine.length() > 1) ? codeLine.substring(1) : "");
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
   * Returns a SignificantCharHandler parsed from the given index code strings.
   */
  private static CharHandler parseSignificantCodes(String[] codeStrings)
  {
    if(codeStrings.length != 1) {
      throw new IllegalStateException("Unexpected code strings " +
                                      Arrays.asList(codeStrings));
    }
    return new SignificantCharHandler(codesToBytes(codeStrings[0], true));
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
    if((codes.length() % 2) != 0) {
      // stripped a leading 0
      codes = "0" + codes;
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
  static int asUnsignedChar(char c)
  {
    return c & 0xFFFF;
  }

  /**
   * Converts an index value for a text column into the entry value (which
   * is based on a variety of nifty codes).
   */
  void writeNonNullIndexTextValue(
      Object value, ByteStream bout, boolean isAscending)
    throws IOException
  {
    // convert to string
    String str = toIndexCharSequence(value);

    // record previous entry length so we can do any post-processing
    // necessary for this entry (handling descending)
    int prevLength = bout.getLength();

    // now, convert each character to a "code" of one or more bytes
    ExtraCodesStream extraCodes = null;
    ByteStream unprintableCodes = null;
    ByteStream crazyCodes = null;
    int charOffset = 0;
    for(int i = 0; i < str.length(); ++i) {

      char c = str.charAt(i);
      CharHandler ch = getCharHandler(c);

      int curCharOffset = charOffset;
      byte[] bytes = ch.getInlineBytes(c);
      if(bytes != null) {
        // write the "inline" codes immediately
        bout.write(bytes);

        // only increment the charOffset for chars with inline codes
        ++charOffset;
      }

      if(ch.getType() == Type.SIMPLE) {
        // common case, skip further code handling
        continue;
      }

      bytes = ch.getExtraBytes();
      byte extraCodeModifier = ch.getExtraByteModifier();
      if((bytes != null) || (extraCodeModifier != 0)) {
        if(extraCodes == null) {
          extraCodes = new ExtraCodesStream(str.length());
        }

        // keep track of the extra codes for later
        writeExtraCodes(curCharOffset, bytes, extraCodeModifier, extraCodes);
      }

      bytes = ch.getUnprintableBytes();
      if(bytes != null) {
        if(unprintableCodes == null) {
          unprintableCodes = new ByteStream();
        }

        // keep track of the unprintable codes for later
        writeUnprintableCodes(curCharOffset, bytes, unprintableCodes,
                              extraCodes);
      }

      byte crazyFlag = ch.getCrazyFlag();
      if(crazyFlag != 0) {
        if(crazyCodes == null) {
          crazyCodes = new ByteStream();
        }

        // keep track of the crazy flags for later
        crazyCodes.write(crazyFlag);
      }
    }

    // write end text flag
    bout.write(END_TEXT);

    boolean hasExtraCodes = trimExtraCodes(
        extraCodes, (byte)0, INTERNATIONAL_EXTRA_PLACEHOLDER);
    boolean hasUnprintableCodes = (unprintableCodes != null);
    boolean hasCrazyCodes = (crazyCodes != null);
    if(hasExtraCodes || hasUnprintableCodes || hasCrazyCodes) {

      // we write all the international extra bytes first
      if(hasExtraCodes) {
        extraCodes.writeTo(bout);
      }

      if(hasCrazyCodes || hasUnprintableCodes) {

        // write 2 more end flags
        bout.write(END_TEXT);
        bout.write(END_TEXT);

        // next come the crazy flags
        if(hasCrazyCodes) {

          writeCrazyCodes(crazyCodes, bout);

          // if we are writing unprintable codes after this, tack on another
          // code
          if(hasUnprintableCodes) {
            bout.write(CRAZY_CODES_UNPRINT_SUFFIX);
          }
        }

        // then we write all the unprintable extra bytes
        if(hasUnprintableCodes) {

          // write another end flag
          bout.write(END_TEXT);

          unprintableCodes.writeTo(bout);
        }
      }
    }

    // handle descending order by inverting the bytes
    if(!isAscending) {

      // we actually write the end byte before flipping the bytes, and write
      // another one after flipping
      bout.write(END_EXTRA_TEXT);

      // flip the bytes that we have written thus far for this text value
      IndexData.flipBytes(bout.getBytes(), prevLength,
                          (bout.getLength() - prevLength));
    }

    // write end extra text
    bout.write(END_EXTRA_TEXT);
  }

  protected static String toIndexCharSequence(Object value)
      throws IOException {

    // first, convert to string
    String str = ColumnImpl.toCharSequence(value).toString();

    // all text columns (including memos) are only indexed up to the max
    // number of chars in a VARCHAR column
    int len = str.length();
    if(len > MAX_TEXT_INDEX_CHAR_LENGTH) {
      str = str.substring(0, MAX_TEXT_INDEX_CHAR_LENGTH);
      len = MAX_TEXT_INDEX_CHAR_LENGTH;
    }

    // trailing spaces are ignored for text index entries
    if((len > 0) && (str.charAt(len - 1) == ' ')) {
      do {
        --len;
      } while((len > 0) && (str.charAt(len - 1) == ' '));

      str = str.substring(0, len);
    }

    return str;
  }

  /**
   * Encodes the given extra code info in the given stream.
   */
  private static void writeExtraCodes(
      int charOffset, byte[] bytes, byte extraCodeModifier,
      ExtraCodesStream extraCodes)
  {
    // we fill in a placeholder value for any chars w/out extra codes
    int numChars = extraCodes.getNumChars();
    if(numChars < charOffset) {
      int fillChars = charOffset - numChars;
      extraCodes.writeFill(fillChars, INTERNATIONAL_EXTRA_PLACEHOLDER);
      extraCodes.incrementNumChars(fillChars);
    }

    if(bytes != null) {

      // write the actual extra codes and update the number of chars
      extraCodes.write(bytes);
      extraCodes.incrementNumChars(1);

    } else {

      // extra code modifiers modify the existing extra code bytes and do not
      // count as additional extra code chars
      int lastIdx = extraCodes.getLength() - 1;
      if(lastIdx >= 0) {

        // the extra code modifier is added to the last extra code written
        byte lastByte = extraCodes.get(lastIdx);
        lastByte += extraCodeModifier;
        extraCodes.set(lastIdx, lastByte);

      } else {

        // there is no previous extra code, add a new code (but keep track of
        // this "unprintable code" prefix)
        extraCodes.write(extraCodeModifier);
        extraCodes.setUnprintablePrefixLen(1);
      }
    }
  }

  /**
   * Trims any bytes in the given range off of the end of the given stream,
   * returning whether or not there are any bytes left in the given stream
   * after trimming.
   */
  private static boolean trimExtraCodes(ByteStream extraCodes,
                                        byte minTrimCode, byte maxTrimCode)
  {
    if(extraCodes == null) {
      return false;
    }

    extraCodes.trimTrailing(minTrimCode, maxTrimCode);

    // anything left?
    return (extraCodes.getLength() > 0);
  }

  /**
   * Encodes the given unprintable char codes in the given stream.
   */
  private static void writeUnprintableCodes(
      int charOffset, byte[] bytes, ByteStream unprintableCodes,
      ExtraCodesStream extraCodes)
  {
    // the offset seems to be calculated based on the number of bytes in the
    // "extra codes" part of the entry (even if there are no extra codes bytes
    // actually written in the final entry).
    int unprintCharOffset = charOffset;
    if(extraCodes != null) {
      // we need to account for some extra codes which have not been written
      // yet.  additionally, any unprintable bytes added to the beginning of
      // the extra codes are ignored.
      unprintCharOffset = extraCodes.getLength() +
        (charOffset - extraCodes.getNumChars()) -
        extraCodes.getUnprintablePrefixLen();
    }

    // we write a whacky combo of bytes for each unprintable char which
    // includes a funky offset and extra char itself
    int offset =
      (UNPRINTABLE_COUNT_START +
       (UNPRINTABLE_COUNT_MULTIPLIER * unprintCharOffset))
      | UNPRINTABLE_OFFSET_FLAGS;

    // write offset as big-endian short
    unprintableCodes.write((offset >> 8) & 0xFF);
    unprintableCodes.write(offset & 0xFF);

    unprintableCodes.write(UNPRINTABLE_MIDFIX);
    unprintableCodes.write(bytes);
  }

  /**
   * Encode the given crazy code bytes into the given byte stream.
   */
  private static void writeCrazyCodes(ByteStream crazyCodes, ByteStream bout)
  {
    // CRAZY_CODE_2 flags at the end are ignored, so ditch them
    trimExtraCodes(crazyCodes, CRAZY_CODE_2, CRAZY_CODE_2);

    if(crazyCodes.getLength() > 0) {

      // the crazy codes get encoded into 6 bit sequences where each code is 2
      // bits (where the first 2 bits in the byte are a common prefix).
      byte curByte = CRAZY_CODE_START;
      int idx = 0;
      for(int i = 0; i < crazyCodes.getLength(); ++i) {
        byte nextByte = crazyCodes.get(i);
        nextByte <<= ((2 - idx) * 2);
        curByte |= nextByte;

        ++idx;
        if(idx == 3) {
          // write current byte and reset
          bout.write(curByte);
          curByte = CRAZY_CODE_START;
          idx = 0;
        }
      }

      // write last byte
      if(idx > 0) {
        bout.write(curByte);
      }
    }

    // write crazy code suffix (note, we write this even if all the codes are
    // trimmed
    bout.write(CRAZY_CODES_SUFFIX);
  }

  /**
   * Extension of ByteStream which keeps track of an additional char count and
   * the length of any "unprintable" code prefix.
   */
  private static final class ExtraCodesStream extends ByteStream
  {
    private int _numChars;
    private int _unprintablePrefixLen;

    private ExtraCodesStream(int length) {
      super(length);
    }

    public int getNumChars() {
      return _numChars;
    }

    public void incrementNumChars(int inc) {
      _numChars += inc;
    }

    public int getUnprintablePrefixLen() {
      return _unprintablePrefixLen;
    }

    public void setUnprintablePrefixLen(int len) {
      _unprintablePrefixLen = len;
    }
  }

}
