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

  // we only have a small range of extended chars which can mapped back into
  // the valid chars
  private static final char FIRST_MAP_CHAR = 338;
  private static final char LAST_MAP_CHAR = 8482;

  private static final byte EXT_CODES_BOUNDS_NIBBLE = (byte)0x00;

  private static final class Codes
  {
    /** handlers for the first 256 chars.  use nested class to lazy load the
        handlers */
    private static final CharHandler[] _values = loadCodes(
        CODES_FILE, FIRST_CHAR, LAST_CHAR);
  }

  private static final class ExtMappings
  {
    /** mappings for a small subset of the rest of the chars in BMP 0.  use
        nested class to lazy load the handlers.  since these codes are for
        single byte encodings, you would think you wouldn't need any ext
        codes.  however, some chars in the extended range have corollaries in
        the single byte range. this array holds the mappings from the ext
        range to the single byte range.  chars without mappings go to 0
        (ignored). */
    private static final short[] _values = loadMappings(
        EXT_MAPPINGS_FILE, FIRST_MAP_CHAR, LAST_MAP_CHAR);
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

    if((c < FIRST_MAP_CHAR) || (c > LAST_MAP_CHAR)) {
      // outside the mapped range, ignored
      return IGNORED_CHAR_HANDLER;
    }

    // some ext chars are equivalent to single byte chars.  most chars have no
    // equivalent, and they map to 0 (which is an "ignored" char, so it all
    // works out)
    int extOffset = asUnsignedChar(c) - asUnsignedChar(FIRST_MAP_CHAR);
    return Codes._values[ExtMappings._values[extOffset]];
  }

  /**
   * Converts a 97 index value for a text column into the entry value (which
   * is based on a variety of nifty codes).
   */
  @Override
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
    NibbleStream extraCodes = null;
    int sigCharCount = 0;
    for(int i = 0; i < str.length(); ++i) {

      char c = str.charAt(i);
      CharHandler ch = getCharHandler(c);

      byte[] bytes = ch.getInlineBytes();
      if(bytes != null) {
        // write the "inline" codes immediately
        bout.write(bytes);
      }

      if(ch.getType() == Type.SIMPLE) {
        // common case, skip further code handling
        continue;
      }

      if(ch.isSignificantChar()) {
        ++sigCharCount;
        // significant chars never have extra bytes
        continue;
      }

      bytes = ch.getExtraBytes();
      if(bytes != null) {
        if(extraCodes == null) {
          extraCodes = new NibbleStream(str.length());
          extraCodes.writeNibble(EXT_CODES_BOUNDS_NIBBLE);
        }

        // keep track of the extra code for later
        writeExtraCodes(sigCharCount, bytes, extraCodes);
        sigCharCount = 0;
      }
    }

    if(extraCodes != null) {

      // write the extra codes to the end
      extraCodes.writeNibble(EXT_CODES_BOUNDS_NIBBLE);
      extraCodes.writeTo(bout);

    } else {

      // write end extra text
      bout.write(END_EXTRA_TEXT);
    }

    // handle descending order by inverting the bytes
    if(!isAscending) {

      // flip the bytes that we have written thus far for this text value
      IndexData.flipBytes(bout.getBytes(), prevLength,
                          (bout.getLength() - prevLength));
    }
  }

  private static void writeExtraCodes(int numSigChars, byte[] bytes,
                                      NibbleStream extraCodes)
  {
    // need to fill in placeholder nibbles for any "significant" chars
    if(numSigChars > 0) {
      extraCodes.writeFillNibbles(numSigChars, INTERNATIONAL_EXTRA_PLACEHOLDER);
    }

    // there should only ever be a single "extra" byte
    extraCodes.writeNibble(bytes[0]);
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

  /**
   * Extension of ByteStream which enables writing individual nibbles.
   */
  protected static final class NibbleStream extends ByteStream
  {
    private int _nibbleLen;

    protected NibbleStream(int length) {
      super(length);
    }

    private boolean nextIsHi() {
      return (_nibbleLen % 2) == 0;
    }

    private static int asLowNibble(int b) {
      return (b & 0x0F);
    }

    private static int asHiNibble(int b) {
      return ((b << 4) & 0xF0);
    }

    private void writeLowNibble(int b) {
      int byteOff = _nibbleLen / 2;
      setBits(byteOff, (byte)asLowNibble(b));
    }

    public void writeNibble(int b) {

      if(nextIsHi()) {
        write(asHiNibble(b));
      } else {
        writeLowNibble(b);
      }

      ++_nibbleLen;
    }

    public void writeFillNibbles(int length, byte b) {

      int newNibbleLen = _nibbleLen + length;
      ensureCapacity((newNibbleLen + 1) / 2);

      if(!nextIsHi()) {
        writeLowNibble(b);
        --length;
      }

      if(length > 1) {
        byte doubleB = (byte)(asHiNibble(b) | asLowNibble(b));

        do {
          write(doubleB);
          length -= 2;
        } while(length > 1);
      }

      if(length == 1) {
        write(asHiNibble(b));
      }

      _nibbleLen = newNibbleLen;
    }

  }

}
