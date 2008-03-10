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
  static final byte[] UNPRINTABLE_COMMON_PREFIX =
    new byte[]{(byte)0x01, (byte)0x01, (byte)0x01};
  static final int UNPRINTABLE_OFFSET_FLAGS = 0x8000;
  static final byte UNPRINTABLE_MIDFIX = (byte)0x06;

  // international char is replaced with ascii char.
  // pattern for international chars in the extra bytes:
  // [ 02 (for each normal char) ] [ <symbol_code> (for each inat char) ]
  static final byte INTERNATIONAL_EXTRA_PLACEHOLDER = (byte)0x02;
  
  /**
   * Map of character to byte[] that Access uses in indexes (not ASCII)
   * (Character -> byte[]) as codes to order text
   */
  static final Map<Character, byte[]> CODES =
    new HashMap<Character, byte[]>(150);

  /**
   * Map of character to byte[] that Access uses in indexes for unprintable
   * characters (not ASCII) (Character -> byte[]), in the extended portion
   */
  static final Map<Character, byte[]> UNPRINTABLE_CODES =
    new HashMap<Character, byte[]>(100);
  
  /**
   * Map of character to byte[] that Access uses in indexes for international
   * characters (not ASCII) (Character -> InternationalCodes), in the extended
   * portion
   */
  static final Map<Character, InternationalCodes> INTERNATIONAL_CODES =
    new HashMap<Character, InternationalCodes>(70);
  
  static {
    
    registerCodes('\u0000', new byte[]{});
    registerCodes('\t',     new byte[]{(byte)0x08, (byte)0x03});
    registerCodes('\n',     new byte[]{(byte)0x08, (byte)0x04});
    registerCodes('\u000B', new byte[]{(byte)0x08, (byte)0x05});
    registerCodes('\f',     new byte[]{(byte)0x08, (byte)0x06});
    registerCodes('\r',     new byte[]{(byte)0x08, (byte)0x07});
    registerCodes('\u0020', new byte[]{(byte)0x07});
    registerCodes('\u0021', new byte[]{(byte)0x09});
    registerCodes('\"',     new byte[]{(byte)0x0A});
    registerCodes('\u0023', new byte[]{(byte)0x0C});
    registerCodes('\u0024', new byte[]{(byte)0x0E});
    registerCodes('\u0025', new byte[]{(byte)0x10});
    registerCodes('\u0026', new byte[]{(byte)0x12});
    registerCodes('\u0028', new byte[]{(byte)0x14});
    registerCodes('\u0029', new byte[]{(byte)0x16});
    registerCodes('\u002A', new byte[]{(byte)0x18});
    registerCodes('\u002B', new byte[]{(byte)0x2C});
    registerCodes('\u002C', new byte[]{(byte)0x1A});
    registerCodes('\u002E', new byte[]{(byte)0x1C});
    registerCodes('\u002F', new byte[]{(byte)0x1E});
    registerCodes('\u0030', new byte[]{(byte)0x36});
    registerCodes('\u0031', new byte[]{(byte)0x38});
    registerCodes('\u0032', new byte[]{(byte)0x3A});
    registerCodes('\u0033', new byte[]{(byte)0x3C});
    registerCodes('\u0034', new byte[]{(byte)0x3E});
    registerCodes('\u0035', new byte[]{(byte)0x40});
    registerCodes('\u0036', new byte[]{(byte)0x42});
    registerCodes('\u0037', new byte[]{(byte)0x44});
    registerCodes('\u0038', new byte[]{(byte)0x46});
    registerCodes('\u0039', new byte[]{(byte)0x48});
    registerCodes('\u003A', new byte[]{(byte)0x20});
    registerCodes('\u003B', new byte[]{(byte)0x22});
    registerCodes('\u003C', new byte[]{(byte)0x2E});
    registerCodes('\u003D', new byte[]{(byte)0x30});
    registerCodes('\u003E', new byte[]{(byte)0x32});
    registerCodes('\u003F', new byte[]{(byte)0x24});
    registerCodes('\u0040', new byte[]{(byte)0x26});
    registerCodes('\u0041', new byte[]{(byte)0x4A});
    registerCodes('\u0042', new byte[]{(byte)0x4C});
    registerCodes('\u0043', new byte[]{(byte)0x4D});
    registerCodes('\u0044', new byte[]{(byte)0x4F});
    registerCodes('\u0045', new byte[]{(byte)0x51});
    registerCodes('\u0046', new byte[]{(byte)0x53});
    registerCodes('\u0047', new byte[]{(byte)0x55});
    registerCodes('\u0048', new byte[]{(byte)0x57});
    registerCodes('\u0049', new byte[]{(byte)0x59});
    registerCodes('\u004A', new byte[]{(byte)0x5B});
    registerCodes('\u004B', new byte[]{(byte)0x5C});
    registerCodes('\u004C', new byte[]{(byte)0x5E});
    registerCodes('\u004D', new byte[]{(byte)0x60});
    registerCodes('\u004E', new byte[]{(byte)0x62});
    registerCodes('\u004F', new byte[]{(byte)0x64});
    registerCodes('\u0050', new byte[]{(byte)0x66});
    registerCodes('\u0051', new byte[]{(byte)0x68});
    registerCodes('\u0052', new byte[]{(byte)0x69});
    registerCodes('\u0053', new byte[]{(byte)0x6B});
    registerCodes('\u0054', new byte[]{(byte)0x6D});
    registerCodes('\u0055', new byte[]{(byte)0x6F});
    registerCodes('\u0056', new byte[]{(byte)0x71});
    registerCodes('\u0057', new byte[]{(byte)0x73});
    registerCodes('\u0058', new byte[]{(byte)0x75});
    registerCodes('\u0059', new byte[]{(byte)0x76});
    registerCodes('\u005A', new byte[]{(byte)0x78});
    registerCodes('\u005B', new byte[]{(byte)0x27});
    registerCodes('\\',     new byte[]{(byte)0x29});
    registerCodes('\u005D', new byte[]{(byte)0x2A});
    registerCodes('\u005E', new byte[]{(byte)0x2B, (byte)0x02});
    registerCodes('\u005F', new byte[]{(byte)0x2B, (byte)0x03});
    registerCodes('\u0060', new byte[]{(byte)0x2B, (byte)0x07});
    registerCodes('\u0061', new byte[]{(byte)0x4A});
    registerCodes('\u0062', new byte[]{(byte)0x4C});
    registerCodes('\u0063', new byte[]{(byte)0x4D});
    registerCodes('\u0064', new byte[]{(byte)0x4F});
    registerCodes('\u0065', new byte[]{(byte)0x51});
    registerCodes('\u0066', new byte[]{(byte)0x53});
    registerCodes('\u0067', new byte[]{(byte)0x55});
    registerCodes('\u0068', new byte[]{(byte)0x57});
    registerCodes('\u0069', new byte[]{(byte)0x59});
    registerCodes('\u006A', new byte[]{(byte)0x5B});
    registerCodes('\u006B', new byte[]{(byte)0x5C});
    registerCodes('\u006C', new byte[]{(byte)0x5E});
    registerCodes('\u006D', new byte[]{(byte)0x60});
    registerCodes('\u006E', new byte[]{(byte)0x62});
    registerCodes('\u006F', new byte[]{(byte)0x64});
    registerCodes('\u0070', new byte[]{(byte)0x66});
    registerCodes('\u0071', new byte[]{(byte)0x68});
    registerCodes('\u0072', new byte[]{(byte)0x69});
    registerCodes('\u0073', new byte[]{(byte)0x6B});
    registerCodes('\u0074', new byte[]{(byte)0x6D});
    registerCodes('\u0075', new byte[]{(byte)0x6F});
    registerCodes('\u0076', new byte[]{(byte)0x71});
    registerCodes('\u0077', new byte[]{(byte)0x73});
    registerCodes('\u0078', new byte[]{(byte)0x75});
    registerCodes('\u0079', new byte[]{(byte)0x76});
    registerCodes('\u007A', new byte[]{(byte)0x78});
    registerCodes('\u007B', new byte[]{(byte)0x2B, (byte)0x09});
    registerCodes('\u007C', new byte[]{(byte)0x2B, (byte)0x0B});
    registerCodes('\u007D', new byte[]{(byte)0x2B, (byte)0x0D});
    registerCodes('\u007E', new byte[]{(byte)0x2B, (byte)0x0F});
    registerCodes('\u00A0', new byte[]{(byte)0x08, (byte)0x02});
    registerCodes('\u00A1', new byte[]{(byte)0x2B, (byte)0x10});
    registerCodes('\u00A2', new byte[]{(byte)0x34, (byte)0xA6});
    registerCodes('\u00A3', new byte[]{(byte)0x34, (byte)0xA7});
    registerCodes('\u00A4', new byte[]{(byte)0x34, (byte)0xA8});
    registerCodes('\u00A5', new byte[]{(byte)0x34, (byte)0xA9});
    registerCodes('\u00A6', new byte[]{(byte)0x2B, (byte)0x11});
    registerCodes('\u00A7', new byte[]{(byte)0x34, (byte)0xAA});
    registerCodes('\u00A8', new byte[]{(byte)0x2B, (byte)0x12});
    registerCodes('\u00A9', new byte[]{(byte)0x34, (byte)0xAB});
    registerCodes('\u00AB', new byte[]{(byte)0x33, (byte)0x05});
    registerCodes('\u00AC', new byte[]{(byte)0x34, (byte)0xAC});
    registerCodes('\u00AE', new byte[]{(byte)0x34, (byte)0xAD});
    registerCodes('\u00AF', new byte[]{(byte)0x2B, (byte)0x13});
    registerCodes('\u00B0', new byte[]{(byte)0x34, (byte)0xAE});
    registerCodes('\u00B1', new byte[]{(byte)0x33, (byte)0x04});
    registerCodes('\u00B2', new byte[]{(byte)0x3A});
    registerCodes('\u00B3', new byte[]{(byte)0x3C});
    registerCodes('\u00B4', new byte[]{(byte)0x2B, (byte)0x14});
    registerCodes('\u00B5', new byte[]{(byte)0x34, (byte)0xAF});
    registerCodes('\u00B6', new byte[]{(byte)0x34, (byte)0xB0});
    registerCodes('\u00B7', new byte[]{(byte)0x34, (byte)0xB1});
    registerCodes('\u00B8', new byte[]{(byte)0x2B, (byte)0x15});
    registerCodes('\u00B9', new byte[]{(byte)0x38});
    registerCodes('\u00BB', new byte[]{(byte)0x33, (byte)0x07});
    registerCodes('\u00BC', new byte[]{(byte)0x37, (byte)0x12});
    registerCodes('\u00BD', new byte[]{(byte)0x37, (byte)0x16});
    registerCodes('\u00BE', new byte[]{(byte)0x37, (byte)0x1A});
    registerCodes('\u00BF', new byte[]{(byte)0x2B, (byte)0x16});
    registerCodes('\u00C6', new byte[]{(byte)0x4A, (byte)0x51});
    registerCodes('\u00D7', new byte[]{(byte)0x33, (byte)0x09});
    registerCodes('\u00DE', new byte[]{(byte)0x6D, (byte)0x57});
    registerCodes('\u00DF', new byte[]{(byte)0x6B, (byte)0x6B});
    registerCodes('\u00E6', new byte[]{(byte)0x4A, (byte)0x51});
    registerCodes('\u00F7', new byte[]{(byte)0x33, (byte)0x0A});
    registerCodes('\u00FE', new byte[]{(byte)0x6D, (byte)0x57});

    registerUnprintableCodes('\u0001', new byte[]{(byte)0x03});
    registerUnprintableCodes('\u0002', new byte[]{(byte)0x04});
    registerUnprintableCodes('\u0003', new byte[]{(byte)0x05});
    registerUnprintableCodes('\u0004', new byte[]{(byte)0x06});
    registerUnprintableCodes('\u0005', new byte[]{(byte)0x07});
    registerUnprintableCodes('\u0006', new byte[]{(byte)0x08});
    registerUnprintableCodes('\u0007', new byte[]{(byte)0x09});
    registerUnprintableCodes('\b',     new byte[]{(byte)0x0A});
    registerUnprintableCodes('\u000E', new byte[]{(byte)0x0B});
    registerUnprintableCodes('\u000F', new byte[]{(byte)0x0C});
    registerUnprintableCodes('\u0010', new byte[]{(byte)0x0D});
    registerUnprintableCodes('\u0011', new byte[]{(byte)0x0E});
    registerUnprintableCodes('\u0012', new byte[]{(byte)0x0F});
    registerUnprintableCodes('\u0013', new byte[]{(byte)0x10});
    registerUnprintableCodes('\u0014', new byte[]{(byte)0x11});
    registerUnprintableCodes('\u0015', new byte[]{(byte)0x12});
    registerUnprintableCodes('\u0016', new byte[]{(byte)0x13});
    registerUnprintableCodes('\u0017', new byte[]{(byte)0x14});
    registerUnprintableCodes('\u0018', new byte[]{(byte)0x15});
    registerUnprintableCodes('\u0019', new byte[]{(byte)0x16});
    registerUnprintableCodes('\u001A', new byte[]{(byte)0x17});
    registerUnprintableCodes('\u001B', new byte[]{(byte)0x18});
    registerUnprintableCodes('\u001C', new byte[]{(byte)0x19});
    registerUnprintableCodes('\u001D', new byte[]{(byte)0x1A});
    registerUnprintableCodes('\u001E', new byte[]{(byte)0x1B});
    registerUnprintableCodes('\u001F', new byte[]{(byte)0x1C});
    registerUnprintableCodes('\'',     new byte[]{(byte)0x80});
    registerUnprintableCodes('\u002D', new byte[]{(byte)0x82});
    registerUnprintableCodes('\u007F', new byte[]{(byte)0x1D});
    registerUnprintableCodes('\u0080', new byte[]{(byte)0x1E});
    registerUnprintableCodes('\u0081', new byte[]{(byte)0x1F});
    registerUnprintableCodes('\u0082', new byte[]{(byte)0x20});
    registerUnprintableCodes('\u0083', new byte[]{(byte)0x21});
    registerUnprintableCodes('\u0084', new byte[]{(byte)0x22});
    registerUnprintableCodes('\u0085', new byte[]{(byte)0x23});
    registerUnprintableCodes('\u0086', new byte[]{(byte)0x24});
    registerUnprintableCodes('\u0087', new byte[]{(byte)0x25});
    registerUnprintableCodes('\u0088', new byte[]{(byte)0x26});
    registerUnprintableCodes('\u0089', new byte[]{(byte)0x27});
    registerUnprintableCodes('\u008A', new byte[]{(byte)0x28});
    registerUnprintableCodes('\u008B', new byte[]{(byte)0x29});
    registerUnprintableCodes('\u008C', new byte[]{(byte)0x2A});
    registerUnprintableCodes('\u008D', new byte[]{(byte)0x2B});
    registerUnprintableCodes('\u008E', new byte[]{(byte)0x2C});
    registerUnprintableCodes('\u008F', new byte[]{(byte)0x2D});
    registerUnprintableCodes('\u0090', new byte[]{(byte)0x2E});
    registerUnprintableCodes('\u0091', new byte[]{(byte)0x2F});
    registerUnprintableCodes('\u0092', new byte[]{(byte)0x30});
    registerUnprintableCodes('\u0093', new byte[]{(byte)0x31});
    registerUnprintableCodes('\u0094', new byte[]{(byte)0x32});
    registerUnprintableCodes('\u0095', new byte[]{(byte)0x33});
    registerUnprintableCodes('\u0096', new byte[]{(byte)0x34});
    registerUnprintableCodes('\u0097', new byte[]{(byte)0x35});
    registerUnprintableCodes('\u0098', new byte[]{(byte)0x36});
    registerUnprintableCodes('\u0099', new byte[]{(byte)0x37});
    registerUnprintableCodes('\u009A', new byte[]{(byte)0x38});
    registerUnprintableCodes('\u009B', new byte[]{(byte)0x39});
    registerUnprintableCodes('\u009C', new byte[]{(byte)0x3A});
    registerUnprintableCodes('\u009D', new byte[]{(byte)0x3B});
    registerUnprintableCodes('\u009E', new byte[]{(byte)0x3C});
    registerUnprintableCodes('\u009F', new byte[]{(byte)0x3D});
    registerUnprintableCodes('\u00AD', new byte[]{(byte)0x83});

    registerInternationalCodes('\u00AA', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x03});
    registerInternationalCodes('\u00BA', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x03});
    registerInternationalCodes('\u00C0', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00C1', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00C2', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00C3', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x19});
    registerInternationalCodes('\u00C4', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00C5', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x1A});
    registerInternationalCodes('\u00C7', new byte[]{(byte)0x4D},
                               new byte[]{(byte)0x1C});
    registerInternationalCodes('\u00C8', new byte[]{(byte)0x51},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00C9', new byte[]{(byte)0x51},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00CA', new byte[]{(byte)0x51},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00CB', new byte[]{(byte)0x51},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00CC', new byte[]{(byte)0x59},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00CD', new byte[]{(byte)0x59},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00CE', new byte[]{(byte)0x59},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00CF', new byte[]{(byte)0x59},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00D0', new byte[]{(byte)0x4F},
                               new byte[]{(byte)0x68});
    registerInternationalCodes('\u00D1', new byte[]{(byte)0x62},
                               new byte[]{(byte)0x19});
    registerInternationalCodes('\u00D2', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00D3', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00D4', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00D5', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x19});
    registerInternationalCodes('\u00D6', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00D8', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x21});
    registerInternationalCodes('\u00D9', new byte[]{(byte)0x6F},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00DA', new byte[]{(byte)0x6F},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00DB', new byte[]{(byte)0x6F},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00DC', new byte[]{(byte)0x6F},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00DD', new byte[]{(byte)0x76},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00E0', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00E1', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00E2', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00E3', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x19});
    registerInternationalCodes('\u00E4', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00E5', new byte[]{(byte)0x4A},
                               new byte[]{(byte)0x1A});
    registerInternationalCodes('\u00E7', new byte[]{(byte)0x4D},
                               new byte[]{(byte)0x1C});
    registerInternationalCodes('\u00E8', new byte[]{(byte)0x51},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00E9', new byte[]{(byte)0x51},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00EA', new byte[]{(byte)0x51},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00EB', new byte[]{(byte)0x51},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00EC', new byte[]{(byte)0x59},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00ED', new byte[]{(byte)0x59},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00EE', new byte[]{(byte)0x59},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00EF', new byte[]{(byte)0x59},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00F0', new byte[]{(byte)0x4F},
                               new byte[]{(byte)0x68});
    registerInternationalCodes('\u00F1', new byte[]{(byte)0x62},
                               new byte[]{(byte)0x19});
    registerInternationalCodes('\u00F2', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00F3', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00F4', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00F5', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x19});
    registerInternationalCodes('\u00F6', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00F8', new byte[]{(byte)0x64},
                               new byte[]{(byte)0x21});
    registerInternationalCodes('\u00F9', new byte[]{(byte)0x6F},
                               new byte[]{(byte)0x0F});
    registerInternationalCodes('\u00FA', new byte[]{(byte)0x6F},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00FB', new byte[]{(byte)0x6F},
                               new byte[]{(byte)0x12});
    registerInternationalCodes('\u00FC', new byte[]{(byte)0x6F},
                               new byte[]{(byte)0x13});
    registerInternationalCodes('\u00FD', new byte[]{(byte)0x76},
                               new byte[]{(byte)0x0E});
    registerInternationalCodes('\u00FF', new byte[]{(byte)0x76},
                               new byte[]{(byte)0x13});
    
  }

  
  private IndexCodes() {
  }

  private static void registerCodes(char c, byte[] codes) {
    CODES.put(c, codes);
  }
  
  private static void registerUnprintableCodes(char c, byte[] codes) {
    UNPRINTABLE_CODES.put(c, codes);
  }
  
  private static void registerInternationalCodes(
      char c, byte[] inlineCodes, byte[] extraCodes) {
    INTERNATIONAL_CODES.put(c,
                            new InternationalCodes(inlineCodes, extraCodes));
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
  
  static final class InternationalCodes {
    public final byte[] _inlineCodes;
    public final byte[] _extraCodes;

    private InternationalCodes(byte[] inlineCodes, byte[] extraCodes) {
      _inlineCodes = inlineCodes;
      _extraCodes = extraCodes;
    }
  }
  
}
