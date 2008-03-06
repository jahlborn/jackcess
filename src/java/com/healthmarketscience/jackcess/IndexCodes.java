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

  static final byte ASC_END_TEXT = (byte)0x01;
  static final byte DESC_END_TEXT = (byte)0xFE;

  static final byte[] ASC_END_EXTRA_TEXT =
    new byte[]{(byte)0x00};
  static final byte[] DESC_END_EXTRA_TEXT =
    new byte[]{(byte)0xFF, (byte)0x00};

  static final byte[] ASC_BOOLEAN_TRUE =
    new byte[]{ASC_START_FLAG, (byte)0x00};
  static final byte[] ASC_BOOLEAN_FALSE =
    new byte[]{ASC_START_FLAG, (byte)0xFF};
  
  static final byte[] DESC_BOOLEAN_TRUE =
    new byte[]{DESC_START_FLAG, (byte)0xFF};
  static final byte[] DESC_BOOLEAN_FALSE =
    new byte[]{DESC_START_FLAG, (byte)0x00};
  
  
  /**
   * Map of character to byte[] that Access uses in indexes (not ASCII)
   * (Character -> byte[]) as codes to order text
   */
  static final Map<Character, byte[]> CODES = new HashMap<Character, byte[]>();

  /**
   * Map of character to byte[] that Access uses in indexes for unprintable
   * characters (not ASCII) (Character -> byte[]), in the extended portion
   */
  static final Map<Character, byte[]> UNPRINTABLE_CODES =
    new HashMap<Character, byte[]>();
  
  static {

    CODES.put('^', new byte[]{(byte)43, (byte)2});
    CODES.put('_', new byte[]{(byte)43, (byte)3});
    CODES.put('`', new byte[]{(byte)43, (byte)7});
    CODES.put('{', new byte[]{(byte)43, (byte)9});
    CODES.put('|', new byte[]{(byte)43, (byte)11});
    CODES.put('}', new byte[]{(byte)43, (byte)13});
    CODES.put('~', new byte[]{(byte)43, (byte)15});
    
    CODES.put('\t', new byte[]{(byte)8, (byte)3});
    CODES.put('\r', new byte[]{(byte)8, (byte)4});
    CODES.put('\n', new byte[]{(byte)8, (byte)7});
        
    CODES.put(' ', new byte[]{(byte)7});
    CODES.put('!', new byte[]{(byte)9});
    CODES.put('"', new byte[]{(byte)10});
    CODES.put('#', new byte[]{(byte)12});
    CODES.put('$', new byte[]{(byte)14});
    CODES.put('%', new byte[]{(byte)16});
    CODES.put('&', new byte[]{(byte)18});
    CODES.put('(', new byte[]{(byte)20});
    CODES.put(')', new byte[]{(byte)22});
    CODES.put('*', new byte[]{(byte)24});
    CODES.put(',', new byte[]{(byte)26});
    CODES.put('.', new byte[]{(byte)28});
    CODES.put('/', new byte[]{(byte)30});
    CODES.put(':', new byte[]{(byte)32});
    CODES.put(';', new byte[]{(byte)34});
    CODES.put('?', new byte[]{(byte)36});
    CODES.put('@', new byte[]{(byte)38});    
    CODES.put('[', new byte[]{(byte)39});
    CODES.put('\\', new byte[]{(byte)41});
    CODES.put(']', new byte[]{(byte)42});
    CODES.put('+', new byte[]{(byte)44});
    CODES.put('<', new byte[]{(byte)46});
    CODES.put('=', new byte[]{(byte)48});
    CODES.put('>', new byte[]{(byte)50});
    CODES.put('0', new byte[]{(byte)54});
    CODES.put('1', new byte[]{(byte)56});
    CODES.put('2', new byte[]{(byte)58});
    CODES.put('3', new byte[]{(byte)60});
    CODES.put('4', new byte[]{(byte)62});
    CODES.put('5', new byte[]{(byte)64});
    CODES.put('6', new byte[]{(byte)66});
    CODES.put('7', new byte[]{(byte)68});
    CODES.put('8', new byte[]{(byte)70});
    CODES.put('9', new byte[]{(byte)72});
    CODES.put('A', new byte[]{(byte)74});
    CODES.put('B', new byte[]{(byte)76});
    CODES.put('C', new byte[]{(byte)77});
    CODES.put('D', new byte[]{(byte)79});
    CODES.put('E', new byte[]{(byte)81});
    CODES.put('F', new byte[]{(byte)83});
    CODES.put('G', new byte[]{(byte)85});
    CODES.put('H', new byte[]{(byte)87});
    CODES.put('I', new byte[]{(byte)89});
    CODES.put('J', new byte[]{(byte)91});
    CODES.put('K', new byte[]{(byte)92});
    CODES.put('L', new byte[]{(byte)94});
    CODES.put('M', new byte[]{(byte)96});
    CODES.put('N', new byte[]{(byte)98});
    CODES.put('O', new byte[]{(byte)100});
    CODES.put('P', new byte[]{(byte)102});
    CODES.put('Q', new byte[]{(byte)104});
    CODES.put('R', new byte[]{(byte)105});
    CODES.put('S', new byte[]{(byte)107});
    CODES.put('T', new byte[]{(byte)109});
    CODES.put('U', new byte[]{(byte)111});
    CODES.put('V', new byte[]{(byte)113});
    CODES.put('W', new byte[]{(byte)115});
    CODES.put('X', new byte[]{(byte)117});
    CODES.put('Y', new byte[]{(byte)118});
    CODES.put('Z', new byte[]{(byte)120});

    // codes are case insensitive, so put in all the lower case codes using
    // the equivalent upper case char
    for(int i = 0; i < 26; ++i) {
      byte[] codes = CODES.get((char)('A' + i));
      CODES.put((char)('a' + i), codes);
    }
    
    UNPRINTABLE_CODES.put('\'', new byte[]{(byte)6, (byte)128});
    UNPRINTABLE_CODES.put('-', new byte[]{(byte)6, (byte)130});
  }

  
  private IndexCodes() {
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
  
  static byte getEndTextEntryFlag(boolean isAscending) {
    return(isAscending ? ASC_END_TEXT : DESC_END_TEXT);
  }
  
  static byte[] getEndExtraTextEntryFlags(boolean isAscending) {
    return(isAscending ? ASC_END_EXTRA_TEXT : DESC_END_EXTRA_TEXT);
  }
  
}
