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

  static final byte ASC_BOOLEAN_TRUE = (byte)0x00;
  static final byte ASC_BOOLEAN_FALSE = (byte)0xFF;
  
  static final byte DESC_BOOLEAN_TRUE = ASC_BOOLEAN_FALSE;
  static final byte DESC_BOOLEAN_FALSE = ASC_BOOLEAN_TRUE;


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
