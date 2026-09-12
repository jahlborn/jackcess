/*
Copyright (c) 2005 Health Market Science, Inc.

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
 * Codes for page types
 * @author Tim McCune
 */
public interface PageTypes {
  
  /** invalid page type */
  public static final byte INVALID = (byte)0x00;
  /** Data page */
  public static final byte DATA = (byte)0x01;
  /** Table definition page */
  public static final byte TABLE_DEF = (byte)0x02;
  /** intermediate index page pointing to other index pages */
  public static final byte INDEX_NODE = (byte)0x03;
  /** leaf index page containing actual entries */
  public static final byte INDEX_LEAF = (byte)0x04;
  /** Table usage map page */
  public static final byte USAGE_MAP = (byte)0x05;
  
}
