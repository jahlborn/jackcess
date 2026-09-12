/*
Copyright (c) 2010 James Ahlborn

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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for a handler which can encode/decode a specific access page
 * encoding.
 *
 * @author James Ahlborn
 */
public interface CodecHandler 
{
  /**
   * Returns {@code true} if this handler can encode partial pages,
   * {@code false} otherwise.  If this method returns {@code false}, the
   * {@link #encodePage} method will never be called with a non-zero
   * pageOffset.
   */
  public boolean canEncodePartialPage();

  /**
   * Returns {@code true} if this handler can decode a page inline,
   * {@code false} otherwise.  If this method returns {@code false}, the
   * {@link #decodePage} method will always be called with separate buffers.
   */
  public boolean canDecodeInline();

  /**
   * Decodes the given page buffer.
   *
   * @param inPage the page to be decoded
   * @param outPage the decoded page.  if {@link #canDecodeInline} is {@code
   *                true}, this will be the same buffer as inPage.
   * @param pageNumber the page number of the given page
   * 
   * @throws IOException if an exception occurs during decoding
   */
  public void decodePage(ByteBuffer inPage, ByteBuffer outPage, int pageNumber) 
    throws IOException;

  /**
   * Encodes the given page buffer into a new page buffer and returns it.  The
   * returned page buffer will be used immediately and discarded so that it
   * may be re-used for subsequent page encodings.
   *
   * @param page the page to be encoded, should not be modified
   * @param pageNumber the page number of the given page
   * @param pageOffset offset within the page at which to start writing the
   *                   page data
   * 
   * @throws IOException  if an exception occurs during decoding
   *
   * @return the properly encoded page buffer for the given page buffer 
   */
  public ByteBuffer encodePage(ByteBuffer page, int pageNumber, 
                               int pageOffset) 
    throws IOException;
}
