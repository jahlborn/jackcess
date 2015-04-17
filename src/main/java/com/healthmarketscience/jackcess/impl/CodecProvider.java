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
import java.nio.charset.Charset;

/**
 * Interface for a provider which can generate CodecHandlers for various types
 * of database encodings.  The {@link DefaultCodecProvider} is the default
 * implementation of this inferface, but it does not have any actual
 * encoding/decoding support (due to possible export issues with calling
 * encryption APIs).  See the separate
 * <a href="https://sourceforge.net/projects/jackcessencrypt/">Jackcess
 * Encrypt</a> project for an implementation of this interface which supports
 * various access database encryption types.
 *
 * @author James Ahlborn
 */
public interface CodecProvider 
{
  /**
   * Returns a new CodecHandler for the database associated with the given
   * PageChannel.
   * 
   * @param channel the PageChannel for a Database
   * @param charset the Charset for the Database
   * 
   * @return a new CodecHandler, may not be {@code null}
   */
  public CodecHandler createHandler(PageChannel channel, Charset charset)
    throws IOException;
}
