/*
Copyright (c) 2010 James Ahlborn

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
*/

package com.healthmarketscience.jackcess;

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
