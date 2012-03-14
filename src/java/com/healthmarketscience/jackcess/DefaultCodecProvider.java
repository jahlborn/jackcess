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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Default implementation of CodecProvider which does not have any actual
 * encoding/decoding support.  See {@link CodecProvider} for details on a more
 * useful implementation.
 *
 * @author James Ahlborn
 */
public class DefaultCodecProvider implements CodecProvider
{
  /** common instance of DefaultCodecProvider */
  public static final CodecProvider INSTANCE = 
    new DefaultCodecProvider();

  /** common instance of {@link DummyHandler} */
  public static final CodecHandler DUMMY_HANDLER = 
    new DummyHandler();

  /** common instance of {@link UnsupportedHandler} */
  public static final CodecHandler UNSUPPORTED_HANDLER = 
    new UnsupportedHandler();


  /**
   * {@inheritDoc}
   * <p>
   * This implementation returns DUMMY_HANDLER for databases with no encoding
   * and UNSUPPORTED_HANDLER for databases with any encoding.
   */
  public CodecHandler createHandler(PageChannel channel, Charset charset)
    throws IOException
  {
    JetFormat format = channel.getFormat();
    switch(format.CODEC_TYPE) {
    case NONE:
      // no encoding, all good
      return DUMMY_HANDLER;

    case JET:
    case OFFICE:
      // check for an encode key.  if 0, not encoded
      ByteBuffer bb = channel.createPageBuffer();
      channel.readPage(bb, 0);
      int codecKey = bb.getInt(format.OFFSET_ENCODING_KEY);
      return((codecKey == 0) ? DUMMY_HANDLER : UNSUPPORTED_HANDLER);

    case MSISAM:
      // always encoded, we don't handle it
      return UNSUPPORTED_HANDLER;

    default:
      throw new RuntimeException("Unknown codec type " + format.CODEC_TYPE);
    }
  }

  /**
   * CodecHandler implementation which does nothing, useful for databases with
   * no extra encoding.
   */
  public static class DummyHandler implements CodecHandler
  {
    public void decodePage(ByteBuffer page, int pageNumber) throws IOException
    {
      // does nothing
    }

    public ByteBuffer encodePage(ByteBuffer page, int pageNumber, 
                                 int pageOffset) 
      throws IOException
    {
      // does nothing
      return page;
    }
  }

  /**
   * CodecHandler implementation which always throws
   * UnsupportedCodecException, useful for databases with unsupported
   * encodings.
   */
  public static class UnsupportedHandler implements CodecHandler
  {
    public void decodePage(ByteBuffer page, int pageNumber) throws IOException
    {
      throw new UnsupportedCodecException("Decoding not supported.  Please choose a CodecProvider which supports reading the current database encoding.");
    }

    public ByteBuffer encodePage(ByteBuffer page, int pageNumber, 
                                 int pageOffset) 
      throws IOException
    {
      throw new UnsupportedCodecException("Encoding not supported.  Please choose a CodecProvider which supports writing the current database encoding.");
    }
  }

}
