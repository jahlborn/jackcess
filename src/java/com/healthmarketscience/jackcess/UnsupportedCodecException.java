/*
Copyright (c) 2012 James Ahlborn

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

/**
 * Exception thrown by a CodecHandler to indicate that the current encoding is
 * not supported.  This generally indicates that a different CodecProvider
 * needs to be chosen.
 *
 * @author James Ahlborn
 */
public class UnsupportedCodecException 
  extends UnsupportedOperationException 
{
  private static final long serialVersionUID = 20120313L;  

  public UnsupportedCodecException(String msg) 
  {
    super(msg);
  }

  public UnsupportedCodecException(String msg, Throwable t) 
  {
    super(msg, t);
  }

  public UnsupportedCodecException(Throwable t) 
  {
    super(t);
  }
}
