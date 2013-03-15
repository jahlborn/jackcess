/*
Copyright (c) 2013 James Ahlborn

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

/**
 * RuntimeException wrapper around an IOException
 *
 * @author James Ahlborn
 */
public class RuntimeIOException extends IllegalStateException 
{
  private static final long serialVersionUID = 20130315L;  

  public RuntimeIOException(IOException e) 
  {
    this(((e != null) ? e.getMessage() : null), e);
  }

  public RuntimeIOException(String msg, IOException e) 
  {
    super(msg, e);
  }
}
