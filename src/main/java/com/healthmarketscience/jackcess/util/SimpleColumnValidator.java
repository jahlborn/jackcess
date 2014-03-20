/*
Copyright (c) 2014 James Ahlborn

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

package com.healthmarketscience.jackcess.util;

import java.io.IOException;

import com.healthmarketscience.jackcess.Column;

/**
 * Simple concrete implementation of ColumnValidator which simply returns the
 * given value.
 *
 * @author James Ahlborn
 */
public class SimpleColumnValidator implements ColumnValidator
{
  public static final SimpleColumnValidator INSTANCE = new SimpleColumnValidator();
  
  public Object validate(Column col, Object val) throws IOException {
    return val;
  }
}
