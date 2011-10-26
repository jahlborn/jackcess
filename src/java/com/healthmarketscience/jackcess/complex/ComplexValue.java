/*
Copyright (c) 2011 James Ahlborn

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

package com.healthmarketscience.jackcess.complex;

import java.io.IOException;

import com.healthmarketscience.jackcess.Column;

/**
 *
 * @author James Ahlborn
 */
public interface ComplexValue 
{
  public int getId();

  public void setId(int newId);
  
  public ComplexValueForeignKey getComplexValueForeignKey();

  public void setComplexValueForeignKey(ComplexValueForeignKey complexValueFk);

  public Column getColumn();

  public void update() throws IOException;
}
