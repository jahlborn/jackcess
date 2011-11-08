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
 * Base class for a value in a complex column (where there may be multiple
 * values for a single row in the main table).
 *
 * @author James Ahlborn
 */
public interface ComplexValue 
{
  /**
   * Returns the unique identifier of this complex value (this value is unique
   * among all values in all rows of the main table).
   * 
   * @return the current id or {@link ComplexColumnInfo#INVALID_ID} for a new,
   *         unsaved value.
   */
  public int getId();

  public void setId(int newId);
  
  /**
   * Returns the foreign key identifier for this complex value (this value is
   * the same for all values in the same row of the main table).
   * 
   * @return the current id or {@link ComplexColumnInfo#INVALID_COMPLEX_VALUE_ID}
   *         for a new, unsaved value.
   */
  public ComplexValueForeignKey getComplexValueForeignKey();

  public void setComplexValueForeignKey(ComplexValueForeignKey complexValueFk);

  /**
   * @return the column in the main table with which this complex value is
   *         associated
   */
  public Column getColumn();

  /**
   * Writes any updated data for this complex value to the database.
   */
  public void update() throws IOException;

  /**
   * Deletes the data for this complex value from the database.
   */
  public void delete() throws IOException;

}
