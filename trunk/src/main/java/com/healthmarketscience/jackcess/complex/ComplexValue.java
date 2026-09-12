/*
Copyright (c) 2011 James Ahlborn

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

package com.healthmarketscience.jackcess.complex;

import java.io.IOException;
import java.io.ObjectStreamException;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.RowId;
import com.healthmarketscience.jackcess.impl.complex.ComplexColumnInfoImpl;

/**
 * Base interface for a value in a complex column (where there may be multiple
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
   * @return the current id or {@link ComplexColumnInfoImpl#INVALID_ID} for a new,
   *         unsaved value.
   */
  public Id getId();

  /**
   * Called once when a new ComplexValue is saved to set the new unique
   * identifier.
   */
  public void setId(Id newId);
  
  /**
   * Returns the foreign key identifier for this complex value (this value is
   * the same for all values in the same row of the main table).
   * 
   * @return the current id or {@link ComplexColumnInfoImpl#INVALID_FK}
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


  /**
   * Identifier for a ComplexValue.  Only valid for comparing complex values
   * for the same column.
   */
  public abstract class Id extends Number 
  {
    private static final long serialVersionUID = 20130318L;    

    @Override
    public byte byteValue() {
      return (byte)get();
    }
  
    @Override
    public short shortValue() {
      return (short)get();
    }
  
    @Override
    public int intValue() {
      return get();
    }
  
    @Override
    public long longValue() {
      return get();
    }
  
    @Override
    public float floatValue() {
      return get();
    }
  
    @Override
    public double doubleValue() {
      return get();
    }
    
    @Override
    public int hashCode() {
      return get();
    }
  
    @Override
    public boolean equals(Object o) {
      return ((this == o) ||
              ((o != null) && (getClass() == o.getClass()) &&
               (get() == ((Id)o).get())));
    }

    @Override
    public String toString() {
      return String.valueOf(get());
    }  

    protected final Object writeReplace() throws ObjectStreamException {
      // if we are going to serialize this ComplexValue.Id, convert it back to
      // a normal Integer (in case it is restored outside of the context of
      // jackcess)
      return Integer.valueOf(get());
    }

    /**
     * Returns the unique identifier of this complex value (this value is unique
     * among all values in all rows of the main table for the complex column).
     */
    public abstract int get();
    
    /**
     * Returns the rowId of this ComplexValue within the secondary table.
     */
    public abstract RowId getRowId();
  }
}
