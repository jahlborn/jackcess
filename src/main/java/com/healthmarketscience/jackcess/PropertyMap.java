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
 * Map of properties for a database object.
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public interface PropertyMap extends Iterable<PropertyMap.Property>
{
  public static final String ACCESS_VERSION_PROP = "AccessVersion";
  public static final String TITLE_PROP = "Title";
  public static final String AUTHOR_PROP = "Author";
  public static final String COMPANY_PROP = "Company";

  public static final String DEFAULT_VALUE_PROP = "DefaultValue";
  public static final String REQUIRED_PROP = "Required";
  public static final String ALLOW_ZERO_LEN_PROP = "AllowZeroLength";
  public static final String DECIMAL_PLACES_PROP = "DecimalPlaces";
  public static final String FORMAT_PROP = "Format";
  public static final String INPUT_MASK_PROP = "InputMask";
  public static final String CAPTION_PROP = "Caption";
  public static final String VALIDATION_RULE_PROP = "ValidationRule";
  public static final String VALIDATION_TEXT_PROP = "ValidationText";
  public static final String GUID_PROP = "GUID";
  public static final String DESCRIPTION_PROP = "Description";
  public static final String RESULT_TYPE_PROP = "ResultType";
  public static final String EXPRESSION_PROP = "Expression";


  public String getName();

  public int getSize();

  public boolean isEmpty();

  /**
   * @return the property with the given name, if any
   */
  public Property get(String name);

  /**
   * @return the value of the property with the given name, if any
   */
  public Object getValue(String name);

  /**
   * @return the value of the property with the given name, if any, otherwise
   *         the given defaultValue
   */
  public Object getValue(String name, Object defaultValue);

  /**
   * Creates a new (or updates an existing) property in the map.  Attempts to
   * determine the type of the property based on the name and value (the
   * property names listed above have their types builtin, otherwise the type
   * of the value is used).
   * <p/>
   * Note, this change will not be persisted until the {@link #save} method
   * has been called.
   *
   * @return the newly created (or updated) property
   * @throws IllegalArgumentException if the type of the property could not be
   *                                  determined automatically
   */
  public Property put(String name, Object value);

  /**
   * Creates a new (or updates an existing) property in the map.
   * <p/>
   * Note, this change will not be persisted until the {@link #save} method
   * has been called.
   *
   * @return the newly created (or updated) property
   */
  public Property put(String name, DataType type, Object value);

  /**
   * Puts all the given properties into this map.
   *
   * @props the properties to put into this map ({@code null} is tolerated and
   *        ignored).
   */
  public void putAll(Iterable<? extends Property> props);
  
  /**
   * Removes the property with the given name
   *
   * @return the removed property, or {@code null} if none found
   */
  public Property remove(String name);

  /**
   * Saves the current state of this map.
   */
  public void save() throws IOException;

  /**
   * Info about a property defined in a PropertyMap.
   */ 
  public interface Property 
  {
    public String getName();

    public DataType getType();

    public Object getValue();

    /**
     * Sets the new value for this property.
     * <p/>
     * Note, this change will not be persisted until the {@link
     * PropertyMap#save} method has been called.
     */
    public void setValue(Object newValue);
  }
}
