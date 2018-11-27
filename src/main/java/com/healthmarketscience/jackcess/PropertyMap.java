/*
Copyright (c) 2013 James Ahlborn

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
  public static final String ALLOW_MULTI_VALUE_PROP = "AllowMultipleValues";
  public static final String ROW_SOURCE_TYPE_PROP = "RowSourceType";
  public static final String ROW_SOURCE_PROP = "RowSource";
  public static final String DISPLAY_CONTROL_PROP = "DisplayControl";
  public static final String TEXT_FORMAT_PROP = "TextFormat";
  public static final String IME_MODE_PROP = "IMEMode";
  public static final String IME_SENTENCE_MODE_PROP = "IMESentenceMode";


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
   * <br>
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
   * <br>
   * Note, this change will not be persisted until the {@link #save} method
   * has been called.
   *
   * @return the newly created (or updated) property
   */
  public Property put(String name, DataType type, Object value);

  /**
   * Creates a new (or updates an existing) property in the map.
   * <br>
   * Note, this change will not be persisted until the {@link #save} method
   * has been called.
   *
   * @return the newly created (or updated) property
   */
  public Property put(String name, DataType type, Object value, boolean isDdl);

  /**
   * Puts all the given properties into this map.
   *
   * @param props the properties to put into this map ({@code null} is
   *              tolerated and ignored).
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

    /**
     * Whether or not this property is a DDL object.  If {@code true}, users
     * can't change or delete the property in access without the dbSecWriteDef
     * permission.  Additionally, certain properties must be flagged correctly
     * or the access engine may not recognize them correctly.
     */
    public boolean isDdl();

    public Object getValue();

    /**
     * Sets the new value for this property.
     * <br>
     * Note, this change will not be persisted until the {@link
     * PropertyMap#save} method has been called.
     */
    public void setValue(Object newValue);
  }

  /**
   * Interface for enums which can be used as property values.
   */
  public interface EnumValue
  {
    /**
     * @return the property value which should be stored in the db
     */
    public Object getValue();
  }

  /**
   * Enum value constants for the DisplayControl property
   */
  public enum DisplayControl implements EnumValue
  {
    BOUND_OBJECT_FRAME(108),
    CHECK_BOX(106),
    COMBO_BOX(111),
    COMMAND_BUTTON(104),
    CUSTOM_CONTROL(119),
    EMPTY_CELL(127),
    IMAGE(103),
    LABEL(100),
    LINE(102),
    LIST_BOX(110),
    NAVIGATION_BUTTON(130),
    NAVIGATION_CONTROL(129),
    OBJECT_FRAME(114),
    OPTION_BUTTON(105),
    OPTION_GROUP(107),
    PAGE(124),
    PAGE_BREAK(118),
    RECTANGLE(101),
    SUB_FORM(112),
    TAB_CTL(123),
    TEXT_BOX(109),
    TOGGLE_BUTTON(122),
    WEB_BROWSER(128);

    private final Short _value;

    private DisplayControl(int value) {
      _value = (short)value;
    }

    public Short getValue() {
      return _value;
    }

    @Override
    public String toString() {
      return name() + "[" + _value + "]";
    }
  }

  /**
   * Enum value constants for the TextFormat property
   */
  public enum TextFormat implements EnumValue
  {
    HTMLRICHTEXT(1),
    PLAIN(0);

    private final Byte _value;

    private TextFormat(int value) {
      _value = (byte)value;
    }

    public Byte getValue() {
      return _value;
    }

    @Override
    public String toString() {
      return name() + "[" + _value + "]";
    }
  }

  /**
   * Enum value constants for the IMEMode property
   */
  public enum IMEMode implements EnumValue
  {
    NOCONTROL(0),
    ON(1),
    OFF(2),
    DISABLE(3),
    HIRAGANA(4),
    KATAKANA(5),
    KATAKANAHALF(6),
    ALPHAFULL(7),
    ALPHA(8),
    HANGULFULL(9),
    HANGUL(10);

    private final Byte _value;

    private IMEMode(int value) {
      _value = (byte)value;
    }

    public Byte getValue() {
      return _value;
    }

    @Override
    public String toString() {
      return name() + "[" + _value + "]";
    }
  }

  /**
   * Enum value constants for the IMESentenceMode property
   */
  public enum IMESentenceMode implements EnumValue
  {
    NORMAL(0),
    PLURAL(1),
    SPEAKING(2),
    NONE(3);

    private final Byte _value;

    private IMESentenceMode(int value) {
      _value = (byte)value;
    }

    public Byte getValue() {
      return _value;
    }

    @Override
    public String toString() {
      return name() + "[" + _value + "]";
    }
  }

}
