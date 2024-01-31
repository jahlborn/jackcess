package com.healthmarketscience.jackcess.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * <p>
 * Static utility methods for null-safe {@link String} operations.
 * </p>
 * 
 * The class prefers interface {@link CharSequence} for inputs over
 * {@code String} whenever possible, so that all implementations (e.g. 
 * {@link StringBuffer}, {@link StringBuilder}, {@link CharBuffer}) can benefit.
 * 
 * @author Markus Spann
 */
public final class StringUtil
{

  private StringUtil() {
  }

  /**
   * Gets the given char sequence's length or {@code 0} if it is {@code null}.
   *
   * @param cs string
   * @return length of string
   */
  public static int length(CharSequence cs)
  {
    return cs == null ? 0 : cs.length();
  }

  /**
   * Checks if the given char sequence is either null or empty.
   *
   * @param cs char sequence to test
   * @return true if char sequence is empty or null, false otherwise
   */
  public static boolean isEmpty(CharSequence cs)
  {
    return length(cs) == 0;
  }

  /**
   * Returns {@code true} if the given char sequence is {@code null} or all blank space,
   * {@code false} otherwise.
   */
  public static boolean isBlank(CharSequence cs)
  {
    int len = length(cs);
    return len == 0 || IntStream.range(0, len).allMatch(i -> Character.isWhitespace(cs.charAt(i)));
  }

  /**
   * Returns the given char sequence trimmed or {@code null} if the string is {@code null} or empty.
   */
  public static String trimToNull(CharSequence cs)
  {
    String str = cs == null ? null : cs.toString().trim();
    return isEmpty(str) ? null : str;
  }

  /**
   * Capitalizes a string changing its first character to title case as per
   * {@link Character#toTitleCase(int)}.
   */
  public static String capitalize(String str)
  {
    if (isEmpty(str)) {
      return str;
    }

    int cp = str.codePointAt(0);
    int newCp = Character.toTitleCase(cp);
    return cp == newCp ? str : new String(Character.toString((char) newCp)) + str.substring(1);
  }

  public static String replace(String text, CharSequence searchString, CharSequence replacement)
  {
    return isEmpty(text) || isEmpty(searchString) ? text : text.replace(searchString, replacement);
  }

  /**
   * Removes all occurrences of character sequence {@code remove} from string {@code cs}.
   *
   * @param cs     the character sequence to remove from
   * @param remove the character sequence to remove
   * @return modified input
   */
  public static String remove(CharSequence cs, CharSequence remove)
  {
    if (cs == null) {
      return null;
    }
    int len = cs.length();
    if (len == 0) {
      return "";
    } else if (isEmpty(remove) || remove.length() > len) {
      return cs.toString();
    }
    return cs.toString().replace(remove, "");
  }

  /**
   * Generates a string representation of {@code obj} using reflection on its
   * non-static declared fields.
   * 
   * @param obj           object to generate string from
   * @param longClassName use full class name if {@code true} or simple name if
   *                      {@code false}
   * @param hashCode      include the object's hash code if {@code true}
   * @return string representation
   */
  public static String reflectionToString(Object obj, boolean longClassName, boolean hashCode)
  {
    if (obj == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder(longClassName ? obj.getClass().getName() : obj.getClass().getSimpleName());
    if (hashCode) {
      sb.append('@').append(Integer.toHexString(System.identityHashCode(obj)));
    }
    sb.append('[');
    AtomicBoolean firstField = new AtomicBoolean(true);
    Arrays.stream(obj.getClass().getDeclaredFields())
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .sorted(Comparator.comparing(Field::getName))
        .forEach(f -> {
      f.setAccessible(true);
      if (!firstField.compareAndSet(true, false)) {
        sb.append(',');
      }
      sb.append(f.getName()).append('=');
      try {
        Object val = f.get(obj);
        sb.append(val == null ? "<null>" : val);
      } catch (Exception _ex) {
        sb.append('<').append(_ex).append('>');
      }
    });
    return sb.append(']').toString(); 
  }

  /**
   * Generates a string representation of {@code obj} using reflection on its
   * non-static declared fields using the object's full class name and including
   * the object's hash code.<br>
   * 
   * @param obj object to generate string from
   * @return string representation
   */
  public static String reflectionToString(Object obj) {
    return reflectionToString(obj, true, true);
  }

}
