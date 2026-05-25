/*
Copyright (c) 2025 James Ahlborn

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

package com.healthmarketscience.jackcess.impl;

import java.text.BreakIterator;
import java.util.Locale;

/**
 * String manipulation utilities.
 * @author James Ahlborn
 */
public final class StringUtil
{
  private StringUtil() {}

  public static boolean isEmpty(String str) {
    return ((str == null) || str.isEmpty());
  }

  public static boolean isBlank(String str) {
    if(isEmpty(str)) {
      return true;
    }
    int len = str.length();
    for (int i = 0; i < len; i++) {
      if (!Character.isWhitespace(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  public static String trimToNull(String str) {
    if(str != null) {
      str = str.trim();
    }
    return isEmpty(str) ? null : str;
  }

  public static String replace(String str, String target, String replacement) {
    return isEmpty(str) ? str : str.replace(target, replacement);
  }

  public static String remove(String str, char c) {
    if(isEmpty(str) || str.indexOf(c) < 0) {
      return str;
    }
    return str.replace(String.valueOf(c), "");
  }

  public static String capitalizeFully(String text, Locale locale) {
    if (isEmpty(text)) {
      return text;
    }

    BreakIterator wordIterator = BreakIterator.getWordInstance(locale);
    wordIterator.setText(text);

    StringBuilder sb = new StringBuilder(text.length());
    int start = wordIterator.first();

    for (int end = wordIterator.next(); end != BreakIterator.DONE;
         start = end, end = wordIterator.next()) {

      String word = text.substring(start, end);

      if (!Character.isLetterOrDigit(word.charAt(0))) {
        sb.append(word);
        continue;
      }

      int cp = word.codePointAt(0);

      String first = new String(Character.toChars(cp)).toUpperCase(locale);
      first = new String(Character.toChars(
                             Character.toTitleCase(first.codePointAt(0))));

      sb.append(first)
        .append(word.substring(Character.charCount(cp)).toLowerCase(locale));
    }

    return sb.toString();
  }
}
