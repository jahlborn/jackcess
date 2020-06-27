/*
Copyright (c) 2020 James Ahlborn

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

package com.healthmarketscience.jackcess.util;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.RuntimeIOException;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.expr.Expressionator;

/**
 * Predicate which tests a column value against a {@link Pattern}.  The static
 * factory methods can be used to construct the Pattern from various forms of
 * wildcard pattern syntaxes.
 *
 * This class can be used as a value pattern in the various Cursor search
 * methods, e.g. {@link com.healthmarketscience.jackcess.Cursor#findFirstRow(com.healthmarketscience.jackcess.Column,Object)}.
 *
 * @author James Ahlborn
 */
public class PatternColumnPredicate implements Predicate<Object>
{
  private static final int LIKE_REGEX_FLAGS = Pattern.DOTALL;
  private static final int CI_LIKE_REGEX_FLAGS =
    LIKE_REGEX_FLAGS | Pattern.CASE_INSENSITIVE |
    Pattern.UNICODE_CASE;

  private final Pattern _pattern;

  public PatternColumnPredicate(Pattern pattern) {
    _pattern = pattern;
  }

  @Override
  public boolean test(Object value) {
    try {
      // convert column value to string
      CharSequence cs = ColumnImpl.toCharSequence(value);

      return _pattern.matcher(cs).matches();
    } catch(IOException e) {
      throw new RuntimeIOException("Could not coerece column value to string", e);
    }
  }

  private static Pattern sqlLikeToRegex(
      String value, boolean caseInsensitive)
  {
    StringBuilder sb = new StringBuilder(value.length());

    for(int i = 0; i < value.length(); ++i) {
      char c = value.charAt(i);

      if(c == '%') {
        sb.append(".*");
      } else if(c == '_') {
        sb.append('.');
      } else if(c == '\\') {
        if(i + 1 < value.length()) {
          appendLiteralChar(sb, value.charAt(++i));
        }
      } else {
        appendLiteralChar(sb, c);
      }
    }

    int flags = (caseInsensitive ? CI_LIKE_REGEX_FLAGS : LIKE_REGEX_FLAGS);
    return Pattern.compile(sb.toString(), flags);
  }

  private static void appendLiteralChar(StringBuilder sb, char c) {
    if(Expressionator.isRegexSpecialChar(c)) {
      sb.append('\\');
    }
    sb.append(c);
  }

  /**
   * @return a PatternColumnPredicate which tests values against the given ms
   *         access wildcard pattern (always case insensitive)
   */
  public static PatternColumnPredicate forAccessLike(String pattern) {
    return new PatternColumnPredicate(Expressionator.likePatternToRegex(pattern));
  }

  /**
   * @return a PatternColumnPredicate which tests values against the given sql
   *         like pattern (supports escape char '\')
   */
  public static PatternColumnPredicate forSqlLike(String pattern) {
    return forSqlLike(pattern, false);
  }

  /**
   * @return a PatternColumnPredicate which tests values against the given sql
   *         like pattern (supports escape char '\'), optionally case
   *         insensitive
   */
  public static PatternColumnPredicate forSqlLike(
      String pattern, boolean caseInsensitive) {
    return new PatternColumnPredicate(sqlLikeToRegex(pattern, caseInsensitive));
  }

  /**
   * @return a PatternColumnPredicate which tests values against the given
   *         java regex pattern
   */
  public static PatternColumnPredicate forJavaRegex(String pattern) {
    return new PatternColumnPredicate(Pattern.compile(pattern));
  }
}
