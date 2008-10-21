/*
Copyright (c) 2008 Health Market Science, Inc.

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

You can contact Health Market Science at info@healthmarketscience.com
or at the following address:

Health Market Science
2700 Horizon Drive
Suite 200
King of Prussia, PA 19406
*/

package com.healthmarketscience.jackcess;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;


/**
 * The RowFilter class encapsulates a filter test for a table row.  This can
 * be used by the {@link #apply(Iterable)} method to create an Iterable over a
 * table which returns only rows matching some criteria.
 * 
 * @author Patricia Donaldson, Xerox Corporation
 */
public abstract class RowFilter
{

  /**
   * Returns {@code true} if the given table row matches the Filter criteria,
   * {@code false} otherwise.
   * @param row current row to test for inclusion in the filter
   */
  public abstract boolean matches(Map<String, Object> row);

  /**
   * Returns an iterable which filters the given iterable based on this
   * filter.
   *
   * @param iterable row iterable to filter
   *
   * @return a filtering iterable
   */
  public Iterable<Map<String, Object>> apply(
      Iterable<Map<String, Object>> iterable)
  {
    return new FilterIterable(iterable);
  }


  /**
   * Creates a filter based on a row pattern.
   * 
   * @param rowPattern Map from column names to the values to be matched.
   *                   A table row will match the target if
   *                   {@code ObjectUtil.equals(rowPattern.get(s), row.get(s))}
   *                   for all column names in the pattern map.
   * @return a filter which matches table rows which match the values in the
   *         row pattern
   */
  public static RowFilter matchPattern(final Map<String, Object> rowPattern) 
  {
    return new RowFilter() {
        @Override
        public boolean matches(Map<String, Object> row) 
        {
          for(Map.Entry<String,Object> e : rowPattern.entrySet()) {
            if(!ObjectUtils.equals(e.getValue(), row.get(e.getKey()))) {
              return false;
            }
          }
          return true;
        }
      };
  }

  /**
   * Creates a filter based on a single value row pattern.
   *
   * @param columnPattern column to be matched
   * @param valuePattern value to be matched.
   *                     A table row will match the target if
   *                     {@code ObjectUtil.equals(valuePattern, row.get(columnPattern.getName()))}.
   * @return a filter which matches table rows which match the value in the
   *         row pattern
   */
  public static RowFilter matchPattern(final Column columnPattern, final Object valuePattern) 
  {
    return new RowFilter() {
        @Override
        public boolean matches(Map<String, Object> row) 
        {
          return ObjectUtils.equals(valuePattern, row.get(columnPattern.getName()));
        }
      };
  }

  /**
   * Creates a filter which inverts the sense of the given filter (rows which
   * are matched by the given filter will not be matched by the returned
   * filter, and vice versa).
   *
   * @param filter filter which to invert
   *
   * @return a RowFilter which matches rows not matched by the given filter
   */
  public static RowFilter invert(final RowFilter filter)
  {
    return new RowFilter() {
        @Override
        public boolean matches(Map<String, Object> row) 
        {
          return !filter.matches(row);
        }
      };
  }


  /**
   * Returns an iterable which filters the given iterable based on the given
   * rowFilter.
   *
   * @param rowFilter the filter criteria, may be {@code null}
   * @param iterable row iterable to filter
   *
   * @return a filtering iterable (or the given iterable if a {@code null}
   *         filter was given)
   */
  public static Iterable<Map<String, Object>> apply(
      RowFilter rowFilter,
      Iterable<Map<String, Object>> iterable)
  {
    return((rowFilter != null) ? rowFilter.apply(iterable) : iterable);
  }


  /**
   * Iterable which creates a filtered view of a another row iterable.
   */
  private class FilterIterable implements Iterable<Map<String, Object>>
  {
    private final Iterable<Map<String, Object>> _iterable;

    private FilterIterable(Iterable<Map<String, Object>> iterable) 
    {
      _iterable = iterable;
    }


    /**
     * Returns an iterator which iterates through the rows of the underlying
     * iterable, returning only rows for which the {@link RowFilter#matches}
     * method returns {@code true}
     */
    public Iterator<Map<String, Object>> iterator() 
    {
      return new Iterator<Map<String, Object>>() {
        private final Iterator<Map<String, Object>> _iter =
          _iterable.iterator();
        private Map<String, Object> _next;

        public boolean hasNext() {
          while(_iter.hasNext()) {
            _next = _iter.next();
            if(RowFilter.this.matches(_next)) {
              return true;
            }
          }
          _next = null;
          return false;
        }

        public Map<String, Object> next() {
          return _next;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }

      };
    }

  }

}
