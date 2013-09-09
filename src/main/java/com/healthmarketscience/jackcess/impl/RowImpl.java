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

package com.healthmarketscience.jackcess.impl;

import java.util.LinkedHashMap;

import com.healthmarketscience.jackcess.Row;

/**
 * A row of data as column->value pairs.
 * </p>
 * Note that the {@link #equals} and {@link #hashCode} methods work on the row
 * contents <i>only</i> (i.e. they ignore the id).
 *
 * @author James Ahlborn
 */
public class RowImpl extends LinkedHashMap<String,Object> implements Row
{
  private static final long serialVersionUID = 20130314L;  

  private final RowIdImpl _id;

  public RowImpl(RowIdImpl id) 
  {
    _id = id;
  }

  public RowImpl(RowIdImpl id, int expectedSize) 
  {
    super(expectedSize);
    _id = id;
  }

  public RowImpl(Row row) 
  {
    super(row);
    _id = (RowIdImpl)row.getId();
  }

  public RowIdImpl getId() {
    return _id;
  }

  @Override
  public String toString() {
    return CustomToStringStyle.valueBuilder("Row[" + _id + "]")
      .append(null, this)
      .toString();
  }
}
