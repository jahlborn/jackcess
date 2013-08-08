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

import java.util.List;

/**
 * Information about a relationship between two tables in the {@link
 * Database}.
 *
 * @author James Ahlborn
 */
public interface Relationship 
{
  public String getName();
  
  public Table getFromTable();

  public List<Column> getFromColumns();

  public Table getToTable();

  public List<Column> getToColumns();

  public boolean isOneToOne();

  public boolean hasReferentialIntegrity();

  public boolean cascadeUpdates();
  
  public boolean cascadeDeletes();

  public boolean isLeftOuterJoin();

  public boolean isRightOuterJoin();
}
