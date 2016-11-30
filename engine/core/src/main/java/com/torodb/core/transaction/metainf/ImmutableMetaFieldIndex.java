/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.core.transaction.metainf;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ImmutableMetaFieldIndex implements MetaFieldIndex {

  private final int position;
  private final String name;
  private final FieldType type;
  private final FieldIndexOrdering ordering;

  public ImmutableMetaFieldIndex(int position, String name, FieldType type,
      FieldIndexOrdering ordering) {
    this.position = position;
    this.name = name;
    this.type = type;
    this.ordering = ordering;
  }

  @Override
  public int getPosition() {
    return position;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public FieldType getType() {
    return type;
  }

  @Override
  public FieldIndexOrdering getOrdering() {
    return ordering;
  }

  @Override
  public String toString() {
    return defautToString();
  }

}
