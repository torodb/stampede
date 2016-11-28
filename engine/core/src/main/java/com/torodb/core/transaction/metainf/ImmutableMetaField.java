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
public class ImmutableMetaField implements MetaField {

  private final String name;
  private final String identifier;
  private final FieldType type;

  public ImmutableMetaField(String name, String identifier, FieldType type) {
    this.name = name;
    this.identifier = identifier;
    this.type = type;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public FieldType getType() {
    return type;
  }

  @Override
  public String toString() {
    return defautToString();
  }

}
