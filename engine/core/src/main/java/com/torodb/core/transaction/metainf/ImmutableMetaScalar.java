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

import java.util.Objects;

/**
 *
 */
public class ImmutableMetaScalar implements MetaScalar {

  private final FieldType type;
  private final String identifier;

  public ImmutableMetaScalar(String identifier, FieldType type) {
    this.type = type;
    this.identifier = identifier;
  }

  @Override
  public FieldType getType() {
    return type;
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 97 * hash + Objects.hashCode(this.type);
    hash = 97 * hash + Objects.hashCode(this.identifier);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ImmutableMetaScalar other = (ImmutableMetaScalar) obj;
    if (!Objects.equals(this.identifier, other.identifier)) {
      return false;
    }
    if (this.type != other.type) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return defautToString();
  }

}
