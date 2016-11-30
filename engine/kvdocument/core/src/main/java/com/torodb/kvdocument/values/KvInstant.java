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

package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.InstantType;

import java.time.Instant;

public abstract class KvInstant extends KvValue<Instant> {

  private static final long serialVersionUID = 5680488951653964418L;

  @Override
  public InstantType getType() {
    return InstantType.INSTANCE;
  }

  public long getMillisFromUnix() {
    return getValue().toEpochMilli();
  }

  @Override
  public Class<? extends Instant> getValueClass() {
    return Instant.class;
  }

  @Override
  public String toString() {
    return getValue().toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvInstant)) {
      return false;
    }
    KvInstant other = (KvInstant) obj;
    return this.getMillisFromUnix() == other.getMillisFromUnix();
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

}
