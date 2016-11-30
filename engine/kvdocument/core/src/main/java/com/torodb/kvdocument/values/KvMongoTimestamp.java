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

import com.torodb.kvdocument.types.MongoTimestampType;

public abstract class KvMongoTimestamp extends KvValue<KvMongoTimestamp> {

  private static final long serialVersionUID = 6199736068678561291L;

  public abstract int getSecondsSinceEpoch();

  public abstract int getOrdinal();

  @Override
  public KvMongoTimestamp getValue() {
    return this;
  }

  @Override
  public Class<? extends KvMongoTimestamp> getValueClass() {
    return getClass();
  }

  @Override
  public MongoTimestampType getType() {
    return MongoTimestampType.INSTANCE;
  }

  @Override
  public String toString() {
    return "Timestamp{"
        + "seconds:" + getSecondsSinceEpoch()
        + ", ordinal:" + getOrdinal()
        + '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvMongoTimestamp)) {
      return false;
    }
    KvMongoTimestamp other = (KvMongoTimestamp) obj;
    if (this.getSecondsSinceEpoch() != other.getSecondsSinceEpoch()) {
      return false;
    }
    return this.getOrdinal() == other.getOrdinal();
  }

  @Override
  public int hashCode() {
    return getSecondsSinceEpoch() << 4 | (getOrdinal() & 0xF);
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

}
