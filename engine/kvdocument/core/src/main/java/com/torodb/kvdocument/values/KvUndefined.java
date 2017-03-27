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

import com.torodb.kvdocument.types.UndefinedType;

/** */
public class KvUndefined extends KvValue<KvUndefined> {

  private static final long serialVersionUID = 4583557874141119051L;

  private KvUndefined() {}

  public static KvUndefined getInstance() {
    return KvUndefinedHolder.INSTANCE;
  }

  @Override
  public KvUndefined getValue() {
    return this;
  }

  @Override
  public Class<? extends KvUndefined> getValueClass() {
    return KvUndefined.class;
  }

  @Override
  public UndefinedType getType() {
    return UndefinedType.INSTANCE;
  }

  @Override
  public String toString() {
    return "Undefined";
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || obj != null && obj instanceof KvUndefined;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  private static class KvUndefinedHolder {

    private static final KvUndefined INSTANCE = new KvUndefined();
  }

  //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
  private Object readResolve() {
    return KvUndefined.getInstance();
  }
}
