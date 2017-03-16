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

import com.torodb.kvdocument.types.DeprecatedType;
import com.torodb.kvdocument.types.KvType;

import javax.annotation.Nonnull;

public abstract class KvDeprecated extends KvValue<String> {

  private static final long serialVersionUID = -628511849455517129L;

  public static KvDeprecated of(String s) {
    return new DefaultKvDeprecated(s);
  }

  @Nonnull
  @Override
  public KvType getType() {
    return DeprecatedType.INSTANCE;
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  @Override
  public Class<? extends String> getValueClass() {
    return String.class;
  }

  @Override
  public String toString() {
    return getValue();
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvDeprecated)) {
      return false;
    }
    return this.getValue().equals(((KvDeprecated) obj).getValue());
  }

  private static class DefaultKvDeprecated extends KvDeprecated {

    private static final long serialVersionUID = -441679709442130566L;

    @Nonnull
    @Override
    public String getValue() {
      return value;
    }

    private String value;

    public DefaultKvDeprecated(String value) {
      this.value = value;
    }
  }
}
