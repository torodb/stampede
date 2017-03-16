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

import com.torodb.kvdocument.types.JavascriptType;
import com.torodb.kvdocument.types.KvType;

import javax.annotation.Nonnull;

public abstract class KvMongoJavascript extends KvValue<String> {

  private static final long serialVersionUID = -628511849455517129L;

  public static KvMongoJavascript of(String s) {
    return new DefaultKvMongoJavascript(s);
  }

  @Nonnull
  @Override
  public KvType getType() {
    return JavascriptType.INSTANCE;
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
    if (!(obj instanceof KvMongoJavascript)) {
      return false;
    }
    return this.getValue().equals(((KvMongoJavascript) obj).getValue());
  }

  private static class DefaultKvMongoJavascript extends KvMongoJavascript {

    private static final long serialVersionUID = -5704502002532582458L;

    private String value;

    @Nonnull
    @Override
    public String getValue() {
      return value;
    }

    public DefaultKvMongoJavascript(String value) {
      this.value = value;
    }
  }
}
