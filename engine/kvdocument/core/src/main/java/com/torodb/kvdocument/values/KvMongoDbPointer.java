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

import com.torodb.kvdocument.types.JavascriptWithScopeType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.types.MongoDbPointerType;

import javax.annotation.Nonnull;

public abstract class KvMongoDbPointer extends KvValue<KvMongoDbPointer> {

  private static final long serialVersionUID = 4130181266747513960L;

  @Override
  public Class<? extends KvMongoDbPointer> getValueClass() {
    return KvMongoDbPointer.class;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvMongoDbPointer)) {
      return false;
    }
    return this.getValue().equals(((KvMongoDbPointer) obj).getValue());
  }

  public static KvMongoDbPointer of(String namespace, KvMongoObjectId id) {
    return new DefaultKvMongoDbPointer(namespace, id);
  }

  @Nonnull
  @Override
  public KvType getType() {
    return MongoDbPointerType.INSTANCE;
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  public abstract String getNamespace();

  public abstract KvMongoObjectId getId();

  private static class DefaultKvMongoDbPointer extends KvMongoDbPointer {

    private String namespace;

    private KvMongoObjectId id;

    @Override
    public String getNamespace() {
      return namespace;
    }

    @Override
    public KvMongoObjectId getId() {
      return id;
    }

    public DefaultKvMongoDbPointer(String namespace, KvMongoObjectId id) {
      this.namespace = namespace;
      this.id = id;
    }

    @Override
    public String toString() {
      return namespace;
    }

    @Override
    public int hashCode() {
      return namespace.hashCode();
    }

    @Nonnull
    @Override
    public KvMongoDbPointer getValue() {
      return this;
    }
  }
}
