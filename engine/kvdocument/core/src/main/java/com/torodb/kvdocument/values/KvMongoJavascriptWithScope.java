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

import javax.annotation.Nonnull;

public abstract class KvMongoJavascriptWithScope extends KvValue<KvMongoJavascriptWithScope> {
  private static final long serialVersionUID = 4130181266747513960L;

  @Nonnull
  @Override
  public KvMongoJavascriptWithScope getValue() {
    return this;
  }

  @Override
  public Class<? extends KvMongoJavascriptWithScope> getValueClass() {
    return KvMongoJavascriptWithScope.class;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvMongoJavascriptWithScope)) {
      return false;
    }
    return this.getValue().equals(((KvMongoJavascriptWithScope) obj).getValue());
  }

  public static KvMongoJavascriptWithScope of(String js, KvDocument scope) {
    return new DefaultKvMongoJavascriptWithScope(js, scope.toString());
  }

  public static KvMongoJavascriptWithScope of(String js, String scope) {
    return new DefaultKvMongoJavascriptWithScope(js, scope);
  }

  @Nonnull
  @Override
  public KvType getType() {
    return JavascriptWithScopeType.INSTANCE;
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  public abstract String getJs();

  public abstract String getScope();

  public abstract String getScopeAsString();

  private static class DefaultKvMongoJavascriptWithScope extends KvMongoJavascriptWithScope {

    private static final long serialVersionUID = 6900846950534864792L;
    private String js;

    private String scope;

    private DefaultKvMongoJavascriptWithScope(String js, String scope) {
      this.js = js;
      this.scope = scope;
    }

    @Override
    public String getJs() {
      return js;
    }

    @Override
    public String getScope() {
      return scope;
    }

    @Override
    public String getScopeAsString() {
      return scope;
    }

    @Override
    public String toString() {
      return js;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      DefaultKvMongoJavascriptWithScope that = (DefaultKvMongoJavascriptWithScope) o;

      if (js != null ? !js.equals(that.js) : that.js != null) {
        return false;
      }
      return scope != null ? scope.equals(that.scope) : that.scope == null;
    }

    @Override
    public int hashCode() {
      int result = js != null ? js.hashCode() : 0;
      result = 31 * result + (scope != null ? scope.hashCode() : 0);
      return result;
    }
  }
}
