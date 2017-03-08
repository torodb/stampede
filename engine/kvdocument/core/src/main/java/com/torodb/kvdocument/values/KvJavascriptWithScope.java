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

public abstract class KvJavascriptWithScope extends KvValue<KvJavascriptWithScope> {
    private static final long serialVersionUID = 4130181266747513960L;

    @Nonnull
    @Override
    public KvJavascriptWithScope getValue() {
        return this;
    }

    @Override
    public Class<? extends KvJavascriptWithScope> getValueClass() {
        return KvJavascriptWithScope.class;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof KvJavascriptWithScope)) {
            return false;
        }
        return this.getValue().equals(((KvJavascriptWithScope) obj).getValue());
    }

    public static KvJavascriptWithScope of(String js, KvDocument scope) {
        return new DefaultKvJavascriptWithScope(js, scope);
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

    public abstract KvDocument getScope();

    public abstract String getScopeAsString();


    private static class DefaultKvJavascriptWithScope extends KvJavascriptWithScope{

        private String js;

        private KvDocument scope;

        public DefaultKvJavascriptWithScope(String js, KvDocument scope) {
            this.js = js;
            this.scope = scope;
        }

        @Override
        public String getJs() {
            return js;
        }

        @Override
        public KvDocument getScope() {
            return scope;
        }

        @Override
        public String getScopeAsString() {
            return scope.toString();
        }


        @Override
        public String toString() {
            return js;
        }

        @Override
        public int hashCode() {
            return js.hashCode();
        }

    }
}
