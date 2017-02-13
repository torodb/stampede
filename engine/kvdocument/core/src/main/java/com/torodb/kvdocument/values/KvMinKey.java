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
import com.torodb.kvdocument.types.MinKeyType;

import javax.annotation.Nonnull;

public class KvMinKey extends KvValue<KvMinKey> {

    private static final long serialVersionUID = 4879628684225402030L;

    private KvMinKey(){}

    private static class KvMinKeyHolder {

        private static final KvMinKey INSTANCE = new KvMinKey();
    }

    public static KvMinKey getInstance(){
        return KvMinKeyHolder.INSTANCE;
    }

    @Nonnull
    @Override
    public KvMinKey getValue() {
        return this;
    }

    @Override
    public Class<? extends KvMinKey> getValueClass() {
        return KvMinKey.class;
    }

    @Override
    public String toString() {
        return "MinKey";
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj != null && obj instanceof KvMinKey;
    }

    @Nonnull
    @Override
    public KvType getType() {
        return MinKeyType.INSTANCE;
    }

    @Override
    public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
        return visitor.visit(this, arg);
    }
}
