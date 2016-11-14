/*
 * ToroDB - KVDocument: Core
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.NullType;

/**
 *
 */
public class KVNull extends KVValue<KVNull> {

    private static final long serialVersionUID = 4583557874141119051L;

    private KVNull() {
    }

    public static KVNull getInstance() {
        return KVNullHolder.INSTANCE;
    }

    @Override
    public KVNull getValue() {
        return this;
    }

    @Override
    public Class<? extends KVNull> getValueClass() {
        return KVNull.class;
    }

    @Override
    public NullType getType() {
        return NullType.INSTANCE;
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj != null && obj instanceof KVNull;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public <Result, Arg> Result accept(KVValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    private static class KVNullHolder {
        private static final KVNull INSTANCE = new KVNull();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve()  {
        return KVNull.getInstance();
    }
 }
