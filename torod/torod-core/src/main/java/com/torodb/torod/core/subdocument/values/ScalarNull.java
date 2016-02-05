/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */


package com.torodb.torod.core.subdocument.values;

import com.torodb.torod.core.subdocument.ScalarType;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ScalarNull extends ScalarValue<ScalarNull> {

    private static final long serialVersionUID = 4583557874141119051L;

    private ScalarNull() {
    }

    public static ScalarNull getInstance() {
        return ScalarNullHolder.INSTANCE;
    }

    @Override
    public ScalarNull getValue() {
        return this;
    }

    @Override
    public Class<? extends ScalarNull> getValueClass() {
        return ScalarNull.class;
    }

    @Override
    public ScalarType getType() {
        return ScalarType.NULL;
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj != null && obj instanceof ScalarNull;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public <Result, Arg> Result accept(ScalarValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    private static class ScalarNullHolder {
        private static final ScalarNull INSTANCE = new ScalarNull();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve()  {
        return ScalarNull.getInstance();
    }
 }
