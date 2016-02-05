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

package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.KVType;
import java.io.Serializable;
import javax.annotation.Nonnull;

/**
 *
 */
public abstract class KVValue<V> implements Serializable {

    private static final long serialVersionUID = -2695420893710884567L;
    
    KVValue() {}

    @Nonnull
    public abstract V getValue();

    @Nonnull
    public abstract Class<? extends V> getValueClass();

    @Nonnull
    public abstract KVType getType();

    @Override
    @Nonnull
    public abstract String toString();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    public abstract <Result, Arg> Result accept(KVValueVisitor<Result, Arg> visitor, Arg arg);

}
