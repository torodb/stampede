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

import com.torodb.common.util.HexUtils;
import com.torodb.kvdocument.types.MongoObjectIdType;
import java.util.Arrays;

/**
 *
 */
public abstract class KVMongoObjectId extends KVValue<KVMongoObjectId> {

    private static final long serialVersionUID = -2548014610816031841L;

    /**
     * Returns an array that contains the same bytes that {@linkplain
     * #getValue()}.
     * <p>
     * Modifications in the given array won't affect the stored value
     * <p>
     * @return
     */
    public abstract byte[] getArrayValue();

    protected byte[] getBytesUnsafe() {
        return getArrayValue();
    }

    @Override
    public KVMongoObjectId getValue() {
        return this;
    }

    @Override
    public Class<? extends KVMongoObjectId> getValueClass() {
        return getClass();
    }

    @Override
    public String toString() {
        return HexUtils.bytes2Hex(getArrayValue());
    }

    @Override
    public MongoObjectIdType getType() {
        return MongoObjectIdType.INSTANCE;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getBytesUnsafe());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof KVMongoObjectId)) {
            return false;
        }
        return Arrays.equals(this.getArrayValue(), ((KVMongoObjectId) obj).getArrayValue());
    }

    @Override
    public <Result, Arg> Result accept(KVValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

}
