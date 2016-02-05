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

import com.google.common.hash.Hashing;
import com.torodb.kvdocument.values.KVBinary.KVBinarySubtype;
import com.torodb.kvdocument.values.utils.NonIOByteSource;
import com.torodb.torod.core.subdocument.ScalarType;

/**
 *
 */
public abstract class ScalarBinary extends ScalarValue<ScalarBinary> {

    private static final long serialVersionUID = 1163196447549122350L;

    /**
     * Returns the ByteSource that contains the binary data.
     *
     * The associated ByteSource <b>shall not</b> throw IO exceptions for
     * @return
     */
    public abstract NonIOByteSource getByteSource();

    public abstract KVBinarySubtype getSubtype();

    /**
     * Returns the category on which the binary is encoded.
     *
     * The category is a 'subsubtype'. Most subtypes are going to accept just
     * one category, but others could can accept some of them. Two different
     * categories of the same subtype represents different encodecs with a
     * common subtype. For example, the subtype
     * {@linkplain ScalarBinarySubtype#MONGO_USER_DEFINED} have several user defined
     * categories.
     *
     * ToroDB is category-agnostic, which means it does not provide any special
     * functionality on categories. It just stores the value to be able to
     * return the specify user defined category when the ScalarBinary is retrieved.
     *
     * @return the category on which the binary is encoded.
     */
    public abstract byte getCategory();

    @Override
    public ScalarType getType() {
        return ScalarType.BINARY;
    }

    @Override
    public ScalarBinary getValue() {
        return this;
    }

    @Override
    public Class<? extends ScalarBinary> getValueClass() {
        return getClass();
    }

    public long size() {
        return getByteSource().size();
    }

    @Override
    public String toString() {
        return "ScalarBinary[subtype= " + getSubtype() + "(" + getCategory() + "), size= " + size() +"]";
    }

    @Override
    public int hashCode() {
        return Hashing.goodFastHash(32)
                .newHasher()
                .putInt(getSubtype().hashCode())
                .putLong(size())
                .hash()
                .asInt();
    }

    /**
     * Two ScalarBinary are equal if their subtypes are the same and
     * their contain the same bytes.
     *
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ScalarBinary)) {
            return false;
        }
        ScalarBinary other = (ScalarBinary) obj;
        if (this.getSubtype() != other.getSubtype()) {
            return false;
        }
        return this.getByteSource().contentEquals(other.getByteSource());
    }

    @Override
    public <Result, Arg> Result accept(ScalarValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

}
