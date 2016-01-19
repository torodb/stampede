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

import com.google.common.collect.ImmutableList;
import com.torodb.torod.core.subdocument.BasicType;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class BinaryValue implements Value<ImmutableList<Byte>> {

    private static final long serialVersionUID = 1L;

    private final ImmutableList<Byte> bytes;

    public BinaryValue(List<Byte> bytes) {
        this.bytes = ImmutableList.copyOf(bytes);
    }
    
    public BinaryValue(byte[] bytes) {
        ImmutableList.Builder<Byte> builder = ImmutableList.builder();

        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            builder.add(b);
        }
        this.bytes = builder.build();
    }

    @Override
    public ImmutableList<Byte> getValue() {
        return bytes;
    }

    @Override
    public BasicType getType() {
        return BasicType.BINARY;
    }

    /**
     * Returns an array that contains the same bytes that {@linkplain
     * #getValue()}.
     * <p>
     * Modifications in the given array won't affect the stored value
     * <p>
     * @return
     */
    public byte[] getArrayValue() {
        byte[] result = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) {
            result[i] = bytes.get(i);
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(24);
        for (Byte aByte : bytes) {
            sb.append(String.format("%02X", aByte));
        }
        return sb.toString();
    }

    public static BinaryValue parse(String string) {
        
        int len = string.length();
        ImmutableList.Builder builder = ImmutableList.builder();
        
        for (int i = 0; i < len; i += 2) {
            builder.add((byte) (
                        Character.digit(string.charAt(i), 16) * 16
                        + Character.digit(string.charAt(i + 1), 16)
                    )
            );
        }
        return new BinaryValue(builder.build());
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 47 * hash + (this.bytes != null ? this.bytes.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BinaryValue other = (BinaryValue) obj;
        if (this.bytes != other.bytes && (this.bytes == null
                                          || !this.bytes.equals(other.bytes))) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(
            ValueVisitor<Result, Arg> visitor,
            Arg arg) {
        return visitor.visit(this, arg);
    }

}
