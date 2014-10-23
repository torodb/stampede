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
public class TwelveBytesValue implements Value<ImmutableList<Byte>> {

    private static final long serialVersionUID = 1L;

    private final ImmutableList<Byte> bytes;

    public TwelveBytesValue(List<Byte> bytes) {
        if (bytes.size() != 12) {
            throw new IllegalArgumentException(
                    bytes + " is not a valid twelve "
                            + "bytes array. It has "+bytes.size()+" "
                            + "bytes instead of 12"
            );
        }
        this.bytes = ImmutableList.copyOf(bytes);
    }
    
    public TwelveBytesValue(byte[] bytes) {
        if (bytes.length != 12) {
            throw new IllegalArgumentException(
                    Arrays.toString(bytes) + " is not a valid twelve "
                            + "bytes array. It has "+bytes.length+" "
                            + "bytes instead of 12"
            );
        }
        this.bytes = ImmutableList.of(
                bytes[0],
                bytes[1], 
                bytes[2], 
                bytes[3], 
                bytes[4], 
                bytes[5], 
                bytes[6], 
                bytes[7], 
                bytes[8], 
                bytes[9], 
                bytes[10], 
                bytes[11]
        );
    }

    @Override
    public ImmutableList<Byte> getValue() {
        return bytes;
    }

    @Override
    public BasicType getType() {
        return BasicType.TWELVE_BYTES;
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
        byte[] result = new byte[12];
        result[0] = bytes.get(0);
        result[1] = bytes.get(1);
        result[2] = bytes.get(2);
        result[3] = bytes.get(3);
        result[4] = bytes.get(4);
        result[5] = bytes.get(5);
        result[6] = bytes.get(6);
        result[7] = bytes.get(7);
        result[8] = bytes.get(8);
        result[9] = bytes.get(9);
        result[10] = bytes.get(10);
        result[11] = bytes.get(11);
        
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

    public static TwelveBytesValue parse(String string) {
        
        int len = string.length();
        ImmutableList.Builder builder = ImmutableList.builder();
        
        for (int i = 0; i < len; i += 2) {
            builder.add((byte) (
                        Character.digit(string.charAt(i), 16) * 16
                        + Character.digit(string.charAt(i + 1), 16)
                    )
            );
        }
        return new TwelveBytesValue(builder.build());
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
        final TwelveBytesValue other = (TwelveBytesValue) obj;
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
