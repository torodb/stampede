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


package com.torodb.common.util;

import com.google.common.primitives.UnsignedBytes;
import java.util.Collection;
import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.*;

/**
 *
 */
public class HexUtils {

    private static final char[] BYTE_HEX_VALUES = new char[256 << 1];
    static {
        for(int i = 0; i < 256; i++) {
            int high = i >>> 4;
            int low = i & 0x0F;
            BYTE_HEX_VALUES[i << 1] = high < 10 ? (char) ('0' + high) : (char) ('A' + high - 10);
            BYTE_HEX_VALUES[(i << 1) + 1] = low < 10 ? (char) ('0' + low) : (char) ('A' + low - 10);
        }
    }

    public static String bytes2Hex(@Nonnull byte[] bytes) {
        checkNotNull(bytes, "bytes");

        final int length = bytes.length;
        final char[] chars = new char[length << 1];
        int index;
        int charPos = 0;
        for(int i = 0; i < length; i++) {
            index = (bytes[i] & 0xFF) << 1;
            chars[charPos++] = BYTE_HEX_VALUES[index++];
            chars[charPos++] = BYTE_HEX_VALUES[index];
        }

        return new String(chars);
    }

    public static String bytes2Hex(@Nonnull Collection<Byte> bytes) {
        checkNotNull(bytes, "bytes");

        final int length = bytes.size();
        final char[] chars = new char[length << 1];
        int index;
        int charPos = 0;
        for(Byte b : bytes) {
            index = (b & 0xFF) << 1;
            chars[charPos++] = BYTE_HEX_VALUES[index++];
            chars[charPos++] = BYTE_HEX_VALUES[index];
        }

        return new String(chars);
    }

    public static void bytes2Hex(@Nonnull byte[] bytes, StringBuilder output) {
        checkNotNull(bytes, "bytes");

        int index;
        for(Byte b : bytes) {
            index = (b & 0xFF) << 1;
            output.append(BYTE_HEX_VALUES[index++]);
            output.append(BYTE_HEX_VALUES[index]);
        }
    }

    public static byte[] hex2Bytes(@Nonnull String value) {
        checkNotNull(value);
        checkArgument(value.length() % 2 == 0, "A string with a even lenght was expected");

        int r = 0;
        byte[] result = new byte[value.length() / 2];
        for (int i = 0; i < value.length(); i += 2) {
            assert r == i / 2;
            
            String substring = value.substring(i, i+2);
            assert substring.length() == 2;
            result[r] = UnsignedBytes.parseUnsignedByte(substring, 16);

            r++;
        }
        return result;
    }
}
