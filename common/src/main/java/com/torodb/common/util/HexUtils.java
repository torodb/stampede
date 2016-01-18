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

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

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

}
