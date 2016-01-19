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

import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HexUtilsTest {
    private static byte[] bytes = new byte[1000];

    @BeforeClass
    public static void generateBytes() {
        for(int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
    }

    @Test
    public void bytes2HexArray() {
        String expected = DatatypeConverter.printHexBinary(bytes);
        String result = HexUtils.bytes2Hex(bytes);

        assertEquals(expected, result);
    }

    @Test
    public void bytes2HexCollection() {
        String expected = DatatypeConverter.printHexBinary(bytes);

        List<Byte> collectionBytes = new ArrayList<>(bytes.length);
        for(byte b : bytes) {
            collectionBytes.add(b);
        }
        String result = HexUtils.bytes2Hex(collectionBytes);

        assertEquals(expected, result);
    }
}
