/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.common.util;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

public class HexUtilsTest {

  private static byte[] bytes = new byte[1000];

  @BeforeClass
  public static void generateBytes() {
    for (int i = 0; i < bytes.length; i++) {
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
    for (byte b : bytes) {
      collectionBytes.add(b);
    }
    String result = HexUtils.bytes2Hex(collectionBytes);

    assertEquals(expected, result);
  }
}
