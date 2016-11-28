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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.primitives.UnsignedBytes;

import java.util.Collection;

import javax.annotation.Nonnull;

/**
 *
 */
public class OctetUtils {

  private static final char[] BYTE_OCTET_VALUES = new char[256 * 3];

  static {
    for (int i = 0; i < 256; i++) {
      int high = i >>> 6;
      int middle = (i >>> 3) & 0x7;
      int low = i & 0x07;
      BYTE_OCTET_VALUES[i * 3] = (char) ('0' + high);
      BYTE_OCTET_VALUES[i * 3 + 1] = (char) ('0' + middle);
      BYTE_OCTET_VALUES[i * 3 + 2] = (char) ('0' + low);
    }
  }

  public static String bytes2Octet(@Nonnull byte[] bytes) {
    checkNotNull(bytes, "bytes");

    final int length = bytes.length;
    final char[] chars = new char[length * 4];
    int index;
    int charPos = 0;
    for (int i = 0; i < length; i++) {
      index = (bytes[i] & 0xFF) * 3;
      chars[charPos++] = '\\';
      chars[charPos++] = BYTE_OCTET_VALUES[index++];
      chars[charPos++] = BYTE_OCTET_VALUES[index++];
      chars[charPos++] = BYTE_OCTET_VALUES[index];
    }

    return new String(chars);
  }

  public static String bytes2Octet(@Nonnull Collection<Byte> bytes) {
    checkNotNull(bytes, "bytes");

    final int length = bytes.size();
    final char[] chars = new char[length * 3];
    int index;
    int charPos = 0;
    for (Byte b : bytes) {
      index = (b & 0xFF) * 3;
      chars[charPos++] = '\\';
      chars[charPos++] = BYTE_OCTET_VALUES[index++];
      chars[charPos++] = BYTE_OCTET_VALUES[index++];
      chars[charPos++] = BYTE_OCTET_VALUES[index];
    }

    return new String(chars);
  }

  public static void bytes2Octet(@Nonnull byte[] bytes, StringBuilder output) {
    checkNotNull(bytes, "bytes");

    int index;
    for (Byte b : bytes) {
      index = (b & 0xFF) * 3;
      output.append('\\');
      output.append(BYTE_OCTET_VALUES[index++]);
      output.append(BYTE_OCTET_VALUES[index++]);
      output.append(BYTE_OCTET_VALUES[index]);
    }
  }

  public static byte[] octet2Bytes(@Nonnull String value) {
    checkNotNull(value);

    int r = 0;
    byte[] result = new byte[value.length() / 5];
    for (int i = 0; i < value.length(); i += 5) {
      if (value.charAt(i) == '\\') {
        String substring = value.substring(i + 2, i + 5);
        assert substring.length() == 3;
        result[r] = UnsignedBytes.parseUnsignedByte(substring, 8);
      } else {
        assert value.charAt(i) < 256;
        result[r] = (byte) value.charAt(i);
      }

      r++;
    }
    return result;
  }
}
