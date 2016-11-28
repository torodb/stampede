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

package com.torodb.packaging.config.util;

import org.junit.Assert;
import org.junit.Test;

public class SimpleRegExpDecoderTest {

  @Test
  public void decodeTest() {
    Assert
        .assertEquals("\\Qmine_and_your\\E", SimpleRegExpDecoder.decode("mine_and_your").pattern());
    Assert.assertEquals("\\Qmine_\\E.*\\Q_your\\E", SimpleRegExpDecoder.decode("mine_*_your")
        .pattern());
    Assert.assertEquals("\\Qmine_*_your\\E", SimpleRegExpDecoder.decode("mine_\\*_your").pattern());
    Assert.assertEquals("\\Qmine_\\\\E.*\\Q_your\\E", SimpleRegExpDecoder.decode(
        "min\\e_\\\\*_yo\\ur").pattern());
    Assert.assertEquals(".*\\Q_and_your\\E", SimpleRegExpDecoder.decode("*_and_your").pattern());
    Assert.assertEquals("\\Qmine_and_\\E.*", SimpleRegExpDecoder.decode("mine_and_*").pattern());
    Assert.assertEquals("\\Q*_and_your\\E", SimpleRegExpDecoder.decode("\\*_and_your").pattern());
    Assert.assertEquals("\\Qmine_and_*\\E", SimpleRegExpDecoder.decode("mine_and_\\*").pattern());
  }

}
