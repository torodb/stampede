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

package com.torodb.core.logging;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.Message;

import java.util.SortedMap;
import java.util.TreeMap;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public final class ToroMessage extends MapMessage {

  private static final long serialVersionUID = 37541345890239492L;
  private final Message msg;

  public ToroMessage(Message msg) {
    this(msg, new TreeMap<>());
  }

  public ToroMessage(Message msg, SortedMap<String, String> map) {
    super(map);
    this.msg = msg;
  }

  @Override
  public String asString() {
    return msg.getFormattedMessage();
  }

  @Override
  public String asString(String format) {
    if (format == null) {
      return asString();
    } else {
      return super.asString(format);
    }
  }

}
