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

package com.torodb.kvdocument.values.heap;

import com.torodb.kvdocument.values.KvInstant;

import java.time.Instant;

/**
 *
 */
public class LongKvInstant extends KvInstant {

  private static final long serialVersionUID = 478419622836413091L;

  private final long millis;

  public LongKvInstant(long millis) {
    this.millis = millis;
  }

  @Override
  public long getMillisFromUnix() {
    return millis;
  }

  @Override
  public Instant getValue() {
    return Instant.ofEpochMilli(millis);
  }

}
