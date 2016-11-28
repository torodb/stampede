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

package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;

import java.time.Instant;

/**
 *
 */
public class OpTimeFactory {

  public OpTime getNextOpTime(OpTime optime) {
    BsonTimestamp ts = DefaultBsonValues.newTimestamp(optime.getTimestamp().getSecondsSinceEpoch()
        + 1, 0);
    return new OpTime(ts, optime.getTerm());
  }

  public OpTime newOpTime() {
    return OpTime.fromOldBson(DefaultBsonValues.newDateTime(Instant.now()));
  }

  public OpTime newOpTime(int secs) {
    return new OpTime(DefaultBsonValues.newTimestamp(secs, 0));
  }
}
