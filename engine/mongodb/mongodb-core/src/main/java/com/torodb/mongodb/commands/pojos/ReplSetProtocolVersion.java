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

package com.torodb.mongodb.commands.pojos;

public enum ReplSetProtocolVersion {
  V0(0),
  V1(1);

  private final long versionId;

  private ReplSetProtocolVersion(long versionId) {
    this.versionId = versionId;
  }

  public long getVersionId() {
    return versionId;
  }

  public static ReplSetProtocolVersion fromVersionId(long versionId) {
    for (ReplSetProtocolVersion value : ReplSetProtocolVersion.values()) {
      if (value.versionId == versionId) {
        return value;
      }
    }
    throw new IllegalArgumentException(
        "There is no replica set protocol version whose version id "
        + "is " + versionId
    );
  }
}
