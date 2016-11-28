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

public enum MemberState {

  /**
   * serving still starting up, or still trying to initiate the set.
   */
  RS_STARTUP(0),
  /**
   * this server thinks it is primary.
   */
  RS_PRIMARY(1),
  /**
   * this server thinks it is a secondary (slave mode).
   */
  RS_SECONDARY(2),
  /**
   * recovering/resyncing; after recovery usually auto-transitions to secondary.
   */
  RS_RECOVERING(3),
  /**
   * loaded config, still determining who is primary.
   */
  RS_STARTUP2(5),
  /**
   * remote node not yet reached
   */
  RS_UNKNOWN(6),
  RS_ARBITER(7),
  /**
   * node not reachable for a report
   */
  RS_DOWN(8),
  RS_ROLLBACK(9),
  /**
   * node removed from replica set
   */
  RS_REMOVED(10);

  private final int id;

  private MemberState(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public static MemberState fromId(int id) throws IllegalArgumentException {
    for (MemberState value : MemberState.values()) {
      if (value.id == id) {
        return value;
      }
    }
    throw new IllegalArgumentException("There is no member state whose id is equal to '"
        + id + "'");
  }

  public boolean isReadable() {
    return this == RS_PRIMARY || this == RS_SECONDARY;
  }

  public static int getMaxId() {
    return values()[values().length - 1].id;
  }
}
