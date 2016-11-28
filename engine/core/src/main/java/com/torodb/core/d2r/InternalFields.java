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

package com.torodb.core.d2r;

public class InternalFields {
  public static final boolean CHILD_ARRAY_VALUE = true;
  public static final boolean CHILD_OBJECT_VALUE = !CHILD_ARRAY_VALUE;

  private final int did;
  private final int rid;
  private final int pid;
  private final int seq;

  public InternalFields(int did, int rid, int  pid, int  seq) {
    super();
    this.did = did;
    this.rid = rid;
    this.pid = pid;
    this.seq = seq;
  }

  public int getDid() {
    return did;
  }

  public int getRid() {
    return rid;
  }

  public int getPid() {
    return pid;
  }

  public int getSeq() {
    return seq;
  }
}
