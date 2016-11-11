/*
 * ToroDB - ToroDB-poc: Core
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.core.d2r;
public class InternalFields {
    public static final boolean CHILD_ARRAY_VALUE = true;
    public static final boolean CHILD_OBJECT_VALUE = !CHILD_ARRAY_VALUE;
    
    public final Integer did, rid, pid, seq;

    public InternalFields(Integer did, Integer rid, Integer pid, Integer seq) {
        super();
        this.did = did;
        this.rid = rid;
        this.pid = pid;
        this.seq = seq;
    }

    public Integer getDid() {
        return did;
    }

    public Integer getRid() {
        return rid;
    }

    public Integer getPid() {
        return pid;
    }

    public Integer getSeq() {
        return seq;
    }
}
