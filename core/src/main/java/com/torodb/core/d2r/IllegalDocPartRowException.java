/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.d2r;

import com.torodb.core.exceptions.SystemException;

/**
 *
 */
public class IllegalDocPartRowException extends SystemException {

    private static final long serialVersionUID = -4712840763476138823L;

    private final Integer did;
    private final Integer rid;
    private final Integer pid;
    private final Integer seq;

    public IllegalDocPartRowException(Integer did, Integer rid, Integer pid, Integer seq) {
        this.did = did;
        this.rid = rid;
        this.pid = pid;
        this.seq = seq;
    }

    public IllegalDocPartRowException(Integer did, Integer rid, Integer pid, Integer seq, String message) {
        super(message);
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
