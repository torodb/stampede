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
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.torodb.core.exceptions.ToroRuntimeException;

/**
 *
 */
public class UnexpectedOplogOperationException extends ToroRuntimeException {

    private static final long serialVersionUID = 2106842711139387929L;

    private final OplogOperation oplogOp;

    public UnexpectedOplogOperationException(OplogOperation oplogOp) {
        this.oplogOp = oplogOp;
    }

    public UnexpectedOplogOperationException(OplogOperation oplogOp, String message) {
        super(message);
        this.oplogOp = oplogOp;
    }

    public OplogOperation getOplogOp() {
        return oplogOp;
    }

}
