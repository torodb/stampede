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
import java.util.List;

/**
 *
 */
public class NormalOplogBatch implements OplogBatch {

    private final List<OplogOperation> ops;
    private boolean readyForMore;

    public NormalOplogBatch(List<OplogOperation> ops, boolean readyForMore) {
        this.ops = ops;
        this.readyForMore = readyForMore;
    }

    @Override
    public List<OplogOperation> getOps() {
        return ops;
    }

    @Override
    public boolean isReadyForMore() {
        return readyForMore;
    }

    @Override
    public boolean isLastOne() {
        return false;
    }

}
