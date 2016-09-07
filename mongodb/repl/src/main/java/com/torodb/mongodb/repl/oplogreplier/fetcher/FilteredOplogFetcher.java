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

package com.torodb.mongodb.repl.oplogreplier.fetcher;

import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.torodb.mongodb.repl.oplogreplier.*;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 *
 */
public class FilteredOplogFetcher implements OplogFetcher {

    private final Predicate<OplogOperation> filter;
    private final OplogFetcher delegate;

    public FilteredOplogFetcher(Predicate<OplogOperation> filter, OplogFetcher delegate) {
        this.filter = filter;
        this.delegate = delegate;
    }

    @Override
    public OplogBatch fetch() throws StopReplicationException, RollbackReplicationException {
        OplogBatch batch = delegate.fetch();

        List<OplogOperation> filteredBatch = batch.getOps().stream().filter(filter)
                .collect(Collectors.toList());

        if (filteredBatch.isEmpty()) {
            if (batch.isLastOne()) {
                return FinishedOplogBatch.getInstance();
            }
            else {
                return NotReadyForMoreOplogBatch.getInstance();
            }
        } else {
            if (batch.isLastOne()) {
                throw new AssertionError("Batchs produced by a finished oplog fetcher cannot "
                        + "contain ops");
            }
            return new NormalOplogBatch(filteredBatch, batch.isReadyForMore());
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

}
