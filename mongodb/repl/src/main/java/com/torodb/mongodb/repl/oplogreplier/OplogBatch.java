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
import com.torodb.mongodb.repl.RecoveryService;
import java.util.stream.Stream;

/**
 *
 */
public interface OplogBatch {

    /**
     * Returns the stream of operations contained by this batch.
     *
     * If {@link #isLastOne() } returns true, then this method will return an empty stream.
     *
     * @return
     */
    public Stream<OplogOperation> getOps();

    /**
     * Returns true if the {@link OplogFetcher} that created this batch thinks that there are more
     * elements that can be fetch right now from the remote oplog.
     * @return
     */
    public boolean isReadyForMore();

    /**
     * Returns true if this batch has been fetch after the producer {@link OplogFetcher} thinks it
     * has finished the remote oplog.
     *
     * This could happen when the fetcher was created with a given limit, which is usual on
     * the context of a {@link RecoveryService}, or when the fetcher is closed for any reason.
     *
     * If a batch returns true to this method, then it cannot contain any that, so {@link #getOps() }
     * will return an empty stream.
     *
     * @return
     */
    public boolean isLastOne();

    public default OplogBatch concat(OplogBatch next) {
        return new NormalOplogBatch(Stream.concat(getOps(), next.getOps()), next.isReadyForMore());
    }

}
