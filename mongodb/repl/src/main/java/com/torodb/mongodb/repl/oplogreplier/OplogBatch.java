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

    public Stream<OplogOperation> getOps();

    /**
     * Returns true if the {@link OplogFetcher} that created this batch thinks that there are more
     * elements that can be fetch from the remote oplog.
     * @return
     */
    public boolean isReadyForMore();

    /**
     * Returns true if this batch is the last one of that the producer {@link OplogFetcher} will
     * fetch.
     *
     * This could happen when the fetcher was created with a given limit, which is usual on
     * the context of a {@link RecoveryService}.
     * @return
     */
    public boolean isFinished();

    public default OplogBatch concat(OplogBatch next) {
        return new NormalOplogBatch(Stream.concat(getOps(), next.getOps()), next.isReadyForMore());
    }

}
