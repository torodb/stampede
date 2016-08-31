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

import com.torodb.mongodb.repl.oplogreplier.OplogBatch;
import com.torodb.mongodb.repl.oplogreplier.RollbackReplicationException;
import com.torodb.mongodb.repl.oplogreplier.StopReplicationException;

/**
 *
 */
public interface OplogFetcher extends AutoCloseable {

    /**
     * Fetchs a new batch.
     *
     * If fetcher has been finished or it is closed, a {@link OplogBatch#isLastOne() finished}
     * batch is returned. If the fetcher thinks it there are no more elements on the remote oplog
     * but more could be there in future, the returned batch will return false to
     * {@link OplogBatch#isReadyForMore()}.
     *
     * @return
     * @throws StopReplicationException
     * @throws RollbackReplicationException
     */
    public OplogBatch fetch() throws StopReplicationException, RollbackReplicationException;

    @Override
    public void close();

}
