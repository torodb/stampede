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

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.OplogOperationUnsupported;
import com.eightkdata.mongowp.exceptions.OplogStartMissingException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.pojos.IteratorMongoCursor;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.UnsignedInteger;
import com.torodb.mongodb.repl.OplogReader;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
public class StaticOplogReader implements OplogReader {
    private static final String DATABASE = "local";
    private static final String COLLECTION = "oplog.rs";

    private final HostAndPort hostAndPort;
    private final SortedMap<OpTime, OplogOperation> oplog;
    private final AtomicInteger idProvider = new AtomicInteger();
    private volatile boolean closed = false;

    public StaticOplogReader(Collection<OplogOperation> oplog) {
        this(HostAndPort.fromParts("localhost", 27017), oplog);
    }

    public StaticOplogReader(HostAndPort hostAndPort, Collection<OplogOperation> oplog) {
        this(hostAndPort, new TreeMap<>(
                oplog.stream().collect(Collectors.toMap(
                        o -> o.getOpTime(),
                        Function.identity()
                )))
        );
    }

    public StaticOplogReader(SortedMap<OpTime, OplogOperation> oplog) {
        this(HostAndPort.fromParts("localhost", 27017), oplog);
    }

    public StaticOplogReader(HostAndPort hostAndPort, SortedMap<OpTime, OplogOperation> oplog) {
        this.hostAndPort = hostAndPort;
        this.oplog = oplog;
    }

    @Override
    public HostAndPort getSyncSource() {
        return hostAndPort;
    }

    @Override
    public MongoCursor<OplogOperation> queryGTE(OpTime lastFetchedOpTime) throws MongoException {
        Preconditions.checkState(!closed);

        return new IteratorMongoCursor<>(DATABASE, COLLECTION, idProvider.incrementAndGet(), hostAndPort,
                oplog.tailMap(lastFetchedOpTime).values().iterator()
        );
    }

    @Override
    public OplogOperation getLastOp() throws OplogStartMissingException, OplogOperationUnsupported,
            MongoException {
        Preconditions.checkState(!closed);
        if (oplog.isEmpty()) {
            throw new OplogStartMissingException(hostAndPort);
        }
        return oplog.get(oplog.lastKey());
    }

    @Override
    public OplogOperation getFirstOp() throws OplogStartMissingException, OplogOperationUnsupported,
            MongoException {
        Preconditions.checkState(!closed);
        if (oplog.isEmpty()) {
            throw new OplogStartMissingException(hostAndPort);
        }
        return oplog.get(oplog.firstKey());
    }

    @Override
    public boolean shouldChangeSyncSource() {
        return false;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public MongoCursor<OplogOperation> between(OpTime from, boolean includeFrom, OpTime to, boolean includeTo)
            throws OplogStartMissingException, OplogOperationUnsupported, MongoException {
        Preconditions.checkState(!closed);

        OpTime includedFrom;
        OpTime excludedTo;
        if (includeFrom) {
            includedFrom = from;
        } else {
            includedFrom = new OpTime(from.getSecs().minus(UnsignedInteger.ONE), from.getTerm());
        }
        if (includeTo) {
            excludedTo = new OpTime(to.getSecs().plus(UnsignedInteger.ONE), to.getTerm());
        } else {
            excludedTo = to;
        }

        SortedMap<OpTime, OplogOperation> subMap = oplog.subMap(includedFrom, excludedTo);
        return new IteratorMongoCursor<>(DATABASE, COLLECTION, idProvider.incrementAndGet(), hostAndPort, subMap.values().iterator());
    }

}
