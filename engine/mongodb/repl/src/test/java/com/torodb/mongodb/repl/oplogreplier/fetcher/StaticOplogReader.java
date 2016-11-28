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

package com.torodb.mongodb.repl.oplogreplier.fetcher;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.OplogOperationUnsupported;
import com.eightkdata.mongowp.exceptions.OplogStartMissingException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.pojos.IteratorMongoCursor;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.OplogReader;

import java.util.*;
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
  public MongoCursor<OplogOperation> queryGte(OpTime lastFetchedOpTime) throws MongoException {
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
  public void close() {
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public MongoCursor<OplogOperation> between(OpTime from, boolean includeFrom, OpTime to,
      boolean includeTo)
      throws OplogStartMissingException, OplogOperationUnsupported, MongoException {
    Preconditions.checkState(!closed);

    Iterator<OplogOperation> iterator = getBetweenIterator(from, includeFrom, to, includeTo);

    return new IteratorMongoCursor<>(DATABASE, COLLECTION, idProvider.incrementAndGet(), hostAndPort,
        iterator);
  }

  private Iterator<OplogOperation> getBetweenIterator(OpTime from, boolean includeFrom, OpTime to,
      boolean includeTo) {
    OpTime includedFrom;
    OpTime excludedTo;

    if (includeFrom || !oplog.containsKey(from)) {
      includedFrom = from;
    } else { //_from_ is excluded, but subMap includes it!
      SortedMap<OpTime, OplogOperation> tailMap = oplog.tailMap(from);
      if (tailMap.size() > 1) {
        includedFrom = tailMap.keySet().iterator().next();
      } else { //the _from_ key is the only key greater or equal than _from_ and we want to exclude it
        return Collections.emptyIterator();
      }
    }

    Iterator<OplogOperation> excludingIt = oplog.subMap(includedFrom, to)
        .values()
        .iterator();
    if (includeTo) {
      OplogOperation toOp = oplog.get(to);
      if (toOp != null) {
        return Iterators.concat(excludingIt, Collections.singleton(toOp).iterator());
      }
    }
    return excludingIt;
  }

}
