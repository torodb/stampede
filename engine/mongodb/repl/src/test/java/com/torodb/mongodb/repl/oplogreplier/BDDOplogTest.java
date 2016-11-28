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

package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.ReadOnlyMongodTransaction;
import com.torodb.mongodb.core.WriteMongodTransaction;

import java.util.stream.Stream;

/**
 *
 */
public abstract class BDDOplogTest implements OplogTest {

  public abstract void given(WriteMongodTransaction trans) throws Exception;

  public abstract Stream<OplogOperation> streamOplog();

  public abstract void then(ReadOnlyMongodTransaction trans) throws Exception;

  public abstract ApplierContext getApplierContext();

  @Override
  public void execute(OplogTestContext context) throws Exception {
    MongodServer server = context.getMongodServer();

    try (MongodConnection conn = server.openConnection()) {
      try (WriteMongodTransaction trans = conn.openWriteTransaction(true)) {
        given(trans);
        trans.commit();
      }
    }

    context.apply(streamOplog(), getApplierContext());

    try (MongodConnection conn = server.openConnection()) {
      try (ReadOnlyMongodTransaction trans = conn.openReadOnlyTransaction()) {
        then(trans);
      }
    }
  }

}
