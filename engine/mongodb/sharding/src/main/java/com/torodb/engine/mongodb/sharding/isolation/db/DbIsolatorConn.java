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

package com.torodb.engine.mongodb.sharding.isolation.db;

import com.torodb.engine.mongodb.sharding.isolation.ConnDecorator;
import com.torodb.torod.ExclusiveWriteTorodTransaction;
import com.torodb.torod.ReadOnlyTorodTransaction;
import com.torodb.torod.SharedWriteTorodTransaction;
import com.torodb.torod.TorodConnection;


public class DbIsolatorConn extends ConnDecorator<DbIsolatorServer> {

  public DbIsolatorConn(DbIsolatorServer server, TorodConnection decorated) {
    super(server, decorated);
  }

  final String convertDatabaseName(String dbName) {
    return getServer().convertDatabaseName(dbName);
  }

  final String convertIndexName(String indexName) {
    return getServer().convertIndexName(indexName);
  }

  final boolean isVisibleDatabase(String dbName) {
    return getServer().isVisibleDatabase(dbName);
  }

  @Override
  public ReadOnlyTorodTransaction openReadOnlyTransaction() {
    return new DbIsolatorReadTrans(this, getDecorated().openReadOnlyTransaction());
  }

  @Override
  public SharedWriteTorodTransaction openWriteTransaction(boolean concurrent) {
    return new DbIsolatorWriteTrans<>(this, getDecorated().openWriteTransaction(concurrent));
  }

  @Override
  public ExclusiveWriteTorodTransaction openExclusiveWriteTransaction(boolean concurrent) {
    return new DbIsolatorExclusiveWriteTrans(this, getDecorated().openExclusiveWriteTransaction(
        concurrent)
    );
  }

}
