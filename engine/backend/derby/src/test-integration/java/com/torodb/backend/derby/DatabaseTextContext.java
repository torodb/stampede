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

package com.torodb.backend.derby;

import com.torodb.backend.DslContextFactory;
import com.torodb.backend.SqlInterface;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;

public interface DatabaseTextContext {

  public void setupDatabase() throws SQLException;

  public void tearDownDatabase();

  public SqlInterface getSqlInterface();

  public DslContextFactory getDslContextFactory();

  public default void executeOnDbConnectionWithDslContext(Consumer<DSLContext> consumer) throws SQLException {
    try (Connection connection = getSqlInterface().getDbBackend().createWriteConnection()) {
      DSLContext dslContext = getDslContextFactory().createDslContext(connection);

      consumer.accept(dslContext);

      connection.commit();
    }
  }

}
