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

package com.torodb.backend.common;

import com.torodb.backend.SqlInterface;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public abstract class AbstractStructureIT {

  private SqlInterface sqlInterface;

  private DatabaseTestContext dbTestContext;

  @Before
  public void setUp() throws Exception {
    dbTestContext = getDatabaseTestContext();
    sqlInterface = dbTestContext.getSqlInterface();
    dbTestContext.setupDatabase();
  }

  protected abstract DatabaseTestContext getDatabaseTestContext();

  @Test
  public void shouldCreateSchema() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      sqlInterface.getStructureInterface().createSchema(dslContext, "schema_name");

      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (ResultSet resultSet = connection.getMetaData().getSchemas("%", "schema_name")) {
        resultSet.next();

        assertEquals("schema_name", resultSet.getString("TABLE_SCHEM"));
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

}
