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
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import org.jooq.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractStructureIT {

  private SqlInterface sqlInterface;

  private DatabaseTestContext dbTestContext;

  @Before
  public void setUp() throws Exception {
    dbTestContext = getDatabaseTestContext();
    sqlInterface = dbTestContext.getSqlInterface();
    dbTestContext.setupDatabase();
  }

  @After
  public void tearDown() throws Exception {
    dbTestContext.tearDownDatabase();
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

  @Test
  public void shouldCreateRootDocPartTable() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      sqlInterface.getStructureInterface().createSchema(dslContext, "schema_name");

      TableRefFactory tableRefFactory = new TableRefFactoryImpl();
      TableRef rootTableRef = tableRefFactory.createRoot();
      sqlInterface.getStructureInterface().createRootDocPartTable(dslContext,
          "schema_name", "table_name", rootTableRef);

      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (Statement foo = connection.createStatement()) {
        ResultSet result = foo.executeQuery("select * from \"schema_name\".\"table_name\"");

        assertTrue(result.getMetaData().getColumnCount() >= 1);
        assertTrue(containsColumn("did", result.getMetaData()));
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  private boolean containsColumn(String columnName, ResultSetMetaData metaData) throws SQLException {
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (columnName.equals(metaData.getColumnLabel(i)))
        return true;
    }

    return false;
  }

}
