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

package com.torodb.backend.tests.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.torodb.backend.SqlInterface;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.MetaDatabase;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractStructureIntegrationSuite {

  private static final String SCHEMA_NAME = "schema_name";

  private SqlInterface sqlInterface;

  private DatabaseTestContext dbTestContext;

  private TableRefFactory tableRefFactory = new TableRefFactoryImpl();

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

  protected abstract String getSqlTypeOf(FieldType fieldType);

  @Test
  public void shouldCreateSchema() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* When */
      createSchema(dslContext);

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (ResultSet resultSet = connection.getMetaData().getSchemas("%", SCHEMA_NAME)) {
        resultSet.next();

        assertEquals(SCHEMA_NAME, resultSet.getString("TABLE_SCHEM"));
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  @Test
  public void shouldCreateRootDocPartTable() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);

      /* When */
      createRootTable(dslContext, "root_table");

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (Statement foo = connection.createStatement()) {
        ResultSet result = foo.executeQuery("select * from \"schema_name\".\"root_table\"");

        assertThatColumnExists(result.getMetaData(), "did");
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  @Test
  public void shouldCreateDocPartTable() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);
      TableRef rootTableRef = createRootTable(dslContext, "root_table");

      /* When */
      createChildTable(dslContext, rootTableRef, "root_table", "child_table");

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (Statement foo = connection.createStatement()) {
        ResultSet result = foo.executeQuery("select * from \"schema_name\".\"child_table\"");

        assertThatColumnExists(result.getMetaData(), "did");
        assertThatColumnExists(result.getMetaData(), "rid");
        assertThatColumnExists(result.getMetaData(), "seq");
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  @Test
  public void shouldCreateSecondLevelDocPartTableWithPid() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /*Given */
      createSchema(dslContext);
      TableRef rootTableRef = createRootTable(dslContext, "root_table");
      TableRef childTableRef =
          createChildTable(dslContext, rootTableRef, "root_table", "child_table");

      /* When */
      createChildTable(dslContext, childTableRef, "child_table", "second_child_table");

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (Statement foo = connection.createStatement()) {
        ResultSet result = foo.executeQuery("select * from \"schema_name\".\"second_child_table\"");

        assertThatColumnExists(result.getMetaData(), "did");
        assertThatColumnExists(result.getMetaData(), "pid");
        assertThatColumnExists(result.getMetaData(), "rid");
        assertThatColumnExists(result.getMetaData(), "seq");
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  @Test
  public void shouldAddColumnsToExistingDocPartTable() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);
      createRootTable(dslContext, "root_table");

      /* When */
      DataTypeForKv<?> dataType = sqlInterface.getDataTypeProvider().getDataType(FieldType.STRING);
      sqlInterface.getStructureInterface()
          .addColumnToDocPartTable(dslContext, SCHEMA_NAME, "root_table", "new_column", dataType);

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (Statement foo = connection.createStatement()) {
        ResultSet result = foo.executeQuery("select * from \"schema_name\".\"root_table\"");

        assertThatColumnIsGivenType(result.getMetaData(), "new_column",
            getSqlTypeOf(FieldType.STRING));
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  @Test
  public void newColumnShouldSupportAnyGivenSupportedFieldType() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      createSchema(dslContext);
      createRootTable(dslContext, "root_table");

      Connection connection = dslContext.configuration().connectionProvider().acquire();

      for (FieldType fieldType : FieldType.values()) {
        DataTypeForKv<?> dataType = sqlInterface.getDataTypeProvider().getDataType(fieldType);
        String columnName = "new_column_" + fieldType.name();

        sqlInterface.getStructureInterface()
            .addColumnToDocPartTable(dslContext, SCHEMA_NAME, "root_table", columnName, dataType);

        try (Statement foo = connection.createStatement()) {
          ResultSet result = foo.executeQuery("select * from \"schema_name\".\"root_table\"");

          assertThatColumnIsGivenType(result.getMetaData(), columnName, getSqlTypeOf(fieldType));
        } catch (SQLException e) {
          throw new RuntimeException("Wrong test invocation", e);
        }
      }
    });
  }

  @Test
  public void databaseShouldBeDeleted() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      String collection = "root_table";

      createSchema(dslContext);
      createRootTable(dslContext, collection);

      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(tableRefFactory.createRoot(),"root_table").build();
      ImmutableMetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(collection, collection).put(metaDocPart).build();
      MetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder(SCHEMA_NAME, SCHEMA_NAME)
          .put(metaCollection).build();

      /* When */
      sqlInterface.getStructureInterface().dropDatabase(dslContext, metaDatabase);

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (ResultSet resultSet = connection.getMetaData().getSchemas("%", SCHEMA_NAME)) {
        assertFalse("Schema shouldn't exist", resultSet.next());
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  private void createSchema(DSLContext dslContext) {
    sqlInterface.getStructureInterface().createSchema(dslContext, SCHEMA_NAME);
  }

  private TableRef createRootTable(DSLContext dslContext, String rootName) {
    TableRef rootTableRef = tableRefFactory.createRoot();
    sqlInterface.getStructureInterface().createRootDocPartTable(dslContext,
        SCHEMA_NAME, rootName, rootTableRef);

    return rootTableRef;
  }

  private TableRef createChildTable(DSLContext dslContext, TableRef tableRef, String parentName,
                                    String childName) {

    TableRef childTableRef = tableRefFactory.createChild(tableRef, childName);
    sqlInterface.getStructureInterface().createDocPartTable(dslContext, SCHEMA_NAME,
        childName, childTableRef, parentName);

    return childTableRef;
  }

  private void assertThatColumnExists(ResultSetMetaData metaData, String columnName)
      throws SQLException {

    boolean findMatch = false;

    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (columnName.equals(metaData.getColumnLabel(i))) {
        findMatch = true;
      }
    }

    if (!findMatch) {
      assertTrue("Column " + columnName + " should exist", false);
    }
  }

  private void assertThatColumnIsGivenType(ResultSetMetaData metaData, String columnName,
                                           String requiredType) throws SQLException {

    boolean findMatch = false;

    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (columnName.equals(metaData.getColumnLabel(i))) {
        findMatch = true;
        assertEquals(requiredType, metaData.getColumnTypeName(i));
      }
    }

    if (!findMatch) {
      assertTrue("Column " + columnName + " should exist", false);
    }
  }

}
