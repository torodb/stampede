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

package com.torodb.backend.meta;

import com.google.common.io.CharStreams;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.exceptions.ToroRuntimeException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Schema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public abstract class AbstractSchemaUpdater implements SchemaUpdater {

  private static final Logger LOGGER = LogManager.getLogger(AbstractSchemaUpdater.class);

  private final SqlInterface sqlInterface;
  private final SqlHelper sqlHelper;

  @Inject
  public AbstractSchemaUpdater(SqlInterface sqlInterface, SqlHelper sqlHelper) {
    this.sqlInterface = sqlInterface;
    this.sqlHelper = sqlHelper;
  }

  @Override
  public void checkOrCreate(
      DSLContext dsl,
      Meta jooqMeta) throws InvalidDatabaseException {
    Optional<Schema> torodbSchema = sqlInterface.getStructureInterface()
        .findTorodbSchema(dsl, jooqMeta);
    if (!torodbSchema.isPresent()) {
      LOGGER.info("Schema '{}' not found. Creating it...", TorodbSchema.IDENTIFIER);
      createSchema(dsl, sqlInterface, sqlHelper);
      LOGGER.info("Schema '{}' created", TorodbSchema.IDENTIFIER);
    } else {
      LOGGER.info("Schema '{}' found. Checking it...", TorodbSchema.IDENTIFIER);
      checkSchema(torodbSchema.get(), sqlInterface);
      LOGGER.info("Schema '{}' checked", TorodbSchema.IDENTIFIER);
    }
  }

  protected void createSchema(DSLContext dsl, SqlInterface sqlInterface, SqlHelper sqlHelper) {
    sqlInterface.getStructureInterface().createSchema(dsl, TorodbSchema.IDENTIFIER);
    sqlInterface.getMetaDataWriteInterface().createMetaDatabaseTable(dsl);
    sqlInterface.getMetaDataWriteInterface().createMetaCollectionTable(dsl);
    sqlInterface.getMetaDataWriteInterface().createMetaDocPartTable(dsl);
    sqlInterface.getMetaDataWriteInterface().createMetaFieldTable(dsl);
    sqlInterface.getMetaDataWriteInterface().createMetaScalarTable(dsl);
    sqlInterface.getMetaDataWriteInterface().createMetaIndexTable(dsl);
    sqlInterface.getMetaDataWriteInterface().createMetaIndexFieldTable(dsl);
    sqlInterface.getMetaDataWriteInterface().createMetaDocPartIndexTable(dsl);
    sqlInterface.getMetaDataWriteInterface().createMetaFieldIndexTable(dsl);
    sqlInterface.getMetaDataWriteInterface().createKvTable(dsl);
  }

  private void checkSchema(Schema torodbSchema, SqlInterface sqlInterface) throws
      InvalidDatabaseException {
    sqlInterface.getStructureInterface().checkMetaDataTables(torodbSchema);
  }

  @SuppressFBWarnings(value = "UI_INHERITANCE_UNSAFE_GETRESOURCE",
      justification = "We want to read resources from the subclass")
  protected void executeSql(
      DSLContext dsl,
      String resourcePath,
      SqlHelper sqlHelper
  ) {
    try (InputStream resourceAsStream =
        getClass().getResourceAsStream(resourcePath)) {
      if (resourceAsStream == null) {
        throw new SystemException(
            "Resource '" + resourcePath + "' does not exist"
        );
      }
      String statementAsString =
          CharStreams.toString(
              new BufferedReader(
                  new InputStreamReader(
                      resourceAsStream,
                      Charset.forName("UTF-8"))));
      sqlHelper.executeStatement(dsl, statementAsString, Context.UNKNOWN);
    } catch (IOException ex) {
      throw new ToroRuntimeException(ex);
    }
  }
}
