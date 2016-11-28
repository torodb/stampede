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

package com.torodb.backend;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.kvdocument.values.KvValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
@SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public abstract class AbstractWriteInterface implements WriteInterface {

  private static final Logger LOGGER = LogManager.getLogger(AbstractWriteInterface.class);

  private final MetaDataReadInterface metaDataReadInterface;
  private final ErrorHandler errorHandler;
  private final SqlHelper sqlHelper;

  public AbstractWriteInterface(MetaDataReadInterface metaDataReadInterface,
      ErrorHandler errorHandler,
      SqlHelper sqlHelper) {
    super();
    this.metaDataReadInterface = metaDataReadInterface;
    this.errorHandler = errorHandler;
    this.sqlHelper = sqlHelper;
  }

  @Override
  public long deleteCollectionDocParts(@Nonnull DSLContext dsl,
      @Nonnull String schemaName, @Nonnull MetaCollection metaCollection,
      @Nonnull Cursor<Integer> didCursor
  ) {
    Connection c = dsl.configuration().connectionProvider().acquire();
    try {
      int maxBatchSize = 100;
      long deleted = 0;

      while (didCursor.hasNext()) {
        Collection<Integer> dids = didCursor.getNextBatch(maxBatchSize);
        deleteCollectionDocParts(c, schemaName, metaCollection, dids);
        deleted += dids.size();
      }

      return deleted;
    } finally {
      dsl.configuration().connectionProvider().release(c);
    }
  }

  @Override
  public void deleteCollectionDocParts(@Nonnull DSLContext dsl,
      @Nonnull String schemaName, @Nonnull MetaCollection metaCollection,
      @Nonnull Collection<Integer> dids
  ) {
    Connection c = dsl.configuration().connectionProvider().acquire();
    try {
      deleteCollectionDocParts(c, schemaName, metaCollection, dids);
    } finally {
      dsl.configuration().connectionProvider().release(c);
    }
  }

  private void deleteCollectionDocParts(Connection c, String schemaName,
      MetaCollection metaCollection,
      Collection<Integer> dids) {
    Iterator<? extends MetaDocPart> iterator = metaCollection.streamContainedMetaDocParts()
        .sorted(TableRefComparator.MetaDocPart.DESC).iterator();
    while (iterator.hasNext()) {
      MetaDocPart metaDocPart = iterator.next();
      String statement = getDeleteDocPartsStatement(schemaName, metaDocPart.getIdentifier(), dids);

      sqlHelper.executeUpdate(c, statement, Context.DELETE);

      LOGGER.trace("Executed {}", statement);
    }
  }

  protected abstract String getDeleteDocPartsStatement(String schemaName, String tableName,
      Collection<Integer> dids);

  @Override
  public void insertDocPartData(DSLContext dsl, String schemaName, DocPartData docPartData) throws
      UserException {
    Iterator<DocPartRow> docPartRowIterator = docPartData.iterator();
    if (!docPartRowIterator.hasNext()) {
      return;
    }

    try {
      MetaDocPart metaDocPart = docPartData.getMetaDocPart();
      Iterator<MetaScalar> metaScalarIterator = docPartData.orderedMetaScalarIterator();
      Iterator<MetaField> metaFieldIterator = docPartData.orderedMetaFieldIterator();
      standardInsertDocPartData(dsl, schemaName, docPartData, metaDocPart, metaScalarIterator,
          metaFieldIterator, docPartRowIterator);
    } catch (DataAccessException ex) {
      throw errorHandler.handleUserException(Context.INSERT, ex);
    }
  }

  protected int getMaxBatchSize() {
    return 30;
  }

  protected void standardInsertDocPartData(DSLContext dsl, String schemaName,
      DocPartData docPartData, MetaDocPart metaDocPart,
      Iterator<MetaScalar> metaScalarIterator, Iterator<MetaField> metaFieldIterator,
      Iterator<DocPartRow> docPartRowIterator) throws UserException {
    final int maxBatchSize = getMaxBatchSize();
    Collection<InternalField<?>> internalFields = metaDataReadInterface.getInternalFields(
        metaDocPart);
    List<FieldType> fieldTypeList = new ArrayList<>();
    String statement = getInsertDocPartDataStatement(schemaName, metaDocPart, metaFieldIterator,
        metaScalarIterator,
        internalFields, fieldTypeList);
    assert assertFieldTypeListIsConsistent(docPartData, fieldTypeList) :
        "fieldTypeList should be an ordered list of FieldType"
        + " from MetaScalar and MetaField following the the ordering of "
        + "DocPartData.orderedMetaScalarIterator and DocPartData.orderedMetaFieldIterator";

    Connection connection = dsl.configuration().connectionProvider().acquire();
    try {
      try (PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
        int docCounter = 0;
        while (docPartRowIterator.hasNext()) {
          DocPartRow docPartRow = docPartRowIterator.next();
          docCounter++;
          int parameterIndex = 1;
          for (InternalField<?> internalField : internalFields) {
            internalField.set(preparedStatement, parameterIndex, docPartRow);
            parameterIndex++;
          }
          Iterator<FieldType> fieldTypeIterator = fieldTypeList.iterator();
          for (KvValue<?> value : docPartRow.getScalarValues()) {
            sqlHelper.setPreparedStatementNullableValue(
                preparedStatement, parameterIndex++,
                fieldTypeIterator.next(),
                value);
          }
          for (KvValue<?> value : docPartRow.getFieldValues()) {
            sqlHelper.setPreparedStatementNullableValue(
                preparedStatement, parameterIndex++,
                fieldTypeIterator.next(),
                value);
          }
          preparedStatement.addBatch();

          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Added to insert {}", preparedStatement.toString());
          }

          if (docCounter % maxBatchSize == 0 || !docPartRowIterator.hasNext()) {
            preparedStatement.executeBatch();

            LOGGER.trace("Insertion batch executed");
          }
        }
      }
    } catch (SQLException ex) {
      throw errorHandler.handleUserException(Context.INSERT, ex);
    } finally {
      dsl.configuration().connectionProvider().release(connection);
    }
  }

  protected abstract String getInsertDocPartDataStatement(
      String schemaName,
      MetaDocPart metaDocPart,
      Iterator<MetaField> metaFieldIterator,
      Iterator<MetaScalar> metaScalarIterator,
      Collection<InternalField<?>> internalFields,
      List<FieldType> fieldTypeList);

  private boolean assertFieldTypeListIsConsistent(DocPartData docPartData,
      List<FieldType> fieldTypeList) {
    Iterator<MetaScalar> metaScalarIterator = docPartData.orderedMetaScalarIterator();
    Iterator<MetaField> metaFieldIterator = docPartData.orderedMetaFieldIterator();
    Iterator<FieldType> fieldTypeIterator = fieldTypeList.iterator();
    while (metaScalarIterator.hasNext()) {
      if (!fieldTypeIterator.hasNext() || !metaScalarIterator.next().getType().equals(
          fieldTypeIterator.next())) {
        return false;
      }
    }
    while (metaFieldIterator.hasNext()) {
      if (!fieldTypeIterator.hasNext() || !metaFieldIterator.next().getType().equals(
          fieldTypeIterator.next())) {
        return false;
      }
    }
    return true;
  }
}
