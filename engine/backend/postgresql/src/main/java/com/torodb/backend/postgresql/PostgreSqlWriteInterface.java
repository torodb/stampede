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

package com.torodb.backend.postgresql;

import com.codahale.metrics.Timer;
import com.torodb.backend.AbstractWriteInterface;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.postgresql.converters.PostgreSqlValueToCopyConverter;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.kvdocument.values.KvValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PostgreSqlWriteInterface extends AbstractWriteInterface {

  private static final Logger LOGGER = LogManager.getLogger(PostgreSqlWriteInterface.class);

  private final PostgreSqlMetaDataReadInterface postgreSqlMetaDataReadInterface;
  private final ErrorHandler errorHandler;
  private final SqlHelper sqlHelper;
  private final PostgreSqlMetrics metrics;

  @Inject
  public PostgreSqlWriteInterface(PostgreSqlMetaDataReadInterface metaDataReadInterface,
      PostgreSqlErrorHandler errorHandler,
      SqlHelper sqlHelper,
      PostgreSqlMetrics metrics) {
    super(metaDataReadInterface, errorHandler, sqlHelper);
    this.postgreSqlMetaDataReadInterface = metaDataReadInterface;
    this.errorHandler = errorHandler;
    this.sqlHelper = sqlHelper;
    this.metrics = metrics;
  }

  @Override
  protected String getDeleteDocPartsStatement(String schemaName, String tableName,
      Collection<Integer> dids) {
    StringBuilder sb = new StringBuilder()
        .append("DELETE FROM \"")
        .append(schemaName)
        .append("\".\"")
        .append(tableName)
        .append("\" WHERE \"")
        .append(MetaDocPartTable.DocPartTableFields.DID.fieldName)
        .append("\" IN (");
    for (Integer did : dids) {
      sb.append(did)
          .append(',');
    }
    sb.setCharAt(sb.length() - 1, ')');
    String statement = sb.toString();
    return statement;
  }

  @Override
  public void insertDocPartData(DSLContext dsl, String schemaName, DocPartData docPartData) throws
      UserException {
    metrics.insertRows.mark(docPartData.rowCount());
    metrics.insertFields.mark(docPartData.rowCount() * (docPartData.fieldColumnsCount()
        + docPartData.scalarColumnsCount()));

    if (docPartData.rowCount() == 0) {
      return;
    }

    try (Timer.Context ctx = metrics.insertDocPartDataTimer.time()) {

      int maxCappedSize = 10;
      int cappedSize = Math.min(docPartData.rowCount(), maxCappedSize);

      if (cappedSize < maxCappedSize) { //there are not enough elements on the insert => fallback
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("The insert window is not big enough to use copy (the limit is {}, the real "
              + "size is {}).",
              maxCappedSize,
              cappedSize);
        }

        metrics.insertDefault.mark();

        super.insertDocPartData(dsl, schemaName, docPartData);
      } else {
        Connection connection = dsl.configuration().connectionProvider().acquire();
        try {
          if (!connection.isWrapperFor(PGConnection.class)) {
            LOGGER.warn("It was impossible to use the PostgreSQL way to "
                + "insert documents. Inserting using the standard "
                + "implementation");
            metrics.insertDefault.mark();
            super.insertDocPartData(dsl, schemaName, docPartData);
          } else {
            try {
              metrics.insertCopy.mark();
              copyInsertDocPartData(
                  connection.unwrap(PGConnection.class),
                  schemaName,
                  docPartData
              );
            } catch (DataAccessException ex) {
              throw errorHandler.handleUserException(Context.INSERT, ex);
            } catch (SQLException ex) {
              throw errorHandler.handleUserException(Context.INSERT, ex);
            } catch (IOException ex) {
              if (ex instanceof EOFException && ex.getMessage() == null) {
                LOGGER.debug(ex);
                ex = new EOFException("End of file while COPYing data");
              }

              throw new SystemException(ex);
            }
          }
        } catch (SQLException ex) {
          throw errorHandler.handleException(Context.INSERT, ex);
        } finally {
          dsl.configuration().connectionProvider().release(connection);
        }
      }
    }
  }

  private void copyInsertDocPartData(
      PGConnection connection,
      String schemaName,
      DocPartData docPartData) throws SQLException, IOException {

    final int maxBatchSize = 1024;
    final CopyManager copyManager = connection.getCopyAPI();
    final MetaDocPart metaDocPart = docPartData.getMetaDocPart();
    Collection<InternalField<?>> internalFields = postgreSqlMetaDataReadInterface
        .getInternalFields(metaDocPart);
    final StringBuilder sb = new StringBuilder(65536);
    final String copyStatement = getCopyInsertDocPartDataStatement(
        schemaName, docPartData, metaDocPart, internalFields);

    Iterator<DocPartRow> docPartRowIterator = docPartData.iterator();
    int docCounter = 0;
    while (docPartRowIterator.hasNext()) {
      DocPartRow tableRow = docPartRowIterator.next();
      docCounter++;

      addValuesToCopy(sb, tableRow, internalFields);
      assert sb.length() != 0;

      if (docCounter % maxBatchSize == 0 || !docPartRowIterator.hasNext()) {
        executeCopy(copyManager, copyStatement, sb);
        sb.setLength(0);
      }
    }
  }

  protected String getCopyInsertDocPartDataStatement(String schemaName, DocPartData docPartData,
      final MetaDocPart metaDocPart, Collection<InternalField<?>> internalFields) {
    final StringBuilder copyStatementBuilder = new StringBuilder();
    copyStatementBuilder.append("COPY \"")
        .append(schemaName)
        .append("\".\"").append(metaDocPart.getIdentifier())
        .append("\"").append(" (");

    for (InternalField<?> internalField : internalFields) {
      copyStatementBuilder.append("\"")
          .append(internalField.getName())
          .append("\",");
    }
    Iterator<MetaScalar> metaScalarIterator = docPartData.orderedMetaScalarIterator();
    while (metaScalarIterator.hasNext()) {
      MetaScalar metaScalar = metaScalarIterator.next();
      copyStatementBuilder.append("\"")
          .append(metaScalar.getIdentifier())
          .append("\",");
    }
    Iterator<MetaField> metaFieldIterator = docPartData.orderedMetaFieldIterator();
    while (metaFieldIterator.hasNext()) {
      MetaField metaField = metaFieldIterator.next();
      copyStatementBuilder.append("\"")
          .append(metaField.getIdentifier())
          .append("\",");
    }
    copyStatementBuilder.setCharAt(copyStatementBuilder.length() - 1, ')');
    copyStatementBuilder.append(" FROM STDIN");
    final String copyStatement = copyStatementBuilder.toString();
    return copyStatement;
  }

  private void addValuesToCopy(
      StringBuilder sb,
      DocPartRow docPartRow,
      Collection<InternalField<?>> internalFields) {
    for (InternalField<?> internalField : internalFields) {
      Object internalValue = internalField.getValue(docPartRow);
      if (internalValue == null) {
        sb.append("\\N");
      } else {
        sb.append(internalValue.toString());
      }
      sb.append("\t");
    }
    for (KvValue<?> value : docPartRow.getScalarValues()) {
      addValueToCopy(sb, value);
    }
    for (KvValue<?> value : docPartRow.getFieldValues()) {
      addValueToCopy(sb, value);
    }
    sb.setCharAt(sb.length() - 1, '\n');
  }

  protected void addValueToCopy(StringBuilder sb, KvValue<?> value) {
    if (value != null) {
      value.accept(PostgreSqlValueToCopyConverter.INSTANCE, sb);
    } else {
      sb.append("\\N");
    }
    sb.append('\t');
  }

  private void executeCopy(CopyManager copyManager, String copyStatement, final StringBuilder sb)
      throws SQLException, IOException {
    Reader reader = new StringBuilderReader(sb);

    copyManager.copyIn(copyStatement, reader);
  }

  private static class StringBuilderReader extends Reader {

    private final StringBuilder sb;
    private int readerIndex = 0;

    public StringBuilderReader(StringBuilder sb) {
      this.sb = sb;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      if (readerIndex == sb.length()) {
        return -1;
      }
      int newReaderIndex = Math.min(sb.length(), readerIndex + len);
      sb.getChars(readerIndex, newReaderIndex, cbuf, off);
      int diff = newReaderIndex - readerIndex;
      readerIndex = newReaderIndex;
      return diff;
    }

    @Override
    public void close() {
    }

  }

  @Override
  protected String getInsertDocPartDataStatement(String schemaName, MetaDocPart metaDocPart,
      Iterator<MetaField> metaFieldIterator, Iterator<MetaScalar> metaScalarIterator,
      Collection<InternalField<?>> internalFields, List<FieldType> fieldTypeList) {
    final StringBuilder insertStatementBuilder = new StringBuilder(2048);
    final StringBuilder insertStatementValuesBuilder = new StringBuilder(1024);
    insertStatementBuilder.append("INSERT INTO \"")
        .append(schemaName)
        .append("\".\"")
        .append(metaDocPart.getIdentifier())
        .append("\" (");
    insertStatementValuesBuilder.append(" VALUES (");
    for (InternalField<?> internalField : internalFields) {
      insertStatementBuilder.append("\"")
          .append(internalField.getName())
          .append("\",");
      insertStatementValuesBuilder.append("?,");
    }
    while (metaScalarIterator.hasNext()) {
      MetaScalar metaScalar = metaScalarIterator.next();
      FieldType type = metaScalar.getType();
      insertStatementBuilder.append("\"")
          .append(metaScalar.getIdentifier())
          .append("\",");
      insertStatementValuesBuilder
          .append(sqlHelper.getPlaceholder(type))
          .append(',');
      fieldTypeList.add(type);
    }
    while (metaFieldIterator.hasNext()) {
      MetaField metaField = metaFieldIterator.next();
      FieldType type = metaField.getType();
      insertStatementBuilder.append("\"")
          .append(metaField.getIdentifier())
          .append("\",");
      insertStatementValuesBuilder
          .append(sqlHelper.getPlaceholder(type))
          .append(',');
      fieldTypeList.add(type);
    }
    insertStatementBuilder.setCharAt(insertStatementBuilder.length() - 1, ')');
    insertStatementValuesBuilder.setCharAt(insertStatementValuesBuilder.length() - 1, ')');
    insertStatementBuilder.append(insertStatementValuesBuilder);

    String statement = insertStatementBuilder.toString();
    return statement;
  }
}
