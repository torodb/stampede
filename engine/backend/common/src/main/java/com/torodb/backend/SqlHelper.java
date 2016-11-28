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
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.converters.jooq.KvValueConverter;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.kvdocument.values.KvValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Converter;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public class SqlHelper {

  private final DataTypeProvider dataTypeProvider;
  private final ErrorHandler errorHandler;

  @Inject
  public SqlHelper(DataTypeProvider dataTypeProvider, ErrorHandler errorHandler) {
    super();
    this.dataTypeProvider = dataTypeProvider;
    this.errorHandler = errorHandler;
  }

  public void executeStatement(DSLContext dsl, String statement, Context context) {
    Connection c = dsl.configuration().connectionProvider().acquire();
    try (PreparedStatement ps = c.prepareStatement(statement)) {
      ps.execute();
    } catch (SQLException ex) {
      throw errorHandler.handleException(context, ex);
    } finally {
      dsl.configuration().connectionProvider().release(c);
    }
  }

  @FunctionalInterface
  public interface SetupPreparedStatement {

    public void accept(PreparedStatement ps) throws SQLException;
  }

  public Result<Record> executeStatementWithResult(DSLContext dsl, String statement,
      Context context) {
    return executeStatementWithResult(dsl, statement, context, ps -> {
    });
  }

  public Result<Record> executeStatementWithResult(DSLContext dsl, String statement,
      Context context,
      SetupPreparedStatement statementSetup) {
    Connection c = dsl.configuration().connectionProvider().acquire();
    try (PreparedStatement ps = c.prepareStatement(statement)) {
      statementSetup.accept(ps);
      try (ResultSet resultSet = ps.executeQuery()) {
        return dsl.fetch(resultSet);
      }
    } catch (SQLException ex) {
      throw errorHandler.handleException(context, ex);
    } finally {
      dsl.configuration().connectionProvider().release(c);
    }
  }

  public int executeUpdate(DSLContext dsl, String statement, Context context) {
    Connection c = dsl.configuration().connectionProvider().acquire();
    try (PreparedStatement ps = c.prepareStatement(statement)) {
      return ps.executeUpdate();
    } catch (SQLException ex) {
      throw errorHandler.handleException(context, ex);
    } finally {
      dsl.configuration().connectionProvider().release(c);
    }
  }

  public int executeUpdate(Connection c, String statement, Context context) {
    try (PreparedStatement ps = c.prepareStatement(statement)) {
      return ps.executeUpdate();
    } catch (SQLException ex) {
      throw errorHandler.handleException(context, ex);
    }
  }

  public int executeUpdateOrThrow(DSLContext dsl, String statement, Context context) throws
      UserException {
    Connection c = dsl.configuration().connectionProvider().acquire();
    try (PreparedStatement ps = c.prepareStatement(statement)) {
      return ps.executeUpdate();
    } catch (SQLException ex) {
      throw errorHandler.handleUserException(context, ex);
    } finally {
      dsl.configuration().connectionProvider().release(c);
    }
  }

  public int executeUpdateOrThrow(Connection c, String statement, Context context) throws
      UserException {
    try (PreparedStatement ps = c.prepareStatement(statement)) {
      return ps.executeUpdate();
    } catch (SQLException ex) {
      throw errorHandler.handleUserException(context, ex);
    }
  }

  public String renderVal(String value) {
    return dsl().render(DSL.val(value));
  }

  public DSLContext dsl() {
    return DSL.using(dataTypeProvider.getDialect());
  }

  @SuppressWarnings({"rawtypes"})
  public Object getResultSetValue(FieldType fieldType, ResultSet resultSet, int index) throws
      SQLException {
    DataTypeForKv dataType = dataTypeProvider.getDataType(fieldType);
    KvValueConverter valueConverter = dataType.getKvValueConverter();
    SqlBinding sqlBinding = valueConverter.getSqlBinding();
    return sqlBinding.get(resultSet, index);
  }

  @SuppressWarnings({"unchecked"})
  public KvValue<?> getResultSetKvValue(FieldType fieldType, DataTypeForKv<?> dataTypeForKv,
      ResultSet resultSet, int index) throws SQLException {
    Object databaseValue = getResultSetValue(FieldType.from(dataTypeForKv.getKvValueConverter()
        .getErasuredType()), resultSet, index);
    if (resultSet.wasNull()) {
      return null;
    }

    return ((Converter<Object, KvValue<?>>) dataTypeForKv.getConverter()).from(databaseValue);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void setPreparedStatementNullableValue(PreparedStatement preparedStatement,
      int parameterIndex,
      FieldType fieldType, KvValue<?> value) throws SQLException {
    DataTypeForKv dataType = dataTypeProvider.getDataType(fieldType);
    if (value != null) {
      KvValueConverter valueConverter = dataType.getKvValueConverter();
      SqlBinding sqlBinding = valueConverter.getSqlBinding();
      Converter converter = dataType.getConverter();
      sqlBinding.set(preparedStatement, parameterIndex, converter.to(value));
    } else {
      preparedStatement.setNull(parameterIndex, dataType.getSQLType());
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void setPreparedStatementValue(PreparedStatement preparedStatement, int parameterIndex,
      FieldType fieldType, KvValue<?> value) throws SQLException {
    DataTypeForKv dataType = dataTypeProvider.getDataType(fieldType);
    KvValueConverter valueConverter = dataType.getKvValueConverter();
    Converter converter = dataType.getConverter();
    SqlBinding sqlBinding = valueConverter.getSqlBinding();
    sqlBinding.set(preparedStatement, parameterIndex, converter.to(value));
  }

  @SuppressWarnings({"rawtypes"})
  public String getPlaceholder(FieldType fieldType) {
    DataTypeForKv dataType = dataTypeProvider.getDataType(fieldType);
    KvValueConverter valueConverter = dataType.getKvValueConverter();
    SqlBinding sqlBinding = valueConverter.getSqlBinding();
    return sqlBinding.getPlaceholder();
  }

  @SuppressWarnings("rawtypes")
  public String getSqlTypeName(FieldType fieldType) {
    DataTypeForKv dataType = dataTypeProvider.getDataType(fieldType);

    return dataType.getTypeName();
  }
}
