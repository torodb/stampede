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

import com.torodb.backend.AbstractReadInterface;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.tables.MetaDocPartTable.DocPartTableFields;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import org.jooq.Converter;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PostgreSqlReadInterface extends AbstractReadInterface {

  private final PostgreSqlMetaDataReadInterface metaDataReadInterface;

  @Inject
  public PostgreSqlReadInterface(PostgreSqlMetaDataReadInterface metaDataReadInterface,
      PostgreSqlDataTypeProvider dataTypeProvider,
      PostgreSqlErrorHandler errorhandler, SqlHelper sqlHelper, TableRefFactory tableRefFactory) {
    super(metaDataReadInterface, dataTypeProvider, errorhandler, sqlHelper, tableRefFactory);
    this.metaDataReadInterface = metaDataReadInterface;
  }

  @Override
  protected String getReadCollectionDidsWithFieldEqualsToStatement(String schemaName,
      String rootTableName,
      String columnName) {
    StringBuilder sb = new StringBuilder()
        .append("SELECT \"")
        .append(DocPartTableFields.DID.fieldName)
        .append("\" FROM \"")
        .append(schemaName)
        .append("\".\"")
        .append(rootTableName)
        .append("\" WHERE \"")
        .append(rootTableName)
        .append("\".\"")
        .append(columnName)
        .append("\" = ? GROUP BY \"")
        .append(DocPartTableFields.DID.fieldName)
        .append("\" ORDER BY \"")
        .append(DocPartTableFields.DID.fieldName)
        .append('"');
    String statement = sb.toString();
    return statement;
  }

  @Override
  protected String getReadCollectionDidsWithFieldInStatement(
      String schemaName, String rootTableName, Stream<Tuple2<String, Integer>> valuesCountList) {
    StringBuilder sb = new StringBuilder()
        .append("SELECT \"")
        .append(DocPartTableFields.DID.fieldName)
        .append("\" FROM \"")
        .append(schemaName)
        .append("\".\"")
        .append(rootTableName)
        .append("\" WHERE \"");

    Iterator<Tuple2<String, Integer>> valuesCountMapEntryIterator =
        valuesCountList.iterator();
    while (valuesCountMapEntryIterator.hasNext()) {
      Tuple2<String, Integer> valuesCountMapEntry =
          valuesCountMapEntryIterator.next();
      String columnName = valuesCountMapEntry.v1;
      Integer valuesCount = valuesCountMapEntry.v2;

      sb
          .append(rootTableName)
          .append("\".\"")
          .append(columnName)
          .append("\" IN (");

      for (int index = 0; index < valuesCount; index++) {
        sb.append("?,");
      }
      sb.setCharAt(sb.length() - 1, ')');
    }

    sb.append(" ORDER BY \"")
        .append(DocPartTableFields.DID.fieldName)
        .append('"');
    String statement = sb.toString();
    return statement;
  }

  @Override
  protected String getReadCollectionDidsAndProjectionWithFieldInStatement(String schemaName,
      String rootTableName,
      String columnName, int valuesCount) {
    StringBuilder sb = new StringBuilder()
        .append("SELECT \"")
        .append(DocPartTableFields.DID.fieldName)
        .append("\",\"")
        .append(columnName)
        .append("\" FROM \"")
        .append(schemaName)
        .append("\".\"")
        .append(rootTableName)
        .append("\" WHERE \"")
        .append(columnName)
        .append("\" IN (");

    for (int index = 0; index < valuesCount; index++) {
      sb.append("?,");
    }
    sb.setCharAt(sb.length() - 1, ')');

    String statement = sb.toString();
    return statement;
  }

  @Override
  protected String getReadAllCollectionDidsStatement(String schemaName, String rootTableName) {
    StringBuilder sb = new StringBuilder()
        .append("SELECT \"")
        .append(DocPartTableFields.DID.fieldName)
        .append("\" FROM \"")
        .append(schemaName)
        .append("\".\"")
        .append(rootTableName)
        .append('"');
    String statement = sb.toString();
    return statement;
  }

  @Override
  protected String getReadCountAllStatement(String schemaName, String rootTableName) {
    StringBuilder sb = new StringBuilder()
        .append("SELECT COUNT(1) FROM \"")
        .append(schemaName)
        .append("\".\"")
        .append(rootTableName)
        .append('"');
    String statement = sb.toString();
    return statement;
  }

  @Override
  protected String getDocPartStatament(MetaDatabase metaDatabase, MetaDocPart metaDocPart,
      Collection<Integer> dids) {
    StringBuilder sb = new StringBuilder()
        .append("SELECT ");
    Collection<InternalField<?>> internalFields = metaDataReadInterface.getInternalFields(
        metaDocPart);
    for (InternalField<?> internalField : internalFields) {
      sb.append('"')
          .append(internalField.getName())
          .append("\",");
    }
    metaDocPart.streamScalars().forEach(metaScalar -> {
      sb.append('"')
          .append(metaScalar.getIdentifier())
          .append("\",");
    });
    metaDocPart.streamFields().forEach(metaField -> {
      sb.append('"')
          .append(metaField.getIdentifier())
          .append("\",");
    });
    sb.setCharAt(sb.length() - 1, ' ');
    sb
        .append("FROM \"")
        .append(metaDatabase.getIdentifier())
        .append("\".\"")
        .append(metaDocPart.getIdentifier())
        .append("\" WHERE \"")
        .append(metaDataReadInterface.getMetaDocPartTable().DID.getName())
        .append("\" IN (");
    Converter<?, Integer> converter =
        metaDataReadInterface.getMetaDocPartTable().DID.getDataType().getConverter();
    for (Integer requestedDoc : dids) {
      sb.append(converter.to(requestedDoc))
          .append(',');
    }
    sb.setCharAt(sb.length() - 1, ')');
    if (!metaDocPart.getTableRef().isRoot()) {
      sb.append(" ORDER BY ");
      Collection<InternalField<?>> internalFieldsIt =
          metaDataReadInterface.getReadInternalFields(metaDocPart);
      for (InternalField<?> internalField : internalFieldsIt) {
        sb
            .append('"')
            .append(internalField.getName())
            .append("\",");
      }
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  @Override
  protected String getLastRowIdUsedStatement(MetaDatabase metaDatabase, MetaDocPart metaDocPart) {
    TableRef tableRef = metaDocPart.getTableRef();

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT max(\"")
        .append(getPrimaryKeyColumnIdentifier(tableRef))
        .append("\") FROM \"")
        .append(metaDatabase.getIdentifier())
        .append("\".\"")
        .append(metaDocPart.getIdentifier())
        .append("\"");
    String statement = sb.toString();
    return statement;
  }
}
