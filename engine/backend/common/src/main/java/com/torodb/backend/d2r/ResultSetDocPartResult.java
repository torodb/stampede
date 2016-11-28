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

package com.torodb.backend.d2r;

import com.google.common.base.Preconditions;
import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.InternalField;
import com.torodb.backend.MetaDataReadInterface;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.DocPartResultRow;
import com.torodb.core.d2r.IllegalDocPartRowException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KvValue;
import org.jooq.Converter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

public class ResultSetDocPartResult implements DocPartResult {

  private final MetaDataReadInterface metaDataReadInterface;
  private final DataTypeProvider dataTypeProvider;
  private final ErrorHandler errorHandler;
  private final MetaDocPart metaDocPart;
  private final ResultSet rs;
  /**
   * true iff {@link ResultSet#next() rs.next()} must be called before use the result set.
   */
  private boolean lastRowConsumed = true;
  private boolean hasNext = false;
  private final SqlHelper sqlHelper;

  public ResultSetDocPartResult(MetaDataReadInterface metaDataReadInterface,
      DataTypeProvider dataTypeProvider, ErrorHandler errorHandler,
      MetaDocPart metaDocPart, ResultSet rs, SqlHelper sqlHelper) {
    this.metaDataReadInterface = metaDataReadInterface;
    this.dataTypeProvider = dataTypeProvider;
    this.errorHandler = errorHandler;
    this.metaDocPart = metaDocPart;
    this.rs = rs;
    this.sqlHelper = sqlHelper;
  }

  @Override
  public MetaDocPart getMetaDocPart() {
    return metaDocPart;
  }

  @Override
  public boolean hasNext() {
    if (lastRowConsumed) {
      lastRowConsumed = false;

      try {
        hasNext = rs.next();
      } catch (SQLException sqlException) {
        throw errorHandler.handleException(Context.FETCH, sqlException);
      }
    }
    return hasNext;
  }

  @Override
  public DocPartResultRow next() {
    Preconditions.checkState(hasNext());

    ResultSetNewDocPartRow result = new ResultSetNewDocPartRow();
    lastRowConsumed = true;

    return result;
  }

  @Override
  public void close() {
    try {
      rs.close();
    } catch (SQLException ex) {
      throw errorHandler.handleException(Context.FETCH, ex);
    }
  }

  private class ResultSetNewDocPartRow implements DocPartResultRow {

    private final int did;
    private final int rid;
    private final int pid;
    private final Integer seq;
    private final int firstUserColumnIndex;

    public ResultSetNewDocPartRow() throws IllegalDocPartRowException {
      Collection<InternalField<?>> internalFields = metaDataReadInterface
          .getInternalFields(metaDocPart);

      Integer didTemp = null;
      Integer pidTemp = null;
      Integer ridTemp = null;
      Integer seqTemp = null;
      int columnIndex = 1;
      MetaDocPartTable<Object, MetaDocPartRecord<Object>> metaDocPartTable = metaDataReadInterface
          .getMetaDocPartTable();

      for (InternalField<?> internalField : internalFields) {
        try {
          if (internalField.isDid()) {
            didTemp = metaDocPartTable.DID.getValue(rs, columnIndex);
          } else if (internalField.isRid()) {
            ridTemp = metaDocPartTable.RID.getValue(rs, columnIndex);
          } else if (internalField.isPid()) {
            pidTemp = metaDocPartTable.PID.getValue(rs, columnIndex);
          } else if (internalField.isSeq()) {
            seqTemp = metaDocPartTable.SEQ.getValue(rs, columnIndex);
          }
        } catch (SQLException sqlException) {
          throw errorHandler.handleException(Context.FETCH, sqlException);
        }
        columnIndex++;

        if (didTemp == null) {
          throw new IllegalDocPartRowException(null, ridTemp, pidTemp, seqTemp,
              "did was not found for doc part " + metaDocPart.getTableRef());
        }

        if (ridTemp == null) {
          ridTemp = didTemp;
        }

        if (pidTemp == null) {
          pidTemp = didTemp;
        }
      }

      this.did = didTemp;
      this.rid = ridTemp;
      this.pid = pidTemp;
      this.seq = seqTemp;
      this.firstUserColumnIndex = columnIndex;
    }

    @Override
    public int getDid() {
      return did;
    }

    @Override
    public int getRid() {
      return rid;
    }

    @Override
    public int getPid() {
      return pid;
    }

    @Override
    public Integer getSeq() {
      return seq;
    }

    @Override
    public KvValue<?> getUserValue(int fieldIndex, FieldType fieldType) {
      Object databaseValue;
      try {
        databaseValue = sqlHelper
            .getResultSetValue(fieldType, rs, fieldIndex + firstUserColumnIndex);
      } catch (SQLException sqlException) {
        throw errorHandler.handleException(Context.FETCH, sqlException);
      }

      if (databaseValue == null) {
        return null;
      }

      DataTypeForKv<?> dataType = dataTypeProvider.getDataType(fieldType);
      @SuppressWarnings("unchecked")
      Converter<Object, KvValue<?>> converter = (Converter<Object, KvValue<?>>) dataType
          .getConverter();
      return converter.from(databaseValue);
    }

  }
}
