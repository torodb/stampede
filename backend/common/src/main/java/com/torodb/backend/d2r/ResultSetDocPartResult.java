
package com.torodb.backend.d2r;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.jooq.Converter;

import com.google.common.base.Preconditions;
import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.InternalField;
import com.torodb.backend.MetaDataReadInterface;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.DocPartResultRow;
import com.torodb.core.d2r.IllegalDocPartRowException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
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

        private final int did, rid, pid;
        private final Integer seq;
        private final int firstUserColumnIndex;

        public ResultSetNewDocPartRow() throws IllegalDocPartRowException {
            Collection<InternalField<?>> internalFields = metaDataReadInterface
                    .getInternalFields(metaDocPart);

            Integer _did = null;
            Integer _pid = null;
            Integer _rid = null;
            Integer _seq = null;
            int columnIndex = 1;
            MetaDocPartTable<Object,MetaDocPartRecord<Object>> metaDocPartTable = metaDataReadInterface.getMetaDocPartTable();

            for (InternalField<?> internalField : internalFields) {
                try {
                    if (internalField.isDid()) {
                        _did = metaDocPartTable.DID.getValue(rs, columnIndex);
                    } else if (internalField.isRid()) {
                        _rid = metaDocPartTable.RID.getValue(rs, columnIndex);
                    } else if (internalField.isPid()) {
                        _pid = metaDocPartTable.PID.getValue(rs, columnIndex);
                    } else if (internalField.isSeq()) {
                        _seq = metaDocPartTable.SEQ.getValue(rs, columnIndex);
                    }
                } catch (SQLException sqlException) {
                    throw errorHandler.handleException(Context.FETCH, sqlException);
                }
                columnIndex++;

                if (_did == null) {
                    throw new IllegalDocPartRowException(null, _rid, _pid, _seq,
                            "did was not found for doc part " + metaDocPart.getTableRef());
                }

                if (_rid == null) {
                    _rid = _did;
                }

                if (_pid == null) {
                    _pid = _did;
                }
            }

            this.did = _did;
            this.rid = _rid;
            this.pid = _pid;
            this.seq = _seq;
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
        public KVValue<?> getUserValue(int fieldIndex, FieldType fieldType) {
            Object databaseValue;
            try {
                databaseValue = sqlHelper.getResultSetValue(fieldType, rs, fieldIndex + firstUserColumnIndex);
            } catch (SQLException sqlException) {
                throw errorHandler.handleException(Context.FETCH, sqlException);
            }

            if (databaseValue == null) {
                return null;
            }

            DataTypeForKV<?> dataType = dataTypeProvider.getDataType(fieldType);
            @SuppressWarnings("unchecked")
            Converter<Object, KVValue<?>> converter = (Converter<Object, KVValue<?>>) dataType.getConverter();
            return converter.from(databaseValue);
        }

    }
}
