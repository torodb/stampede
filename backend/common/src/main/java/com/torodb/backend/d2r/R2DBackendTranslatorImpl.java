package com.torodb.backend.d2r;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.jooq.Converter;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.core.d2r.InternalFields;
import com.torodb.core.d2r.R2DBackendTranslator;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KVValue;

public class R2DBackendTranslatorImpl implements R2DBackendTranslator<ResultSet, R2DBackendTranslatorImpl.ResultSetInternalFields> {

    private final SqlInterface sqlInterface;
    private final SqlHelper sqlHelper;
    private final MetaDatabase metaDatabase;
    private final MetaCollection metaCollection;
    private final Converter<Object, Integer> didConverter;
    private final Converter<Object, Integer> ridConverter;
    private final Converter<Object, Integer> pidConverter;
    private final Converter<Object, Integer> seqConverter;
	
	@SuppressWarnings("unchecked")
    public R2DBackendTranslatorImpl(SqlInterface sqlInterface, SqlHelper sqlHelper, MetaDatabase metaDatabase, MetaCollection metaCollection) {
        this.sqlInterface = sqlInterface;
        this.sqlHelper = sqlHelper;
        this.metaDatabase = metaDatabase;
        this.metaCollection = metaCollection;
        MetaDocPartTable<Object,MetaDocPartRecord<Object>> metaDocPartTable = sqlInterface.getMetaDataReadInterface().getMetaDocPartTable();
	    this.didConverter = (Converter<Object, Integer>) metaDocPartTable.DID.getConverter();
	    this.ridConverter = (Converter<Object, Integer>) metaDocPartTable.RID.getConverter();
	    this.pidConverter = (Converter<Object, Integer>) metaDocPartTable.PID.getConverter();
	    this.seqConverter = (Converter<Object, Integer>) metaDocPartTable.SEQ.getConverter();
	}

    public class ResultSetInternalFields extends InternalFields {
        public final int columnIndex;

        public ResultSetInternalFields(Integer did, Integer rid, Integer pid, Integer seq, int columnIndex) {
            super(did, rid, pid, seq);
            this.columnIndex = columnIndex;
        }

        public int getColumnIndex() {
            return columnIndex;
        }
    }
    
    @Override
    public ResultSetInternalFields readInternalFields(MetaDocPart metaDocPart, ResultSet resultSet) {
        Integer did = null;
        Integer pid = null;
        Integer rid = null;
        Integer seq = null;
        Collection<InternalField<?>> internalFields = sqlInterface.getMetaDataReadInterface().getDocPartTableInternalFields(
                metaDocPart);
        int columnIndex = 1;
        for (InternalField<?> internalField : internalFields) {
            try {
                if (internalField.isDid()) {
                    did = didConverter.from(resultSet.getInt(columnIndex));
                } else if (internalField.isRid()) {
                    rid = ridConverter.from(resultSet.getInt(columnIndex));
                } else if (internalField.isPid()) {
                    pid = pidConverter.from(resultSet.getInt(columnIndex));
                } else if (internalField.isSeq()) {
                    seq = seqConverter.from(resultSet.getInt(columnIndex));
                }
            } catch (SQLException sqlException) {
                sqlInterface.getErrorHandler().handleRollbackException(Context.fetch, sqlException);
                
                throw new SystemException(sqlException);
            }
            columnIndex++;
        }
        if (did == null) {
            throw new AssertionError("did was not found for doc part " + metaDocPart.getTableRef() 
                    + " in collection " + metaCollection.getName() + " and database " + metaDatabase.getName());
        }
        
        if (rid == null) {
            rid = did;
        }
        
        if (pid == null) {
            pid = did;
        }
        
        return new ResultSetInternalFields(did, rid, pid, seq, columnIndex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KVValue<?> getValue(FieldType type, ResultSet resultSet, ResultSetInternalFields internalFields,
            int fieldIndex) {
        Object databaseValue;
        try {
            databaseValue = sqlHelper.getResultSetValue(type, resultSet, fieldIndex + internalFields.columnIndex);
        } catch (SQLException sqlException) {
            sqlInterface.getErrorHandler().handleRollbackException(Context.fetch, sqlException);
            throw new SystemException(sqlException);
        }
        
        if (databaseValue == null) {
            return null;
        }
        
        DataTypeForKV<?> dataType = sqlInterface.getDataTypeProvider().getDataType(type);
        Converter<Object, KVValue<?>> converter = (Converter<Object, KVValue<?>>) dataType.getConverter();
        return converter.from(databaseValue);
    }

    @Override
    public boolean next(ResultSet resultSet) {
        try {
            return resultSet.next();
        } catch (SQLException sqlException) {
            sqlInterface.getErrorHandler().handleRollbackException(Context.fetch, sqlException);
            
            throw new SystemException(sqlException);
        }
    }
}
