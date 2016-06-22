package com.torodb.backend.d2r;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.jooq.Converter;

import com.torodb.backend.InternalField;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.tables.MetaDocPartTable;
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
    private final MetaDatabase metaDatabase;
    private final MetaCollection metaCollection;
    private final Converter<Object, Integer> didConverter;
    private final Converter<Object, Integer> ridConverter;
    private final Converter<Object, Integer> pidConverter;
    private final Converter<Object, Integer> seqConverter;
	
	@SuppressWarnings("unchecked")
    public R2DBackendTranslatorImpl(SqlInterface sqlInterface, MetaDatabase metaDatabase, MetaCollection metaCollection) {
        this.sqlInterface = sqlInterface;
        this.metaDatabase = metaDatabase;
        this.metaCollection = metaCollection;
	    this.didConverter = (Converter<Object, Integer>) 
	            sqlInterface.getMetaDocPartTable().DID.getConverter();
	    this.ridConverter = (Converter<Object, Integer>) 
	            sqlInterface.getMetaDocPartTable().RID.getConverter();
	    this.pidConverter = (Converter<Object, Integer>) 
	            sqlInterface.getMetaDocPartTable().PID.getConverter();
	    this.seqConverter = (Converter<Object, Integer>) 
	            sqlInterface.getMetaDocPartTable().SEQ.getConverter();
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
        Collection<InternalField<?>> internalFields = sqlInterface.getDocPartTableInternalFields(
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
                sqlInterface.handleRollbackException(Context.fetch, sqlException);
                
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

    @Override
    public String getScalarName() {
        return MetaDocPartTable.DocPartTableFields.SCALAR.fieldName;
    }

    @Override
    public KVValue<?> getValue(FieldType type, ResultSet resultSet, ResultSetInternalFields internalFields,
            int fieldIndex) {
        Object databaseValue;
        try {
            databaseValue = resultSet.getObject(fieldIndex + internalFields.columnIndex);
        } catch (SQLException sqlException) {
            sqlInterface.handleRollbackException(Context.fetch, sqlException);
            throw new SystemException(sqlException);
        }
        
        if (databaseValue == null) {
            return null;
        }
        
        DataTypeForKV<?> dataType = sqlInterface.getDataType(type);
        Converter<Object, KVValue<?>> converter = (Converter<Object, KVValue<?>>) dataType.getConverter();
        KVValue<?> value = converter.from(databaseValue);
        
        return value;
    }

    @Override
    public boolean next(ResultSet resultSet) {
        try {
            return resultSet.next();
        } catch (SQLException sqlException) {
            sqlInterface.handleRollbackException(Context.fetch, sqlException);
            
            throw new SystemException(sqlException);
        }
    }
}
