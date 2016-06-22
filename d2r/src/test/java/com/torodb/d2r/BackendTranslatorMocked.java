package com.torodb.d2r;

import com.torodb.core.d2r.InternalFields;
import com.torodb.core.d2r.R2DBackendTranslator;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.d2r.MockedResultSet.MockedRow;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.StringKVString;

public class BackendTranslatorMocked implements R2DBackendTranslator<MockedResultSet, InternalFields> {

	@Override
	public boolean next(MockedResultSet result) {
		return result.next();
	}

	@Override
	public InternalFields readInternalFields(MetaDocPart metaDocPart, MockedResultSet resultSet) {
		MockedRow result = resultSet.getCurrent();
		return new InternalFields(result.getDid(), result.getRid(), result.getPid(), result.getSeq());
	}

	@Override
	public KVValue<?> getValue(FieldType type, MockedResultSet resultSet, InternalFields internalFields, int fieldIndex) {
		MockedRow result = resultSet.getCurrent();
		return convertValue(result.get(fieldIndex));
	}

	private KVValue<?> convertValue(Object value) {
		if (value == null){
			return null;
		}
		if (value == KVNull.getInstance()){
			return KVNull.getInstance();
		}
		if (value instanceof String) {
			return new StringKVString((String) value);
		} else if (value instanceof Integer) {
			return KVInteger.of((Integer) value);
		} else if (value instanceof Double) {
			return KVDouble.of((Double) value);
		} else if (value instanceof Long) {
			return KVLong.of((Long) value);
		} else if (value instanceof Boolean) {
			return KVBoolean.from((boolean) value);
		} else {
			throw new RuntimeException("Unexpected type value");
		}
	}

}
