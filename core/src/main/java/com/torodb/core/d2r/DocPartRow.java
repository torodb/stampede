package com.torodb.core.d2r;

import com.torodb.kvdocument.values.KVValue;

public interface DocPartRow {

	DocPartData getDocPartData();

	int getDid();

	int getRid();

	Integer getPid();

	Integer getSeq();

	Iterable<KVValue<?>> getFieldValues();
}
