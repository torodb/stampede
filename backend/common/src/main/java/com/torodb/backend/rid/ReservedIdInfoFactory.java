package com.torodb.backend.rid;

import com.torodb.core.TableRef;

public interface ReservedIdInfoFactory {

	ReservedIdInfo create(String dbName, String collectionName, TableRef tableRef);

}
