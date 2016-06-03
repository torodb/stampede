package com.torodb.backend;

import com.torodb.core.TableRef;

public interface RidGenerator {

	int nextRid(String dbName, String collectionName, TableRef tableRef);
	
	
}
