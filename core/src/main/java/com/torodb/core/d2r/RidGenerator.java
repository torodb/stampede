package com.torodb.core.d2r;

import com.torodb.core.TableRef;

public interface RidGenerator {

	int nextRid(String dbName, String collectionName, TableRef tableRef);
	
	DocPartRidGenerator getDocPartRidGenerator(String dbName, String collectionName);
	
	public interface DocPartRidGenerator{
		
		int nextRid(TableRef tableRef);
		
	}
	
}
