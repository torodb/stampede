package com.torodb.core.d2r;

import com.google.common.util.concurrent.Service;
import com.torodb.core.TableRef;

public interface ReservedIdGenerator extends Service {

	int nextRid(String dbName, String collectionName, TableRef tableRef);
	
    void setNextRid(String dbName, String collectionName, TableRef tableRef, int nextRid);
	
	DocPartRidGenerator getDocPartRidGenerator(String dbName, String collectionName);
	
	public interface DocPartRidGenerator{
		
		int nextRid(TableRef tableRef);
		
	    void setNextRid(TableRef tableRef, int nextRid);
	    
	}
	
}
