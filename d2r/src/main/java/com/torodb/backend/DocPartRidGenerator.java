package com.torodb.backend;

import com.torodb.core.TableRef;
import com.torodb.core.d2r.RidGenerator;

public class DocPartRidGenerator {

	private final String dbName;
	private final String collectionName;
	private final RidGenerator ridGenerator;

	public DocPartRidGenerator(String dbName, String collectionName, RidGenerator ridGenerator) {
		this.dbName = dbName;
		this.collectionName = collectionName;
		this.ridGenerator = ridGenerator;
	}

	public int nextRid(TableRef tableRef) {
		return ridGenerator.nextRid(dbName, collectionName, tableRef);
	}
	
}
