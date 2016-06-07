package com.torodb.backend.d2r;

import com.torodb.backend.IdentifierFactory;
import com.torodb.backend.RidGenerator;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;

public class CollectionMetaInfo {

	private final String dbName;
	private final String collectionName;
	private final MutableMetaCollection metaCollection;
	private final IdentifierFactory identifierFactory;
	private final RidGenerator ridGenerator;
	
	public CollectionMetaInfo(String dbName, String collectionName, MutableMetaCollection metaCollection, IdentifierFactory identifierFactory, RidGenerator ridGenerator) {
		this.dbName = dbName;
		this.collectionName = collectionName;
		this.metaCollection = metaCollection;
		this.identifierFactory = identifierFactory;
		this.ridGenerator = ridGenerator;
	}
	
	public int getNextRowId(TableRef tableRef) {
		return ridGenerator.nextRid(dbName, collectionName, tableRef);
	}
	
	public MutableMetaDocPart findMetaDocPart(TableRef tableRef){
		MutableMetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(tableRef);
		if (metaDocPart==null){
			String tableIdentifier = identifierFactory.toTableIdentifier(metaCollection, collectionName, tableRef);
			metaDocPart = metaCollection.addMetaDocPart(tableRef, tableIdentifier);
		}
		return metaDocPart;
	}
	
	public String getFieldIdentifier(TableRef tableRef, FieldType fieldType, String field){
		MutableMetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(tableRef);
		return identifierFactory.toFieldIdentifier(metaDocPart, fieldType, field);
	}
	
}
