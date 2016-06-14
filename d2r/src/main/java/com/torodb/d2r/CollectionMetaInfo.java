package com.torodb.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.d2r.RidGenerator.DocPartRidGenerator;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;

public class CollectionMetaInfo {

	private final String dbName;
	private final String collectionName;
	private final MutableMetaCollection metaCollection;
	private final IdentifierFactory identifierFactory;
	private final DocPartRidGenerator docPartRidGenerator;
	
	public CollectionMetaInfo(String dbName, String collectionName, MutableMetaCollection metaCollection, IdentifierFactory identifierFactory, RidGenerator ridGenerator) {
		this.dbName = dbName;
		this.collectionName = collectionName;
		this.metaCollection = metaCollection;
		this.identifierFactory = identifierFactory;
		this.docPartRidGenerator = ridGenerator.getDocPartRidGenerator(dbName, collectionName);
	}
	
	public int getNextRowId(TableRef tableRef) {
		return docPartRidGenerator.nextRid(tableRef);
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
