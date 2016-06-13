package com.torodb.backend.d2r;

import com.torodb.backend.IdentifierFactory;
import com.torodb.backend.RidGenerator;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDocument;

public class D2RTranslatorStack implements D2RTranslator {

	private final CollectionMetaInfo collectionMetaInfo;
	private final DocPartDataCollection docPartDataCollection;
	private final D2Relational d2Relational;
	
	public D2RTranslatorStack(TableRefFactory tableRefFactory, IdentifierFactory identifierFactory, RidGenerator ridGenerator, MutableMetaSnapshot mutableSnapshot, String dbName, String collectionName) {
		MutableMetaCollection metaCollection = mutableSnapshot.getMetaDatabaseByName(dbName).getMetaCollectionByName(collectionName);
		this.collectionMetaInfo=new CollectionMetaInfo(dbName, collectionName, metaCollection, identifierFactory, ridGenerator);
		this.docPartDataCollection = new DocPartDataCollection(collectionMetaInfo);
		this.d2Relational = new D2Relational(tableRefFactory, docPartDataCollection);
	}

	@Override
	public void translate(KVDocument doc) {
		d2Relational.translate(doc);
	}

	@Override
	public CollectionData getCollectionDataAccumulator() {
		return docPartDataCollection;
	}
	
}
