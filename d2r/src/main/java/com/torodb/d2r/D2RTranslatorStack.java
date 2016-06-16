package com.torodb.d2r;

import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDocument;

public class D2RTranslatorStack implements D2RTranslator {

	private final CollectionMetaInfo collectionMetaInfo;
	private final DocPartDataCollection docPartDataCollection;
	private final D2Relational d2Relational;
	
	public D2RTranslatorStack(TableRefFactory tableRefFactory, IdentifierFactory identifierFactory, RidGenerator ridGenerator, MutableMetaSnapshot mutableSnapshot, String dbName, String collectionName) {
	    MutableMetaDatabase mutableMetaDatabase = mutableSnapshot.getMetaDatabaseByName(dbName);
		MutableMetaCollection mutableMetaCollection = mutableMetaDatabase.getMetaCollectionByName(collectionName);
		this.collectionMetaInfo=new CollectionMetaInfo(mutableMetaDatabase, mutableMetaCollection, identifierFactory, ridGenerator);
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
