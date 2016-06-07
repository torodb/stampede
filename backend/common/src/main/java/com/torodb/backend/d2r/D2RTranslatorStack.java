package com.torodb.backend.d2r;

import java.util.List;

import com.torodb.backend.IdentifierFactory;
import com.torodb.backend.RidGenerator;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDocument;

public class D2RTranslatorStack implements D2RTranslator {

	private final CollectionMetaInfo collectionMetaInfo;
	private final TableRepository tableRepo;
	private final D2Relational tr;
	
	public D2RTranslatorStack(IdentifierFactory identifierFactory, RidGenerator ridGenerator, MutableMetaSnapshot mutableSnapshot, String dbName, String collectionName) {
		MutableMetaCollection metaCollection = mutableSnapshot.getMetaDatabaseByName(dbName).getMetaCollectionByName(collectionName);
		this.collectionMetaInfo=new CollectionMetaInfo(dbName, collectionName, metaCollection, identifierFactory, ridGenerator);
		this.tableRepo = new TableRepository(collectionMetaInfo);
		this.tr = new D2Relational(tableRepo);
	}

	@Override
	public void translate(KVDocument doc) {
		tr.translate(doc);
	}

	@Override
	public CollectionData getCollectionDataAccumulator() {
		return tableRepo;
	}
	
	public List<DocPartData> getDocParts(){
		return tableRepo.getTables();
	}
}
