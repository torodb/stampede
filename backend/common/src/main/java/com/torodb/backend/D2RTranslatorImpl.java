package com.torodb.backend;

import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDocument;

public class D2RTranslatorImpl implements D2RTranslator {

	private final D2RVisitor<DocPartRowImpl> visitor;
	private final D2RVisitorCallbackImpl d2RVisitorCallback;

	public D2RTranslatorImpl(RidGenerator ridGenerator, MutableMetaSnapshot mutableSnapshot, String dbName, String collectionName) {
		AttributeReferenceTranslator attrRefTranslator = new AttributeReferenceTranslator();
		MutableMetaCollection mutableMetaCollection = mutableSnapshot.getMetaDatabaseByName(dbName).getMetaCollectionByName(collectionName);
		d2RVisitorCallback=new D2RVisitorCallbackImpl(mutableMetaCollection, new DocPartRidGenerator(dbName, collectionName, ridGenerator), attrRefTranslator);
		
		visitor=new D2RVisitor<DocPartRowImpl>(attrRefTranslator, d2RVisitorCallback);
	}

	@Override
	public void translate(KVDocument doc) {
		visitor.visit(doc);
	}

	@Override
	public CollectionData getCollectionDataAccumulator() {
		return d2RVisitorCallback;
	}

}
