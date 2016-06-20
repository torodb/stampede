package com.torodb.backend;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.d2r.RidGenerator.DocPartRidGenerator;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KVDocument;

public class D2RTranslatorImpl implements D2RTranslator {

	private final D2RVisitor<DocPartRowImpl> visitor;
	private final D2RVisitorCallbackImpl d2RVisitorCallback;

    @Inject
	public D2RTranslatorImpl(TableRefFactory tableRefFactory, RidGenerator ridGenerator, @Assisted MetaDatabase database, @Assisted MutableMetaCollection collection) {
		AttributeReferenceTranslator attrRefTranslator = new AttributeReferenceTranslator();
		MutableMetaCollection mutableMetaCollection = collection;
		DocPartRidGenerator docPartRidGenerator = ridGenerator.getDocPartRidGenerator(database.getName(), collection.getName());
		d2RVisitorCallback=new D2RVisitorCallbackImpl(tableRefFactory, mutableMetaCollection, docPartRidGenerator, attrRefTranslator);
		visitor=new D2RVisitor<>(tableRefFactory, attrRefTranslator, d2RVisitorCallback);
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
