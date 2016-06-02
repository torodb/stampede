package com.torodb.backend;

import javax.inject.Provider;

import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KVDocument;

public class D2RTranslatorImpl implements D2RTranslator {

	private final D2RVisitor<DocPartRowImpl> visitor;
	private final D2RVisitorCallbackImpl d2RVisitorCallback;
	
	public D2RTranslatorImpl(MutableMetaCollection mutableMetaCollection) {
		d2RVisitorCallback=new D2RVisitorCallbackImpl(mutableMetaCollection, new Provider<DocPartRidGenerator>() {
			
			@Override
			public DocPartRidGenerator get() {
				return new DocPartRidGenerator();
			}
		});
		
		visitor=new D2RVisitor<DocPartRowImpl>(d2RVisitorCallback);
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
