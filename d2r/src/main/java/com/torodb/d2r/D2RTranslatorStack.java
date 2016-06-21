package com.torodb.d2r;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KVDocument;

public class D2RTranslatorStack implements D2RTranslator {

	private final CollectionMetaInfo collectionMetaInfo;
	private final DocPartDataCollection docPartDataCollection;
	private final D2Relational d2Relational;

    @Inject
	public D2RTranslatorStack(TableRefFactory tableRefFactory, IdentifierFactory identifierFactory, 
            RidGenerator ridGenerator, @Assisted MetaDatabase database, @Assisted MutableMetaCollection collection) {
        this.collectionMetaInfo = new CollectionMetaInfo(database, collection, identifierFactory, ridGenerator);
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
