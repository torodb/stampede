package com.torodb.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator.DocPartRidGenerator;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.d2r.ReservedIdGenerator;

public class CollectionMetaInfo {

    private final MetaDatabase metaDatabase;
    private final MutableMetaCollection metaCollection;
	private final IdentifierFactory identifierFactory;
	private final DocPartRidGenerator docPartRidGenerator;
	
	public CollectionMetaInfo(MetaDatabase metaDatabase, MutableMetaCollection metaCollection, IdentifierFactory identifierFactory, ReservedIdGenerator ridGenerator) {
		this.metaDatabase = metaDatabase;
		this.metaCollection = metaCollection;
		this.identifierFactory = identifierFactory;
		this.docPartRidGenerator = ridGenerator.getDocPartRidGenerator(metaDatabase.getName(), metaCollection.getName());
	}
	
	public int getNextRowId(TableRef tableRef) {
		return docPartRidGenerator.nextRid(tableRef);
	}
	
	public MutableMetaDocPart findMetaDocPart(TableRef tableRef){
		MutableMetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(tableRef);
		if (metaDocPart==null){
			String docPartIdentifier = identifierFactory.toDocPartIdentifier(metaDatabase, metaCollection.getName(), tableRef);
			metaDocPart = metaCollection.addMetaDocPart(tableRef, docPartIdentifier);
		}
		return metaDocPart;
	}
	
	public String getFieldIdentifier(TableRef tableRef, FieldType fieldType, String field){
		MutableMetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(tableRef);
		return identifierFactory.toFieldIdentifier(metaDocPart, field, fieldType);
	}
	
	public String getScalarIdentifier(FieldType fieldType){
		return identifierFactory.toFieldIdentifierForScalar(fieldType);
	}

	
}
