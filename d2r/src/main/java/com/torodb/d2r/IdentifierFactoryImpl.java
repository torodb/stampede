package com.torodb.d2r;

import java.util.Locale;

import com.torodb.core.TableRef;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;

public class IdentifierFactoryImpl implements IdentifierFactory {

	/*
	 * 0 BINARY, 1 BOOLEAN, 2 DATE, 3 DOUBLE, 4 INSTANT, 5 INTEGER, 6 LONG,
	 * 7 MONGO_OBJECT_ID, 8 MONGO_TIME_STAMP, 9 NULL, 10 STRING, 11 TIME, 12
	 * CHILD;
	 */
	private static final char[] FIELD_TYPE_IDENTIFIERS = new char[FieldType.values().length]; //{ 'r', 'b', 't', 'd', 'k', 'i', 'l', 'x', 'y','n', 's', 'c', 'e' };
	
	static {
		FIELD_TYPE_IDENTIFIERS[FieldType.BINARY.ordinal()]='r';
		FIELD_TYPE_IDENTIFIERS[FieldType.BOOLEAN.ordinal()]='b';
		FIELD_TYPE_IDENTIFIERS[FieldType.DATE.ordinal()]='t';
		FIELD_TYPE_IDENTIFIERS[FieldType.DOUBLE.ordinal()]='d';
		FIELD_TYPE_IDENTIFIERS[FieldType.INSTANT.ordinal()]='k';
		FIELD_TYPE_IDENTIFIERS[FieldType.INTEGER.ordinal()]='i';
		FIELD_TYPE_IDENTIFIERS[FieldType.LONG.ordinal()]='l';
		FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_OBJECT_ID.ordinal()]='x';
		FIELD_TYPE_IDENTIFIERS[FieldType.MONGO_TIME_STAMP.ordinal()]='y';
		FIELD_TYPE_IDENTIFIERS[FieldType.NULL.ordinal()]='n';
		FIELD_TYPE_IDENTIFIERS[FieldType.STRING.ordinal()]='s';
		FIELD_TYPE_IDENTIFIERS[FieldType.TIME.ordinal()]='c';
		FIELD_TYPE_IDENTIFIERS[FieldType.CHILD.ordinal()]='e';
	}
	
	/* (non-Javadoc)
	 * @see com.torodb.backend.IdentifierFact#toTableIdentifier(com.torodb.core.transaction.metainf.MutableMetaCollection, java.lang.String, com.torodb.core.TableRef)
	 */
	@Override
	public String toTableIdentifier(MetaCollection mutableMetaCollection, String collection, TableRef tableRef) {
		StringBuilder sb=new StringBuilder(collection);
		buildTableName(sb, tableRef);
		String id = sb.toString();
		while (true){
			//TODO: Add Database conditions and restrictions: maxlenght, reserved keywords. Inject it
			MetaDocPart metaDocPartByIdentifier = mutableMetaCollection.getMetaDocPartByIdentifier(id);
			if (metaDocPartByIdentifier==null){
				return id;
			}
			throw new RuntimeException("Identifier collision. Create other value");
		}
	}
	
	/* (non-Javadoc)
	 * @see com.torodb.backend.IdentifierFact#toFieldIdentifier(com.torodb.core.transaction.metainf.MutableMetaDocPart, com.torodb.core.transaction.metainf.FieldType, java.lang.String)
	 */
	@Override
	public String toFieldIdentifier(MetaDocPart metaDocPart, FieldType fieldType, String field){
		String value=normalize(field)+"_"+FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()];
		while (true){
			//TODO: Add Database conditions and restrictions: maxlenght, reserved keywords. Inject it
			MetaField metaFieldByIdentifier = metaDocPart.getMetaFieldByIdentifier(value);
			if (metaFieldByIdentifier==null){
				return value;
			}
			throw new RuntimeException("Identifier collision. Create other value");
		}
	}
	
	private void buildTableName(StringBuilder sb, TableRef tr){
		if (tr.isRoot()){
			return;
		}
		buildTableName(sb, tr.getParent().get());
		sb.append(".").append(normalize(tr.getName()));
	}
	
	private String normalize(String key){
		return key.toLowerCase(Locale.US).replaceAll("[^a-z0-9$]", "_");
	}
}
