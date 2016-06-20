package com.torodb.core.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaSnapshot;

public interface IdentifierFactory {

    String toDatabaseIdentifier(MetaSnapshot metaSnapshot, String database);

    String toCollectionIdentifier(MetaDatabase metaDatabase, String collection);

    String toDocPartIdentifier(MetaDatabase metaDatabase, String collection, TableRef tableRef);

	String toFieldIdentifier(MetaDocPart metaDocPart, FieldType fieldType, String field);

}