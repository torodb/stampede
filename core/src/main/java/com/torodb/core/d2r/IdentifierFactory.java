package com.torodb.core.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaSnapshot;

public interface IdentifierFactory {

    String toSchemaIdentifier(MetaSnapshot metaSnapshot, String database);

    String toTableIdentifier(MetaDatabase metaDatabase, String collection, TableRef tableRef);

	String toFieldIdentifier(MetaDocPart metaDocPart, FieldType fieldType, String field);

}