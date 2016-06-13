package com.torodb.core.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDocPart;

public interface IdentifierFactory {

	String toTableIdentifier(MetaCollection mutableMetaCollection, String collection, TableRef tableRef);

	String toFieldIdentifier(MetaDocPart metaDocPart, FieldType fieldType, String field);

}