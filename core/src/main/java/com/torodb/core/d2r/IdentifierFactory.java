package com.torodb.core.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.*;

public interface IdentifierFactory {

    String toDatabaseIdentifier(MetaSnapshot snapshot, String dbName);

    String toCollectionIdentifier(MetaDatabase db, String colName);

    String toTableIdentifier(MetaCollection metaCollection, TableRef tableRef);

	String toFieldIdentifier(MetaDocPart metaDocPart, FieldType fieldType, String field);

}