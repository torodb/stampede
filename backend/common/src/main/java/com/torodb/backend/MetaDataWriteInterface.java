package com.torodb.backend;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndex;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.core.transaction.metainf.MetaScalar;

public interface MetaDataWriteInterface {
    void createMetaDatabaseTable(@Nonnull DSLContext dsl);
    void createMetaCollectionTable(@Nonnull DSLContext dsl);
    void createMetaDocPartTable(@Nonnull DSLContext dsl);
    void createMetaFieldTable(@Nonnull DSLContext dsl);
    void createMetaScalarTable(@Nonnull DSLContext dsl);
    void createMetaIndexTable(@Nonnull DSLContext dsl);
    void createMetaIndexFieldTable(@Nonnull DSLContext dsl);
    void createMetaDocPartIndexTable(@Nonnull DSLContext dsl);
    void createMetaFieldIndexTable(@Nonnull DSLContext dsl);
    
    void addMetaDatabase(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database);
    void addMetaCollection(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection);
    void addMetaDocPart(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart);
    void addMetaField(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart, @Nonnull MetaField field);
    void addMetaScalar(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart, @Nonnull MetaScalar scalar);
    void addMetaIndex(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaIndex index);
    void addMetaIndexField(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaIndex index, @Nonnull MetaIndexField field);
    void addMetaDocPartIndex(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart, @Nonnull MetaDocPartIndex index);
    void addMetaDocPartIndexColumn(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart, @Nonnull MetaDocPartIndex index, @Nonnull MetaDocPartIndexColumn field);

    void deleteMetaDatabase(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database);
    void deleteMetaCollection(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection);
    void deleteMetaIndex(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaIndex index);
    void deleteMetaDocPartIndex(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart, @Nonnull MetaDocPartIndex index);
    
    int consumeRids(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart, int count);
}
