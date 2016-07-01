package com.torodb.backend;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.backend.DidCursor;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.transaction.metainf.MetaCollection;

public interface WriteInterface {
    void insertDocPartData(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull DocPartData docPartData);
    long deleteCollectionDocParts(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull MetaCollection metaCollection, @Nonnull DidCursor didCursor);
}
