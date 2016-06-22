package com.torodb.backend;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.transaction.metainf.MetaCollection;

import java.util.List;
import javax.annotation.Nonnull;
import org.jooq.DSLContext;

public interface WriteInterface {
    void insertDocPartData(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull DocPartData docPartData);
    void deleteDocParts(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull MetaCollection metaCollection, @Nonnull List<Integer> dids);
}
