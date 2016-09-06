package com.torodb.backend;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetaCollection;
import java.util.Collection;
import javax.annotation.Nonnull;
import org.jooq.DSLContext;

public interface WriteInterface {
    
    void insertDocPartData(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull DocPartData docPartData) throws UserException;
    
    long deleteCollectionDocParts(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull MetaCollection metaCollection, @Nonnull Cursor<Integer> didCursor);
    
    void deleteCollectionDocParts(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull MetaCollection metaCollection, @Nonnull Collection<Integer> dids);

}
