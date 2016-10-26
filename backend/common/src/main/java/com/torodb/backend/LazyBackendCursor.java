
package com.torodb.backend;

import com.google.common.base.Preconditions;
import com.torodb.core.backend.BackendCursor;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import javax.annotation.Nonnull;
import org.jooq.DSLContext;

/**
 *
 */
public class LazyBackendCursor implements BackendCursor {

    private final Cursor<Integer> didCursor;
    private final DefaultDocPartResultCursor docCursor;
    private boolean usedAsDocPartCursor = false;
    private boolean usedAsDidCursor = false;

    public LazyBackendCursor(
            @Nonnull SqlInterface sqlInterface,
            final @Nonnull Cursor<Integer> didCursor,
            @Nonnull DSLContext dsl,
            @Nonnull MetaDatabase metaDatabase,
            @Nonnull MetaCollection metaCollection) {
        docCursor = new DefaultDocPartResultCursor(sqlInterface, didCursor, dsl, metaDatabase, metaCollection);
        this.didCursor = didCursor;
    }

    @Override
    public Cursor<DocPartResult> asDocPartResultCursor() {
        Preconditions.checkState(!usedAsDidCursor, "This cursor has already been used as a did cursor");
        usedAsDocPartCursor = true;
        return docCursor;
    }

    @Override
    public Cursor<Integer> asDidCursor() {
        Preconditions.checkState(!usedAsDocPartCursor, "This cursor has already been used as a doc part cursor");
        usedAsDidCursor = true;
        return didCursor;
    }
}
