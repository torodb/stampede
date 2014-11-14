
package com.torodb.torod.db.executor.report;

import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public interface QueryReport {

    public void taskExecuted(
            @Nonnull String collection, 
            @Nonnull CursorId cursorId, 
            @Nullable QueryCriteria filter, 
            @Nullable Projection projection,
            @Nonnegative int maxResults,
            @Nonnegative int realResultCount);
}
