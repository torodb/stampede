/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.backend;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.backend.DidCursor;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.jooq.DSLContext;


/**
 *
 */
public class DefaultCursor implements Cursor<ToroDocument> {
    
    private static final int BATCH_SIZE = 1000;

    private final SqlInterface sqlInterface;
    private final R2DTranslator r2dTranslator;
    private final DidCursor didCursor;
    private final DSLContext dsl;
    private final MetaDatabase metaDatabase;
    private final MetaCollection metaCollection;
    
    public DefaultCursor(
            @Nonnull SqlInterface sqlInterface,
            @Nonnull R2DTranslator r2dTranslator,
            @Nonnull DidCursor didCursor,
            @Nonnull DSLContext dsl,
            @Nonnull MetaDatabase metaDatabase,
            @Nonnull MetaCollection metaCollection) {
        this.sqlInterface = sqlInterface;
        this.r2dTranslator = r2dTranslator;
        this.didCursor = didCursor;
        this.dsl = dsl;
        this.metaDatabase = metaDatabase;
        this.metaCollection = metaCollection;
    }

    @Override
    public boolean hasNext() {
        return didCursor.hasNext();
    }

    @Override
    public ToroDocument next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return getNextBatch(1).get(0);
    }

    @Override
    public List<ToroDocument> getRemaining() {
        List<ToroDocument> allDocuments = new ArrayList<>();

        List<ToroDocument> readedDocuments;
        do {
            readedDocuments = getNextBatch(BATCH_SIZE);
            allDocuments.addAll(readedDocuments);
        } while(readedDocuments.isEmpty());

        return allDocuments;
    }

    @Override
    public List<ToroDocument> getNextBatch(int maxResults) {
        Preconditions.checkArgument(maxResults > 0, "max results must be at least 1, but "+maxResults+" was recived");

        if (!didCursor.hasNext()) {
            return Collections.emptyList();
        }

        
        try (DocPartResultBatch batch = sqlInterface.getReadInterface().getCollectionResultSets(
                    dsl, metaDatabase, metaCollection, didCursor, maxResults)) {
            return r2dTranslator.translate(batch);
        } catch(SQLException ex) {
            sqlInterface.getErrorHandler().handleRollbackException(Context.FETCH, ex);
            
            throw new SystemException(ex);
        }
    }

    @Override
    public void forEachRemaining(Consumer<? super ToroDocument> action) {
        getRemaining().forEach(action);
    }

    @Override
    public void close() {
        didCursor.close();
    }
}
