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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.google.common.base.Preconditions;
import com.torodb.backend.interfaces.ErrorHandlerInterface.Context;
import com.torodb.core.backend.BackendCursor;
import com.torodb.core.backend.DidCursor;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;


/**
 *
 */
public class DefaultCursor implements BackendCursor {
    
    private static final int BATCH_SIZE = 1000;

    private final SqlInterface sqlInterface;
    private final R2DTranslator r2dTranslator;
    private final DidCursor didCursor;
    private final DSLContext dsl;
    private final MetaDatabase metaDatabase;
    private final MetaCollection metaCollection;
    
    /**
     * @param r2dTranslator
     * @param didCursor
     */
    public DefaultCursor(
            @Nonnull SqlInterface sqlInterface,
            @Nonnull R2DTranslator r2dTranslator,
            @Nonnull DidCursor didCursor,
            @Nonnull DSLContext dsl,
            @Nonnull MetaDatabase metaDatabase,
            @Nonnull MetaCollection metaCollection
            ) {
        this.sqlInterface = sqlInterface;
        this.r2dTranslator = r2dTranslator;
        this.didCursor = didCursor;
        this.dsl = dsl;
        this.metaDatabase = metaDatabase;
        this.metaCollection = metaCollection;
    }
    
    @Override
    public Collection<ToroDocument> readDocuments(int maxResults) {
        Preconditions.checkArgument(maxResults > 0, "max results must be at least 1, but "+maxResults+" was recived");
        
        List<Integer> requiredDocs = new ArrayList<>();
        for (int i = 0; i < maxResults && didCursor.next(); i++) {
            requiredDocs.add(didCursor.get());
        }
        
        if (requiredDocs.isEmpty()) {
            return Collections.emptyList();
        }
        
        DocPartResults<ResultSet> docPartResults;
        try {
            docPartResults = sqlInterface.getCollectionResultSets(
                    dsl, metaDatabase, metaCollection, requiredDocs);
        } catch(SQLException ex) {
            sqlInterface.handleRollbackException(Context.fetch, ex);
            
            throw new SystemException(ex);
        }
        
        return r2dTranslator.translate(docPartResults);
    }

    @Override
    public Collection<ToroDocument> readAllDocuments() {
        List<ToroDocument> allDocuments = new ArrayList<>();
        
        Collection<ToroDocument> readedDocuments = Collections.emptyList();
        do {
            readedDocuments = readDocuments(BATCH_SIZE);
            allDocuments.addAll(readedDocuments);
        } while(readedDocuments.isEmpty());
        
        return allDocuments;
    }

    @Override
    public void close() {
        didCursor.close();
    }
}
