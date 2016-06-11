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

package com.torodb.torod.db.backends.meta.routines;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.Configuration;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.DatabaseInterface.FindDocsSelectStatementRow;
import com.torodb.torod.db.backends.converters.SplitDocumentConverter;
import com.torodb.torod.db.backends.meta.CollectionSchema;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Singleton
public class QueryRoutine {

    private final SplitDocumentConverter splitDocumentConverter;

    @Inject
    protected QueryRoutine(SplitDocumentConverter splitDocumentConverter) {
        this.splitDocumentConverter = splitDocumentConverter;
    }

    @SuppressFBWarnings(
            value = {
                "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING",
                "OBL_UNSATISFIED_OBLIGATION"
            })
    public List<SplitDocument> execute(
            Configuration configuration, CollectionSchema colSchema, Integer[] requestedDocs, Projection projection,
            @Nonnull DatabaseInterface databaseInterface
    ) {

        if (requestedDocs.length == 0) {
            return Collections.emptyList();
        }

        Connection c;

        c = configuration.connectionProvider().acquire();
        try (PreparedStatement ps = c.prepareStatement(databaseInterface.findDocsSelectStatement())) {

            databaseInterface.setFindDocsSelectStatementParameters(colSchema, requestedDocs, projection, c, ps);

            return translateDocuments(colSchema, requestedDocs.length, ps, databaseInterface);
        } catch (SQLException ex) {
            //TODO: Study exceptions
            throw new RuntimeException(ex);
        } finally {
            configuration.connectionProvider().release(c);
        }
    }

    @Nonnull
    private List<SplitDocument> translateDocuments(CollectionSchema colSchema, int expectedDocs, PreparedStatement ps,
            @Nonnull DatabaseInterface databaseInterface
            ) {
        try {
            List<SplitDocument> result = Lists.newArrayListWithCapacity(expectedDocs);

            Table<Integer, Integer, String> docInfo = HashBasedTable.create();

            Integer lastDocId = null;
            Integer structureId = null;
            
            ResultSet rs = databaseInterface.getFindDocsSelectStatementResultSet(ps);
            
            while (rs.next()) {
                FindDocsSelectStatementRow findDocsSelectStatementRow = databaseInterface.getFindDocsSelectStatementRow(rs);
                int docId = findDocsSelectStatementRow.getDocId();
                if (lastDocId == null || lastDocId != docId) {
                    if (lastDocId != null) { //if this is not the first iteration
                        SplitDocument doc = processDocument(colSchema, lastDocId, structureId, docInfo);
                        result.add(doc);
                    }
                    lastDocId = docId;
                    structureId = null;
                    assert docInfo.isEmpty();
                }

                Integer typeId = findDocsSelectStatementRow.getTypeId();
                Integer index = findDocsSelectStatementRow.getindex();
                String json = findDocsSelectStatementRow.getJson();

                if (findDocsSelectStatementRow.isSubdocument()) { //subdocument
                    docInfo.put((Integer) typeId, (Integer) index, json);
                } else { //metainfo
                    structureId = (Integer) index;
                }
            }
            if (lastDocId != null) {
                SplitDocument doc = processDocument(colSchema, lastDocId, structureId, docInfo);
                result.add(doc);
            }

            return result;
        } catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    private SplitDocument processDocument(
            CollectionSchema colSchema,
            int lastDocId,
            Integer structureId,
            Table<Integer, Integer, String> docInfo) {

        if (structureId == null) {
            //TODO: Change exception
            throw new RuntimeException("Structure id was expected");
        }

        return splitDocumentConverter.convert(colSchema, lastDocId, structureId, docInfo);
    }
}
