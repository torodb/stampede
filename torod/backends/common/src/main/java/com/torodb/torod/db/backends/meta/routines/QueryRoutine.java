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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.SplitDocumentConverter;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.tables.SubDocTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Configuration;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class QueryRoutine {

    public static final String DOC_ID = "did";
    public static final String TYPE_ID = "typeid";
    public static final String INDEX = "index";
    public static final String _JSON = "_json";

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
            Configuration configuration, IndexStorage.CollectionSchema colSchema, Integer[] requestedDocs, Projection projection,
            @Nonnull DatabaseInterface databaseInterface
    ) {

        if (requestedDocs.length == 0) {
            return Collections.emptyList();
        }

        Connection c = null;

        c = configuration.connectionProvider().acquire();
        try (PreparedStatement ps = c.prepareStatement(databaseInterface.findDocsSelectStatement(
                DOC_ID,
                TYPE_ID,
                INDEX,
                _JSON))) {

            ps.setString(1, colSchema.getName());

            ps.setArray(2, c.createArrayOf("integer", requestedDocs));

            Integer[] requiredTables = requiredTables(colSchema, projection);
            ps.setArray(3, c.createArrayOf("integer", requiredTables));

            return translateDocuments(colSchema, requestedDocs.length, ps.executeQuery());
        } catch (SQLException ex) {
            //TODO: Study exceptions
            throw new RuntimeException(ex);
        } finally {
            configuration.connectionProvider().release(c);
        }
    }

    private Integer[] requiredTables(IndexStorage.CollectionSchema colSchema, Projection projection) {
        Collection<SubDocTable> subDocTables = colSchema.getSubDocTables();

        Integer[] result = new Integer[subDocTables.size()];
        int i = 0;
        for (SubDocTable subDocTable : subDocTables) {
            result[i] = subDocTable.getTypeId();
            i++;
        }
        return result;
    }

    @Nonnull
    private List<SplitDocument> translateDocuments(IndexStorage.CollectionSchema colSchema, int expectedDocs, ResultSet rs) {
        try {
            List<SplitDocument> result = Lists.newArrayListWithCapacity(expectedDocs);

            Table<Integer, Integer, String> docInfo = HashBasedTable.create();

            Integer lastDocId = null;
            Integer structureId = null;
            while (rs.next()) {
                int docId = rs.getInt(DOC_ID);
                if (lastDocId == null || lastDocId != docId) {
                    if (lastDocId != null) { //if this is not the first iteration
                        SplitDocument doc = processDocument(colSchema, lastDocId, structureId, docInfo);
                        result.add(doc);
                    }
                    lastDocId = docId;
                    structureId = null;
                    assert docInfo.isEmpty();
                }

                Object typeId = rs.getObject(TYPE_ID);
                Object index = rs.getObject(INDEX);
                String json = rs.getString(_JSON);

                if (typeId != null) { //subdocument
                    assert typeId instanceof Integer;
                    assert index == null || index instanceof Integer;
                    assert json != null;

                    if (index == null) {
                        index = 0;
                    }

                    docInfo.put((Integer) typeId, (Integer) index, json);
                } else { //metainfo
                    assert index != null;
                    assert json == null;

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
            IndexStorage.CollectionSchema colSchema,
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
