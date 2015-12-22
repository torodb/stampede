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

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.core.subdocument.structure.StructureElementVisitor;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.sql.AutoCloser;
import com.torodb.torod.db.backends.tables.SubDocTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.*;
import org.jooq.impl.DSL;

import javax.annotation.Nonnull;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class DeleteDocuments {

    public static int execute(
            Configuration configuration, IndexStorage.CollectionSchema colSchema, Multimap<DocStructure, Integer> didsByStructure,
            boolean justOne, @Nonnull DatabaseInterface databaseInterface
    ) {
        Multimap<DocStructure, Integer> didsByStructureToDelete;
        if (didsByStructure.isEmpty()) {
            return 0;
        }

        if (justOne) {
            didsByStructureToDelete
                    = MultimapBuilder.hashKeys(1).arrayListValues(1).build();

            Map.Entry<DocStructure, Integer> aEntry
                    = didsByStructure.entries().iterator().next();

            didsByStructureToDelete.put(aEntry.getKey(), aEntry.getValue());
        }
        else {
            didsByStructureToDelete = didsByStructure;
        }

        try {
            return execute(configuration, colSchema, didsByStructureToDelete, databaseInterface);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static int execute(
            Configuration configuration, IndexStorage.CollectionSchema colSchema, Multimap<DocStructure, Integer> didsByStructure,
            @Nonnull DatabaseInterface databaseInterface
    ) throws SQLException {
        TableProvider tableProvider = new TableProvider(colSchema);

        DSLContext dsl = DSL.using(configuration);

        Set<SubDocTable> tables = Sets.newHashSet();
        for (DocStructure structure : didsByStructure.keySet()) {
            tables.clear();
            structure.accept(tableProvider, tables);

            executeDeleteSubDocuments(dsl, tables, didsByStructure.get(structure), databaseInterface);
        }

        Set<Integer> dids = Sets.newHashSet(didsByStructure.values());
        return executeDeleteRoots(dsl, colSchema, dids, databaseInterface);
    }

    private static void executeDeleteSubDocuments(
            DSLContext dsl, Set<SubDocTable> tables, Collection<Integer> dids,
            @Nonnull DatabaseInterface databaseInterface
    ) {
        
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        try {
            for (SubDocTable table : tables) {
                delete(connection, table.getSchema(), table, dids, databaseInterface);
            }
        }
        catch (SQLException ex) {
            //TODO: change exceptions
            throw new RuntimeException(ex);
        }
        finally {
            connectionProvider.release(connection);
        }
    }
    
    private static int executeDeleteRoots(
            DSLContext dsl, IndexStorage.CollectionSchema colSchema, Collection<Integer> dids,
            @Nonnull DatabaseInterface databaseInterface
    ) throws SQLException {
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        
        try {
            Table<Record> rootTable = DSL.tableByName(colSchema.getName(), "root");

            return delete(connection, colSchema, rootTable, dids, databaseInterface);
        } finally {
            connectionProvider.release(connection);
        }
    }
    
    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    private static int delete(
            Connection connection, Schema schema, Table table, Collection<Integer> dids,
            @Nonnull DatabaseInterface databaseInterface
    ) throws SQLException {
        final int maxInArray = (2 << 15) - 1; // = 2^16 -1 = 65535

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(
                    databaseInterface.deleteDidsStatement(
                            schema.getName(), table.getName(), SubDocTable.DID_COLUMN_NAME
                    )
            );

            Integer[] didsToDelete = dids.toArray(new Integer[dids.size()]);

            int i = 0;
            while (i < didsToDelete.length) {
                int toIndex = Math.min(i + maxInArray, didsToDelete.length);
                Integer[] subDids
                        = Arrays.copyOfRange(didsToDelete, i, toIndex);

                Array arr
                    = connection.createArrayOf("integer", subDids);
                ps.setArray(1, arr);
                ps.addBatch();

                i = toIndex;
            }
            int[] executeResult = ps.executeBatch();
            int result = 0;
            for (i = 0; i < executeResult.length; i++) {
                int iestResult = executeResult[i];
                if (iestResult >= 0) {
                    result += iestResult;
                }
            }
            return result;
        } finally {
            AutoCloser.close(ps);
        }
    }

    private static class TableProvider implements
            StructureElementVisitor<Void, Collection<SubDocTable>> {

        private final IndexStorage.CollectionSchema colSchema;

        public TableProvider(IndexStorage.CollectionSchema colSchema) {
            this.colSchema = colSchema;
        }

        @Override
        public Void visit(DocStructure structure, Collection<SubDocTable> arg) {
            arg.add(colSchema.getSubDocTable(structure.getType()));

            for (StructureElement child : structure.getElements().values()) {
                child.accept(this, arg);
            }

            return null;
        }

        @Override
        public Void visit(ArrayStructure structure, Collection<SubDocTable> arg) {
            for (StructureElement child : structure.getElements().values()) {
                child.accept(this, arg);
            }

            return null;
        }
    }
}
