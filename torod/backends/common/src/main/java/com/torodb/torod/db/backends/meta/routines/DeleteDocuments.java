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
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.torodb.torod.core.connection.exceptions.RetryTransactionException;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.core.subdocument.structure.StructureElementVisitor;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.meta.CollectionSchema;
import com.torodb.torod.db.backends.tables.SubDocTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public class DeleteDocuments {

    public static int execute(
            Configuration configuration, CollectionSchema colSchema, Multimap<DocStructure, Integer> didsByStructure,
            boolean justOne, @Nonnull DatabaseInterface databaseInterface
    ) throws RetryTransactionException {
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
            Configuration configuration, CollectionSchema colSchema, Multimap<DocStructure, Integer> didsByStructure,
            @Nonnull DatabaseInterface databaseInterface
    ) throws SQLException, RetryTransactionException {
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
    ) throws RetryTransactionException {
        
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
            DSLContext dsl, CollectionSchema colSchema, Collection<Integer> dids,
            @Nonnull DatabaseInterface databaseInterface
    ) throws SQLException, RetryTransactionException {
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
    ) throws SQLException, RetryTransactionException {
        try (PreparedStatement ps = connection.prepareStatement(
                    databaseInterface.deleteDidsStatement(
                            schema.getName(), table.getName(), SubDocTable.DID_COLUMN_NAME
                    )
            )) {

            databaseInterface.setDeleteDidsStatementParameters(ps, dids);
            
            int[] executeResult = ps.executeBatch();
            int result = 0;
            for (int i = 0; i < executeResult.length; i++) {
                int iestResult = executeResult[i];
                if (iestResult >= 0) {
                    result += iestResult;
                }
            }
            return result;
        } catch(SQLException sqlException) {
            databaseInterface.handleRetryException(sqlException);
            
            throw sqlException;
        }
    }

    private static class TableProvider implements
            StructureElementVisitor<Void, Collection<SubDocTable>> {

        private final CollectionSchema colSchema;

        public TableProvider(CollectionSchema colSchema) {
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
