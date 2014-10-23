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

package com.torodb.torod.db.postgresql.meta.routines;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.core.subdocument.structure.StructureElementVisitor;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.tables.SubDocTable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

/**
 *
 */
public class DeleteDocuments {

    public static int execute(Configuration configuration, CollectionSchema colSchema, Multimap<DocStructure, Integer> didsByStructure, boolean justOne) {
        Multimap<DocStructure, Integer> didsByStructureToDelete;
        if (didsByStructure.isEmpty()) {
            return 0;
        }
        
        if (justOne) {
            didsByStructureToDelete = MultimapBuilder.hashKeys(1).arrayListValues(1).build();
            
            Map.Entry<DocStructure, Integer> aEntry = didsByStructure.entries().iterator().next();
            
            didsByStructureToDelete.put(aEntry.getKey(), aEntry.getValue());
        }
        else {
            didsByStructureToDelete = didsByStructure;
        }
        
        return execute(configuration, colSchema, didsByStructureToDelete);
    }
    
    public static int execute(Configuration configuration, CollectionSchema colSchema, Multimap<DocStructure, Integer> didsByStructure) {
        TableProvider tableProvider = new TableProvider(colSchema);

        DSLContext dsl = DSL.using(configuration);

        Set<SubDocTable> tables = Sets.newHashSet();
        for (DocStructure structure : didsByStructure.keySet()) {
            tables.clear();
            structure.accept(tableProvider, tables);

            executeDeleteSubDocuments(dsl, tables, didsByStructure.get(structure));
        }

        Set<Integer> dids = Sets.newHashSet(didsByStructure.values());
        return executeDeleteRoots(dsl, colSchema, dids);
    }

    private static void executeDeleteSubDocuments(DSLContext dsl, Set<SubDocTable> tables, Collection<Integer> dids) {
        for (SubDocTable table : tables) {
            dsl.delete(table)
                    .where(
                            table.getDidColumn().in(dids)
                    )
                    .execute();
        }
    }

    private static int executeDeleteRoots(DSLContext dsl, CollectionSchema colSchema, Collection<Integer> dids) {
        Field<Integer> idField = DSL.field("did", SQLDataType.INTEGER.nullable(false));
        Table<Record> rootTable = DSL.tableByName(colSchema.getName(), "root");

        return dsl.delete(rootTable)
                .where(
                        idField.in(dids)
                )
                .execute();
    }

    private static class TableProvider implements StructureElementVisitor<Void, Collection<SubDocTable>> {

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
