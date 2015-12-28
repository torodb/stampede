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

package com.torodb.torod.db.backends.meta;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.backends.converters.StructureConverter;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 *
 */
public class StructuresCache implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Field<Integer> sidField = DSL.field("sid", SQLDataType.INTEGER);
    private static final Field<String> structuresField = DSL.field("_structure", SQLDataType.VARCHAR);
    private final IndexStorage.CollectionSchema colSchema;
    private final StructureConverter converter;
    private final BiMap<Integer, DocStructure> structures;
    /**
     * Contains the first free available id
     */
    private volatile int idProvider;
    private transient Table<?> table;
    private transient Lock insertLock;
    private static final Logger LOG
            = Logger.getLogger(StructuresCache.class.getName());

    public StructuresCache(
            IndexStorage.CollectionSchema colSchema,
            String schemaName, 
            StructureConverter converter) {
        this.colSchema = colSchema;
        this.converter = converter;
        this.table = DSL.table(DSL.name(schemaName, "structures"));
        this.structures = HashBiMap.create(1000);
        this.insertLock = new ReentrantLock();
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {

        stream.defaultReadObject();

        insertLock = new ReentrantLock();
        table = DSL.table(DSL.name(colSchema.getName(), "structures"));
    }

    public void initialize(DSLContext dsl, Iterable<? extends Table> tables) {

        boolean structureTableExists = false;
        for (Table table1 : tables) {
            if (isStructureTable(table1)) {
                structureTableExists = true;
                break;
            }
        }
        idProvider = 0;

        if (structureTableExists) {

            Result<Record2<Integer, String>> fetch = dsl.select(sidField, structuresField)
                    .from(table)
                    .fetch();

            int maxId = Integer.MIN_VALUE;

            for (Record2<Integer, String> record : fetch) {
                DocStructure docStructure = converter.from(record.value2());

                structures.put(record.value1(), docStructure);

                if (record.value1() > maxId) {
                    maxId = record.value1();
                }
            }
            if (maxId == Integer.MIN_VALUE) {
                maxId = -1;
            }            
            idProvider = maxId + 1;
        }
    }

    private boolean isStructureTable(Table<?> t) {
        return t.getName().equals(table.getName());
    }

    @Nullable
    public DocStructure getStructure(Integer structureId) {
        return structures.get(structureId);
    }

    @Nullable
    public Integer getStructureId(DocStructure structure) {
        return structures.inverse().get(structure);
    }

    public int getOrCreateStructure(
            DocStructure structure, 
            DSLContext dsl, 
            NewStructureListener newStructureListener) {
        Integer id = structures.inverse().get(structure);

        if (id != null) {
            return id;
        }

        insertLock.lock();
        try {
            id = structures.inverse().get(structure);
            if (id == null) {
                id = createStructure(structure, dsl, newStructureListener);
            }
        } finally {
            insertLock.unlock();
        }

        assert id != null;
        return id;
    }
    
    public BiMap<Integer, DocStructure> getAllStructures() {
        return ImmutableBiMap.copyOf(structures);
    }
    
    private Integer createStructure(
            DocStructure structure, 
            DSLContext dsl,
            NewStructureListener newStructureListener) {
        Integer id = idProvider++;

        assert !structures.containsKey(id);

        dsl.insertInto(table)
                .set(sidField, id)
                .set(structuresField, converter.to(structure))
                .execute();

        structures.put(id, structure);
        
        newStructureListener.eventNewStructure(colSchema, structure);
        
        return id;
    }

    public static interface NewStructureListener {
        public void eventNewStructure(IndexStorage.CollectionSchema colSchema, DocStructure newStructure);
    }
}
