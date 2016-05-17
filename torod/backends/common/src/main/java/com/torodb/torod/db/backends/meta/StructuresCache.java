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

import java.io.Serializable;

import javax.annotation.Nullable;

import org.jooq.DSLContext;
import org.jooq.Table;

import com.google.common.collect.BiMap;
import com.torodb.torod.core.subdocument.structure.DocStructure;

/**
 *
 */
public interface StructuresCache extends Serializable {

    public void initialize(DSLContext dsl, Iterable<? extends Table> tables);

    @Nullable
    public DocStructure getStructure(Integer structureId);

    @Nullable
    public Integer getStructureId(DocStructure structure);

    public int getOrCreateStructure(
            DocStructure structure, 
            DSLContext dsl, 
            NewStructureListener newStructureListener);
    
    public BiMap<Integer, DocStructure> getAllStructures();

    public static interface NewStructureListener {
        public void eventNewStructure(CollectionSchema colSchema, DocStructure newStructure);
    }
}
