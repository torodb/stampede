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

package com.torodb.torod.db.backends.greenplum.meta;

import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;

import com.torodb.torod.db.backends.converters.StructureConverter;
import com.torodb.torod.db.backends.meta.AbstractStructuresCache;
import com.torodb.torod.db.backends.meta.CollectionSchema;

/**
 *
 */
public class GreenplumStructuresCache extends AbstractStructuresCache {

    private static final long serialVersionUID = 1L;

    private static final Field<Integer> sidField = DSL.field("sid", SQLDataType.INTEGER);
    private static final Field<String> structuresField = DSL.field("_structure", new DefaultDataType<String>(null, String.class, "text"));

    public GreenplumStructuresCache(
            CollectionSchema colSchema,
            String schemaName, 
            StructureConverter converter) {
        super(colSchema, schemaName, converter);
    }

    @Override
    protected Field<Integer> getSidField() {
        return sidField;
    }

    @Override
    protected Field<String> getStructuresField() {
        return structuresField;
    }
}
