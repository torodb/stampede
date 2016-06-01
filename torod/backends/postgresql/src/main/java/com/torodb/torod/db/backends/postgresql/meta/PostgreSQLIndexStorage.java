/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General PublicSchema License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General PublicSchema License for more details.
 *
 *     You should have received a copy of the GNU Affero General PublicSchema License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */
package com.torodb.torod.db.backends.postgresql.meta;

import org.jooq.Record2;
import org.jooq.TableField;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;

import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.jooq.JSONBBinding;
import com.torodb.torod.db.backends.converters.json.ToroIndexToJsonConverter;
import com.torodb.torod.db.backends.meta.AbstractIndexStorage;
import com.torodb.torod.db.backends.meta.CollectionSchema;

/**
 *
 */
public class PostgreSQLIndexStorage extends AbstractIndexStorage {
    private static final long serialVersionUID = 1L;

    public PostgreSQLIndexStorage(String databaseName, CollectionSchema colSchema, DatabaseInterface databaseInterface) {
        super(databaseName, colSchema, databaseInterface);
    }

    @Override
    protected AbstractToroIndexTable createIndexTable(CollectionSchema colSchema,
            ToroIndexToJsonConverter indexToJsonConverter) {
        return new PostgreSQLToroIndexTable(colSchema, indexToJsonConverter);
    }

    private static class PostgreSQLToroIndexTable extends AbstractToroIndexTable {
        private static final long serialVersionUID = 1L;

        public PostgreSQLToroIndexTable(CollectionSchema colSchema, ToroIndexToJsonConverter indexToJsonConverter) {
            super(colSchema, indexToJsonConverter);
        }

        @Override
        protected TableField<Record2<String, NamedToroIndex>, String> createNameField() {
            return createField("name", SQLDataType.VARCHAR, this);
        }

        @Override
        protected TableField<Record2<String, NamedToroIndex>, NamedToroIndex> createIndexField(ToroIndexToJsonConverter indexToJsonConverter) {
            return createField("index", new DefaultDataType<>(null, String.class, "jsonb")
                    .asConvertedDataType(new JSONBBinding<>(indexToJsonConverter)), this, "");
        }
    }
}
