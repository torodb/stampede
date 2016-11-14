/*
 * ToroDB - ToroDB-poc: Backend PostgreSQL
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.postgresql.converters.jooq;

import org.jooq.util.postgres.PostgresDataType;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.sql.LongSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.types.LongType;
import com.torodb.kvdocument.values.KVLong;

/**
 *
 */
public class LongValueConverter implements KVValueConverter<Long, Long, KVLong>{
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVLong> TYPE = DataTypeForKV.from(PostgresDataType.INT8, new LongValueConverter());

    @Override
    public KVType getErasuredType() {
        return LongType.INSTANCE;
    }

    @Override
    public KVLong from(Long databaseObject) {
        return KVLong.of(databaseObject);
    }

    @Override
    public Long to(KVLong userObject) {
        return userObject.getValue();
    }

    @Override
    public Class<Long> fromType() {
        return Long.class;
    }

    @Override
    public Class<KVLong> toType() {
        return KVLong.class;
    }

    @Override
    public SqlBinding<Long> getSqlBinding() {
        return LongSqlBinding.INSTANCE;
    }
    
}
