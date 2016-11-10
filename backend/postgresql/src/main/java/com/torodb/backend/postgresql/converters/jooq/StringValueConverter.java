/*
 * MongoWP - ToroDB-poc: Backend PostgreSQL
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

import org.jooq.impl.SQLDataType;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.postgresql.converters.sql.StringSqlBinding;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.types.StringType;
import com.torodb.kvdocument.values.KVString;
import com.torodb.kvdocument.values.heap.StringKVString;

/**
 *
 */
public class StringValueConverter implements KVValueConverter<String, String, KVString>{
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVString> TYPE = DataTypeForKV.from(SQLDataType.VARCHAR, new StringValueConverter());

    @Override
    public KVType getErasuredType() {
        return StringType.INSTANCE;
    }

    @Override
    public KVString from(String databaseObject) {
        return new StringKVString(databaseObject);
    }

    @Override
    public String to(KVString userObject) {
        return userObject.getValue();
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }

    @Override
    public Class<KVString> toType() {
        return KVString.class;
    }

    @Override
    public SqlBinding<String> getSqlBinding() {
        return StringSqlBinding.INSTANCE;
    }
    
}
