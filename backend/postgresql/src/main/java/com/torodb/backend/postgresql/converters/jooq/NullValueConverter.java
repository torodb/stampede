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


package com.torodb.backend.postgresql.converters.jooq;

import java.sql.Types;

import org.jooq.util.postgres.PostgresDataType;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.sql.BooleanSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.values.KVNull;

/**
 *
 */
public class NullValueConverter implements KVValueConverter<Boolean, Boolean, KVNull>{
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVNull> TYPE = DataTypeForKV.from(PostgresDataType.BOOL, new NullValueConverter(), Types.BIT);

    @Override
    public KVType getErasuredType() {
        return NullType.INSTANCE;
    }

    @Override
    public KVNull from(Boolean databaseObject) {
        return KVNull.getInstance();
    }

    @Override
    public Boolean to(KVNull userObject) {
        return Boolean.TRUE;
    }

    @Override
    public Class<Boolean> fromType() {
        return Boolean.class;
    }

    @Override
    public Class<KVNull> toType() {
        return KVNull.class;
    }

    @Override
    public SqlBinding<Boolean> getSqlBinding() {
        return BooleanSqlBinding.INSTANCE;
    }
    
}
