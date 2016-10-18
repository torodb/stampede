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

import java.sql.Time;

import org.jooq.impl.SQLDataType;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.converters.sql.TimeSqlBinding;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.types.TimeType;
import com.torodb.kvdocument.values.KVTime;
import com.torodb.kvdocument.values.heap.LocalTimeKVTime;

/**
 *
 */
public class TimeValueConverter implements KVValueConverter<Time, Time, KVTime>{
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVTime> TYPE = DataTypeForKV.from(SQLDataType.TIME, new TimeValueConverter());

    @Override
    public KVType getErasuredType() {
        return TimeType.INSTANCE;
    }

    @Override
    public KVTime from(Time databaseObject) {
        return new LocalTimeKVTime(databaseObject.toLocalTime());
    }

    @Override
    public Time to(KVTime userObject) {
        return Time.valueOf(userObject.getValue());
    }

    @Override
    public Class<Time> fromType() {
        return Time.class;
    }

    @Override
    public Class<KVTime> toType() {
        return KVTime.class;
    }

    @Override
    public SqlBinding<Time> getSqlBinding() {
        return TimeSqlBinding.INSTANCE;
    }
    
}