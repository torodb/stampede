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

package com.torodb.backend.derby.converters.jooq;

import java.sql.Timestamp;
import java.time.Instant;

import org.jooq.impl.SQLDataType;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.kvdocument.types.InstantType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVInstant;
import com.torodb.kvdocument.values.heap.InstantKVInstant;

/**
 *
 */
public class InstantValueConverter implements KVValueConverter<Timestamp, KVInstant>{
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVInstant> TYPE = DataTypeForKV.from(SQLDataType.TIMESTAMP, new InstantValueConverter());

    @Override
    public KVType getErasuredType() {
        return InstantType.INSTANCE;
    }

    @Override
    public KVInstant from(Timestamp databaseObject) {
        return new InstantKVInstant(
                Instant.ofEpochSecond(databaseObject.getTime() / 1000, databaseObject.getNanos())
            );
    }

    @Override
    public Timestamp to(KVInstant userObject) {
        Instant instant = userObject.getValue();
        try {
            Timestamp ts = new Timestamp(instant.getEpochSecond() * 1000);
            ts.setNanos(instant.getNano());
            return ts;
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @Override
    public Class<Timestamp> fromType() {
        return Timestamp.class;
    }

    @Override
    public Class<KVInstant> toType() {
        return KVInstant.class;
    }
    
}
