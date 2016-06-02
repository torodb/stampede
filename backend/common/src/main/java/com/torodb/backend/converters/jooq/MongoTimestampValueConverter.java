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
 *     Copyright (c) 2016, 8Kdata Technology
 *     
 */

package com.torodb.backend.converters.jooq;

import com.torodb.backend.udt.MongoTimestampUDT;
import com.torodb.backend.udt.record.MongoTimestampRecord;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.types.MongoTimestampType;
import com.torodb.kvdocument.values.KVMongoTimestamp;
import com.torodb.kvdocument.values.heap.DefaultKVMongoTimestamp;

/**
 *
 */
public class MongoTimestampValueConverter implements
        KVValueConverter<MongoTimestampRecord, KVMongoTimestamp> {

    private static final long serialVersionUID = 1251948867583783920L;

    public static final DataTypeForKV<KVMongoTimestamp> TYPE = DataTypeForKV.from(MongoTimestampUDT.MONGO_TIMESTAMP.getDataType(), new MongoTimestampValueConverter());

    @Override
    public KVType getErasuredType() {
        return MongoTimestampType.INSTANCE;
    }

    @Override
    public KVMongoTimestamp from(MongoTimestampRecord databaseObject) {
        return new DefaultKVMongoTimestamp(databaseObject.getSecs(), databaseObject.getCounter());
    }

    @Override
    public MongoTimestampRecord to(KVMongoTimestamp userObject) {
        return new MongoTimestampRecord(userObject.getSecondsSinceEpoch(), userObject.getOrdinal());
    }

    @Override
    public Class<MongoTimestampRecord> fromType() {
        return MongoTimestampRecord.class;
    }

    @Override
    public Class<KVMongoTimestamp> toType() {
        return KVMongoTimestamp.class;
    }
}
