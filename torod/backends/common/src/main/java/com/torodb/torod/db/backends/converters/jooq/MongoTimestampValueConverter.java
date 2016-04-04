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

package com.torodb.torod.db.backends.converters.jooq;

import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarMongoTimestamp;
import com.torodb.torod.core.subdocument.values.heap.DefaultScalarMongoTimestamp;
import com.torodb.torod.db.backends.udt.MongoTimestampUDT;
import com.torodb.torod.db.backends.udt.records.MongoTimestampRecord;

/**
 *
 */
public class MongoTimestampValueConverter implements
        SubdocValueConverter<MongoTimestampRecord, ScalarMongoTimestamp> {

    private static final long serialVersionUID = 1251948867583783920L;

    public static final DataTypeForScalar<ScalarMongoTimestamp> TYPE = DataTypeForScalar.from(MongoTimestampUDT.MONGO_TIMESTAMP.getDataType(), new MongoTimestampValueConverter());

    @Override
    public ScalarType getErasuredType() {
        return ScalarType.MONGO_TIMESTAMP;
    }

    @Override
    public ScalarMongoTimestamp from(MongoTimestampRecord databaseObject) {
        return new DefaultScalarMongoTimestamp(databaseObject.getSecs(), databaseObject.getCounter());
    }

    @Override
    public MongoTimestampRecord to(ScalarMongoTimestamp userObject) {
        return new MongoTimestampRecord(userObject.getSecondsSinceEpoch(), userObject.getOrdinal());
    }

    @Override
    public Class<MongoTimestampRecord> fromType() {
        return MongoTimestampRecord.class;
    }

    @Override
    public Class<ScalarMongoTimestamp> toType() {
        return ScalarMongoTimestamp.class;
    }
}
