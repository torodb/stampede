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


package com.torodb.backend.postgresql.converters;

import java.sql.Types;

import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.mocks.KVTypeToSqlType;
import com.torodb.backend.mocks.ToroImplementationException;
import com.torodb.backend.udt.MongoObjectIdUDT;
import com.torodb.backend.udt.MongoTimestampUDT;
import com.torodb.kvdocument.types.BinaryType;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.types.DateType;
import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.InstantType;
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.types.LongType;
import com.torodb.kvdocument.types.MongoObjectIdType;
import com.torodb.kvdocument.types.MongoTimestampType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.types.StringType;
import com.torodb.kvdocument.types.TimeType;

/**
 *
 */
public class PostgreSQLKVTypeToSqlType implements KVTypeToSqlType {

    public static final String MONGO_OBJECT_ID_TYPE = "mongo_object_id";
    public static final String MONGO_TIMESTAMP_TYPE = "mongo_timestamp";

    private static final long serialVersionUID = 385628201;

    @Override
    public KVType toKVType(
            String columnName,
            int jdbcIntType,
            String jdbcStringType
    ) {
        switch (jdbcIntType) {
            case Types.BIGINT:
                return LongType.INSTANCE;
            case Types.BOOLEAN:
            case Types.BIT:
                return BooleanType.INSTANCE;
            case Types.DATE:
                return DateType.INSTANCE;
            case Types.DOUBLE:
                return DoubleType.INSTANCE;
            case Types.INTEGER:
                return IntegerType.INSTANCE;
            case Types.SMALLINT:
            case Types.NULL:
                return NullType.INSTANCE;
            case Types.TIME:
                return TimeType.INSTANCE;
            case Types.TIMESTAMP:
                return InstantType.INSTANCE;
            case Types.VARCHAR:
                return StringType.INSTANCE;
            case Types.BINARY:
                return BinaryType.INSTANCE;
            case Types.OTHER:
                break;
            case Types.DISTINCT: {
                if (jdbcStringType.equals("\"" + TorodbSchema.TORODB_SCHEMA + "\".\"" + MONGO_OBJECT_ID_TYPE + "\"")
                        || jdbcStringType.equals(MongoObjectIdUDT.MONGO_OBJECT_ID.getName())) {
                    return MongoObjectIdType.INSTANCE;
                }
                if (jdbcStringType.equals("\"" + TorodbSchema.TORODB_SCHEMA + "\".\"" + MONGO_TIMESTAMP_TYPE + "\"")
                        || jdbcStringType.equals(MongoTimestampUDT.MONGO_TIMESTAMP.getName())) {
                    return MongoTimestampType.INSTANCE;
                }
                break;
            }
        }
        throw new ToroImplementationException(
                "SQL type " + jdbcStringType + " (with int "
                + jdbcIntType + ") is not supported (column "
                + columnName + ")"
        );
    }

 }
