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


package com.torodb.torod.db.backends.postgresql.converters;

import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.db.backends.converters.ScalarTypeToSqlType;
import com.torodb.torod.db.backends.meta.TorodbSchema;
import com.torodb.torod.db.backends.udt.MongoObjectIdUDT;
import com.torodb.torod.db.backends.udt.MongoTimestampUDT;
import java.sql.Types;

/**
 *
 */
public class PostgreSQLScalarTypeToSqlType implements ScalarTypeToSqlType {

    public static final String ARRAY_TYPE = "jsonb";
    public static final String MONGO_OBJECT_ID_TYPE = "mongo_object_id";
    public static final String MONGO_TIMESTAMP_TYPE = "mongo_timestamp";
    public static final String FULL_MONGO_OBJECT_ID_TYPE = "\"" + TorodbSchema.TORODB_SCHEMA + "\".\"" + MONGO_OBJECT_ID_TYPE + "\"";
    public static final String FULL_MONGO_TIMESTAMP_TYPE = "\"" + TorodbSchema.TORODB_SCHEMA + "\".\"" + MONGO_TIMESTAMP_TYPE + "\"";

    private static final long serialVersionUID = 385628201;

    @Override
    public ScalarType toScalarType(
            String columnName,
            int jdbcIntType,
            String jdbcStringType
    ) {
        switch (jdbcIntType) {
            case Types.BIGINT:
                return ScalarType.LONG;
            case Types.BOOLEAN:
            case Types.BIT:
                return ScalarType.BOOLEAN;
            case Types.DATE:
                return ScalarType.DATE;
            case Types.DOUBLE:
                return ScalarType.DOUBLE;
            case Types.INTEGER:
                return ScalarType.INTEGER;
            case Types.SMALLINT:
            case Types.NULL:
                return ScalarType.NULL;
            case Types.TIME:
                return ScalarType.TIME;
            case Types.TIMESTAMP:
                return ScalarType.INSTANT;
            case Types.VARCHAR:
                return ScalarType.STRING;
            case Types.BINARY:
                return ScalarType.BINARY;
            case Types.OTHER:
                if (jdbcStringType.equals(ARRAY_TYPE)) {
                    return ScalarType.ARRAY;
                }
                break;
            case Types.DISTINCT: {
                if (jdbcStringType.equals(FULL_MONGO_OBJECT_ID_TYPE)
                        || jdbcStringType.equals(MongoObjectIdUDT.MONGO_OBJECT_ID.getName())) {
                    return ScalarType.MONGO_OBJECT_ID;
                }
                if (jdbcStringType.equals(FULL_MONGO_TIMESTAMP_TYPE)
                        || jdbcStringType.equals(MongoTimestampUDT.MONGO_TIMESTAMP.getName())) {
                    return ScalarType.MONGO_TIMESTAMP;
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
