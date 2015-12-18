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
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.db.backends.converters.BasicTypeToSqlType;
import com.torodb.torod.db.backends.udt.TwelveBytesUDT;

import java.sql.Types;

/**
 *
 */
public class PostgresBasicTypeToSqlType implements BasicTypeToSqlType {

    private static final long serialVersionUID = 385628201;

    @Override
    public BasicType toBasicType(
            String columnName,
            int jdbcIntType,
            String jdbcStringType
    ) {
        switch (jdbcIntType) {
            case Types.BIGINT:
                return BasicType.LONG;
            case Types.BOOLEAN:
            case Types.BIT:
                return BasicType.BOOLEAN;
            case Types.DATE:
                return BasicType.DATE;
            case Types.DOUBLE:
                return BasicType.DOUBLE;
            case Types.INTEGER:
                return BasicType.INTEGER;
            case Types.SMALLINT:
            case Types.NULL:
                return BasicType.NULL;
            case Types.TIME:
                return BasicType.TIME;
            case Types.TIMESTAMP:
                return BasicType.DATETIME;
            case Types.VARCHAR:
                return BasicType.STRING;
            case Types.OTHER:
                if (jdbcStringType.equals("jsonb")) {
                    return BasicType.ARRAY;
                }
                break;
            case Types.DISTINCT: {
                if (jdbcStringType.equals("\"torodb\".\"twelve_bytes\"")
                        || jdbcStringType.equals(TwelveBytesUDT.TWELVE_BYTES.getName())) {
                    return BasicType.TWELVE_BYTES;
                }
                if (jdbcStringType.equals("\"torodb\".\"torodb_pattern\"")
                        || jdbcStringType.equals("torodb_pattern")) {
                    return BasicType.PATTERN;
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
