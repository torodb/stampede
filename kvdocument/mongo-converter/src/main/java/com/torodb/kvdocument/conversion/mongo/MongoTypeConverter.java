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


package com.torodb.kvdocument.conversion.mongo;

import com.torodb.kvdocument.types.*;

/**
 *
 */
public class MongoTypeConverter {
    
    private MongoTypeConverter() {}
    
    public static DocType translateType(int mongoType) {
        switch (mongoType) {
            case 1:
                return DoubleType.INSTANCE;
            case 2:
                return StringType.INSTANCE;
            case 3:
                return ObjectType.INSTANCE;
            case 4:
                return new ArrayType(GenericType.INSTANCE);
            case 8:
                return BooleanType.INSTANCE;
            case 10:
                return NullType.INSTANCE;
            case 16:
                return IntegerType.INSTANCE;
            case 18:
                return LongType.INSTANCE;
            case 7:
                return TwelveBytesType.INSTANCE;
            case 9:
                return DateTimeType.INSTANCE;
            case 11:
                return PatternType.INSTANCE;
            case 17:
            case 15:
            case 14:
            case 13:
            case 5:
            case 6:
            case 255:
            case 127:
                throw new UnsupportedOperationException("The type "+toStringMongoType(mongoType)+ " is not supported");
            default:
                throw new IllegalArgumentException("The mongodb type '"+mongoType+"' is not recognized");
        }
    }
    
    public static String toStringMongoType(int mongoType) {
        switch (mongoType) {
            case 1:
                return "double";
            case 2:
                return "string";
            case 3:
                return "object";
            case 4:
                return "array";
            case 8:
                return "boolean";
            case 10:
                return "null";
            case 16:
                return "integer";
            case 18:
                return "double";

            case 17:
                return "timestamp";
            case 15:
                return "JavaScript (with scope)";
            case 14:
                return "symbol";
            case 13:
                return "JavaScript";
            case 11:
                return "RegExp";
            case 9:
                return "Date";
            case 7:
                return "Object id";
            case 5:
                return "binary data";
            case 6:
                return "undefined";
            case 255:
                return "Min key";
            case 127:
                return "Max key";
            default:
                throw new IllegalArgumentException("The mongodb type '"+mongoType+"' is not recognized");
        }
    }
    
}
