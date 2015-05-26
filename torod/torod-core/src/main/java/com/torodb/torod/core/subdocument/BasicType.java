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

package com.torodb.torod.core.subdocument;

import com.torodb.kvdocument.types.*;
import javax.annotation.Nonnull;

/**
 * Elements of this enum are like {@link Type} but arrays doesn't have element type, so two arrays are
 * indistinguishable.
 * <p>
 */
public enum BasicType {

    NULL,
    BOOLEAN,
    INTEGER,
    LONG,
    DOUBLE,
    STRING,
    ARRAY,
    TWELVE_BYTES,
    DATE,
    DATETIME,
    TIME,
    PATTERN;
    
    @Nonnull
    public static BasicType fromDocType(DocType docType) {
        if (docType instanceof NullType) {
            return NULL;
        }
        if (docType instanceof BooleanType) {
            return BOOLEAN;
        }
        if (docType instanceof IntegerType) {
            return INTEGER;
        }
        if (docType instanceof LongType) {
            return LONG;
        }
        if (docType instanceof DoubleType) {
            return DOUBLE;
        }
        if (docType instanceof StringType) {
            return STRING;
        }
        if (docType instanceof ArrayType) {
            return ARRAY;
        }
        if (docType instanceof TwelveBytesType) {
            return TWELVE_BYTES;
        }
        if (docType instanceof DateTimeType) {
            return DATETIME;
        }
        if (docType instanceof DateType) {
            return TIME;
        }
        if (docType instanceof TimeType) {
            return DATE;
        }
        if (docType instanceof PatternType) {
            return PATTERN;
        }
        throw new IllegalArgumentException(docType + " does not correspond with a basic type");
    }
}
