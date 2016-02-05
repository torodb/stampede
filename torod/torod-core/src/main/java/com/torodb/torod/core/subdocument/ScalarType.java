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
 * Elements of this enum are like {@link Type} but they do not contain documents and arrays doesn't
 * have element type, so two arrays are indistinguishable.
 * <p>
 */
public enum ScalarType {

    NULL,
    BOOLEAN,
    INTEGER,
    LONG,
    DOUBLE,
    STRING,
    ARRAY,
    MONGO_OBJECT_ID,
    MONGO_TIMESTAMP,
    DATE,
    INSTANT,
    TIME,
    BINARY;

    private static final FromDocTypeVisitor FROM_DOC_TYPE_VISITOR = new FromDocTypeVisitor();
    
    @Nonnull
    public static ScalarType fromDocType(KVType docType) {
        return docType.accept(FROM_DOC_TYPE_VISITOR, null);
    }

    private static class FromDocTypeVisitor implements KVTypeVisitor<ScalarType, Void> {

        @Override
        public ScalarType visit(ArrayType type, Void arg) {
            return ARRAY;
        }

        @Override
        public ScalarType visit(BooleanType type, Void arg) {
            return BOOLEAN;
        }

        @Override
        public ScalarType visit(DoubleType type, Void arg) {
            return DOUBLE;
        }

        @Override
        public ScalarType visit(IntegerType type, Void arg) {
            return INTEGER;
        }

        @Override
        public ScalarType visit(LongType type, Void arg) {
            return LONG;
        }

        @Override
        public ScalarType visit(NullType type, Void arg) {
            return NULL;
        }

        @Override
        public ScalarType visit(DocumentType type, Void arg) {
            throw new IllegalArgumentException(type + " does not correspond with a scalar type");
        }

        @Override
        public ScalarType visit(StringType type, Void arg) {
            return STRING;
        }

        @Override
        public ScalarType visit(GenericType type, Void arg) {
            throw new IllegalArgumentException(type + " does not correspond with a scalar type");
        }

        @Override
        public ScalarType visit(MongoObjectIdType type, Void arg) {
            return MONGO_OBJECT_ID;
        }

        @Override
        public ScalarType visit(InstantType type, Void arg) {
            return INSTANT;
        }

        @Override
        public ScalarType visit(DateType type, Void arg) {
            return DATE;
        }

        @Override
        public ScalarType visit(TimeType type, Void arg) {
            return TIME;
        }

        @Override
        public ScalarType visit(BinaryType type, Void arg) {
            return BINARY;
        }

        @Override
        public ScalarType visit(NonExistentType type, Void arg) {
            throw new IllegalArgumentException(type + " does not correspond with a scalar type");
        }

        @Override
        public ScalarType visit(MongoTimestampType type, Void arg) {
            return MONGO_TIMESTAMP;
        }

    }
}
