/*
 * ToroDB - ToroDB-poc: Core
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.core.transaction.metainf;

import com.torodb.kvdocument.types.*;

/**
 *
 */
public enum FieldType {
    BINARY,
    BOOLEAN,
    DATE,
    DOUBLE,
    INSTANT,
    INTEGER,
    LONG,
    MONGO_OBJECT_ID,
    MONGO_TIME_STAMP,
    NULL,
    STRING,
    TIME,
    CHILD;

    private static final FromKVTypeVisitor FROM_KVTYPE_VISITOR = new FromKVTypeVisitor();

    public static FieldType from(KVType type) {
        return type.accept(FROM_KVTYPE_VISITOR, null);
    }

    private static class FromKVTypeVisitor implements KVTypeVisitor<FieldType, Void> {

        @Override
        public FieldType visit(ArrayType type, Void arg) {
            return CHILD;
        }

        @Override
        public FieldType visit(BooleanType type, Void arg) {
            return BOOLEAN;
        }

        @Override
        public FieldType visit(DoubleType type, Void arg) {
            return DOUBLE;
        }

        @Override
        public FieldType visit(IntegerType type, Void arg) {
            return INTEGER;
        }

        @Override
        public FieldType visit(LongType type, Void arg) {
            return LONG;
        }

        @Override
        public FieldType visit(NullType type, Void arg) {
            return NULL;
        }

        @Override
        public FieldType visit(DocumentType type, Void arg) {
            return CHILD;
        }

        @Override
        public FieldType visit(StringType type, Void arg) {
            return STRING;
        }

        @Override
        public FieldType visit(GenericType type, Void arg) {
            throw new IllegalArgumentException("There is no " + FieldType.class.getSimpleName()
                    + " that represents a " + GenericType.class.getSimpleName());
        }

        @Override
        public FieldType visit(MongoObjectIdType type, Void arg) {
            return MONGO_OBJECT_ID;
        }

        @Override
        public FieldType visit(InstantType type, Void arg) {
            return INSTANT;
        }

        @Override
        public FieldType visit(DateType type, Void arg) {
            return DATE;
        }

        @Override
        public FieldType visit(TimeType type, Void arg) {
            return TIME;
        }

        @Override
        public FieldType visit(BinaryType type, Void arg) {
            return BINARY;
        }

        @Override
        public FieldType visit(NonExistentType type, Void arg) {
            throw new IllegalArgumentException("There is no " + FieldType.class.getSimpleName()
                    + " that represents a " + NonExistentType.class.getSimpleName());
        }

        @Override
        public FieldType visit(MongoTimestampType type, Void arg) {
            return MONGO_TIME_STAMP;
        }

    }
}
