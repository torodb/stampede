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

package com.torodb.torod.core.language.querycriteria.utils;

import com.torodb.kvdocument.values.TwelveBytesValue;
import com.torodb.kvdocument.values.DateTimeValue;
import com.torodb.kvdocument.values.BooleanValue;
import com.torodb.kvdocument.values.StringValue;
import com.torodb.kvdocument.values.DateValue;
import com.torodb.kvdocument.values.DocValueVisitor;
import com.torodb.kvdocument.values.NullValue;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.kvdocument.values.IntegerValue;
import com.torodb.kvdocument.values.ArrayValue;
import com.torodb.kvdocument.values.TimeValue;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.values.DoubleValue;
import com.torodb.kvdocument.values.LongValue;
import com.google.common.collect.Lists;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.ContainsAttributesQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsEqualQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.subdocument.values.Value;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.ObjectType;
import com.torodb.kvdocument.values.*;
import java.util.LinkedList;
import java.util.Map;

/**
 *
 */
public class EqualFactory {

    public static QueryCriteria createEquality(
            AttributeReference attRef,
            DocValue docValue) {
        EqualityQueryFinder eqf = new EqualityQueryFinder(attRef);

        LinkedList<AttributeReference.Key> keys = Lists.newLinkedList();

        docValue.accept(eqf, keys);

        return eqf.getConjunctionBuilder().
                build();
    }

    public static QueryCriteria createEquality(
            AttributeReference attRef,
            ToroDocument doc) {
        return createEquality(attRef, doc.getRoot());
    }

    public static QueryCriteria createEquality(
            ToroDocument doc) {
        return createEquality(AttributeReference.EMPTY_REFERENCE, doc.getRoot());
    }

    private static class Converter implements
            DocValueVisitor<Value<?>, Void> {

        @Override
        public Value<?> visit(ObjectValue value,
                              Void arg) {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public Value<?> visit(
                ArrayValue value,
                Void arg) {
            com.torodb.torod.core.subdocument.values.ArrayValue.Builder withoutObjectsBuilder
                    = new com.torodb.torod.core.subdocument.values.ArrayValue.Builder();

            for (DocValue docValue : value) {
                if (docValue.getType().
                        equals(ObjectType.INSTANCE)) {
                    throw new IllegalArgumentException(
                            value + " contains objects, but arrays with "
                            + "objects are not directly translatable"
                    );
                }
                else {
                    Value<?> val = docValue.accept(this, null);
                    withoutObjectsBuilder.add(val);
                }
            }

            return withoutObjectsBuilder.build();
        }

        @Override
        public Value<?> visit(BooleanValue value,
                              Void arg) {
            if (value.getValue()) {
                return com.torodb.torod.core.subdocument.values.BooleanValue.TRUE;
            }
            else {
                return com.torodb.torod.core.subdocument.values.BooleanValue.FALSE;
            }
        }

        @Override
        public Value<?> visit(NullValue value,
                              Void arg) {
            return com.torodb.torod.core.subdocument.values.NullValue.INSTANCE;
        }

        @Override
        public Value<?> visit(IntegerValue value,
                              Void arg) {
            return new com.torodb.torod.core.subdocument.values.IntegerValue(
                    value.getValue());
        }

        @Override
        public Value<?> visit(LongValue value,
                              Void arg) {
            return new com.torodb.torod.core.subdocument.values.LongValue(value.
                    getValue());
        }

        @Override
        public Value<?> visit(DoubleValue value,
                              Void arg) {
            return new com.torodb.torod.core.subdocument.values.DoubleValue(
                    value.getValue());
        }

        @Override
        public Value<?> visit(StringValue value,
                              Void arg) {
            return new com.torodb.torod.core.subdocument.values.StringValue(
                    value.getValue());
        }

        @Override
        public Value<?> visit(TwelveBytesValue value,
                              Void arg) {
            return new com.torodb.torod.core.subdocument.values.TwelveBytesValue(
                    value.getValue()
            );
        }

        @Override
        public Value<?> visit(DateTimeValue value,
                              Void arg) {
            return new com.torodb.torod.core.subdocument.values.DateTimeValue(
                    value.getValue()
            );
        }

        @Override
        public Value<?> visit(DateValue value,
                              Void arg) {
            return new com.torodb.torod.core.subdocument.values.DateValue(
                    value.getValue()
            );
        }

        @Override
        public Value<?> visit(TimeValue value,
                              Void arg) {
            return new com.torodb.torod.core.subdocument.values.TimeValue(
                    value.getValue()
            );
        }

        @Override
        public Value<?> visit(PatternValue value, Void arg) {
            return new com.torodb.torod.core.subdocument.values.PatternValue(
                    value.getValue()
            );
        }

    }

    private static class EqualityQueryFinder implements
            DocValueVisitor<Void, LinkedList<AttributeReference.Key>> {

        private static final Converter converter = new Converter();
        private final ConjunctionBuilder conjunctionBuilder
                = new ConjunctionBuilder();
        private final AttributeReference basicAttRef;

        public EqualityQueryFinder(AttributeReference basicAttRef) {
            this.basicAttRef = basicAttRef;
        }

        public ConjunctionBuilder getConjunctionBuilder() {
            return conjunctionBuilder;
        }

        @Override
        public Void visit(
                ObjectValue value,
                LinkedList<AttributeReference.Key> arg) {
            for (Map.Entry<String, DocValue> entry : value.getAttributes()) {
                arg.addLast(new AttributeReference.ObjectKey(entry.getKey()));

                entry.getValue().
                        accept(this, arg);

                arg.removeLast();
            }

            ContainsAttributesQueryCriteria caqc
                    = new ContainsAttributesQueryCriteria(
                            basicAttRef.append(arg),
                            value.keySet(),
                            true);

            conjunctionBuilder.add(caqc);

            return null;
        }

        private boolean containsObjects(ArrayValue value) {
            for (DocValue subValue : value) {
                if (subValue.getType().equals(ObjectType.INSTANCE)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Void visit(
                ArrayValue value,
                LinkedList<AttributeReference.Key> arg) {
            if (!containsObjects(value)) {
                Value<?> arrValue = value.accept(converter, null);
                conjunctionBuilder.add(
                        new IsEqualQueryCriteria(
                                basicAttRef.append(arg),
                                arrValue
                        )
                );
            }

            for (int i = 0; i < value.size(); i++) {
                arg.addLast(new AttributeReference.ArrayKey(i));

                DocValue docValue = value.get(i);

                if (docValue.getType().equals(ObjectType.INSTANCE)) {
                    docValue.accept(this, arg);
                }
                else if (docValue.getType() instanceof ArrayType) {
                    docValue.accept(this, arg);
                }

                arg.removeLast();
            }

            return null;
        }

        private Void defaultcase(
                DocValue value,
                LinkedList<AttributeReference.Key> arg) {

            AttributeReference attRef = basicAttRef.append(arg);
            Value<?> converted = value.accept(converter, null);

            conjunctionBuilder.add(
                    new IsEqualQueryCriteria(
                            attRef,
                            converted
                    )
            );

            return null;
        }

        @Override
        public Void visit(
                BooleanValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                NullValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                IntegerValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                LongValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                DoubleValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                StringValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                TwelveBytesValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                DateTimeValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                DateValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                TimeValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

        @Override
        public Void visit(
                PatternValue value,
                LinkedList<AttributeReference.Key> arg) {
            return defaultcase(value, arg);
        }

    }

}
