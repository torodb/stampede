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

import com.google.common.collect.Lists;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.KVValueAdaptor;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.AttributeReference.Key;
import com.torodb.torod.core.language.querycriteria.ContainsAttributesQueryCriteria;
import com.torodb.torod.core.language.querycriteria.IsEqualQueryCriteria;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.core.utils.KVValueToScalarValue;
import java.util.LinkedList;

/**
 *
 */
public class EqualFactory {

    private EqualFactory() {
    }

    public static QueryCriteria createEquality(AttributeReference attRef, KVValue<?> docValue) {
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

    private static class EqualityQueryFinder extends KVValueAdaptor<Void, LinkedList<AttributeReference.Key>> {

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
        public Void visit(KVDocument value, LinkedList<AttributeReference.Key> arg) {
            for (DocEntry<?> entry : value) {
                arg.addLast(new AttributeReference.ObjectKey(entry.getKey()));

                entry.getValue().accept(this, arg);

                arg.removeLast();
            }

            ContainsAttributesQueryCriteria caqc = new ContainsAttributesQueryCriteria(
                            basicAttRef.append(arg),
                            value.getKeys(),
                            true);

            conjunctionBuilder.add(caqc);

            return null;
        }

        private boolean containsObjects(KVArray value) {
            for (KVValue<?> subValue : value) {
                if (subValue.getType().equals(DocumentType.INSTANCE)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Void visit(KVArray value, LinkedList<AttributeReference.Key> arg) {
            if (!containsObjects(value)) {
                ScalarValue<?> arrValue = KVValueToScalarValue.fromDocValue(value);
                conjunctionBuilder.add(
                        new IsEqualQueryCriteria(
                                basicAttRef.append(arg),
                                arrValue
                        )
                );
            }

            for (int i = 0; i < value.size(); i++) {
                arg.addLast(new AttributeReference.ArrayKey(i));

                KVValue<?> docValue = value.get(i);

                if (docValue.getType().equals(DocumentType.INSTANCE)) {
                    docValue.accept(this, arg);
                }
                else if (docValue.getType() instanceof ArrayType) {
                    docValue.accept(this, arg);
                }

                arg.removeLast();
            }

            return null;
        }

        @Override
        public Void defaultCase(KVValue<?> value, LinkedList<Key> arg) {
            assert !(value instanceof KVDocument);

            AttributeReference attRef = basicAttRef.append(arg);
            ScalarValue<?> converted = KVValueToScalarValue.fromDocValue(value);

            conjunctionBuilder.add(
                    new IsEqualQueryCriteria(
                            attRef,
                            converted
                    )
            );

            return null;
        }

    }

}
