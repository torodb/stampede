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

package com.torodb.torod.d2r;

import com.torodb.kvdocument.values.LongValue;
import com.torodb.kvdocument.values.StringValue;
import com.torodb.kvdocument.values.TimeValue;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.values.IntegerValue;
import com.torodb.kvdocument.values.NullValue;
import com.torodb.kvdocument.values.BooleanValue;
import com.torodb.kvdocument.values.DoubleValue;
import com.torodb.kvdocument.values.ArrayValue;
import com.torodb.kvdocument.values.DocValueVisitor;
import com.torodb.kvdocument.values.ValueDFW;
import com.torodb.kvdocument.values.DateValue;
import com.torodb.kvdocument.values.DateTimeValue;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.kvdocument.values.TwelveBytesValue;
import com.google.common.collect.Maps;
import com.torodb.kvdocument.values.*;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.values.Value;
import java.util.Map;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import java.util.HashSet;
import javax.annotation.concurrent.NotThreadSafe;

/**
 *
 */
public class DocumentSplitter {

    private final DbMetaInformationCache cache;
    private final TypesCollector typesCollector = new TypesCollector();
    private final static TypeTranslator typeTranslator = new TypeTranslator();

    public DocumentSplitter(DbMetaInformationCache cache) {
        this.cache = cache;
    }

    /**
     *
     * @param sessionExecutor
     * @param collection
     * @param doc
     * @return
     */
    public SplitDocument split(SessionExecutor sessionExecutor,
                               String collection,
                               ToroDocument doc) {
        int docId = cache.reserveDocIds(sessionExecutor, collection, 1);

        Map<ObjectValue, SubDocType> collectedTypes = typesCollector
                .collectTypes(doc.getRoot());
        prepareValueTypesTables(sessionExecutor, collection, collectedTypes);

        return translate(doc, docId, collectedTypes);
    }

    private void prepareValueTypesTables(
            SessionExecutor sessionExecutor,
            String collection,
            Map<ObjectValue, SubDocType> collectedTypes) {

        for (SubDocType subDocType : collectedTypes.values()) {
            cache.createSubDocTypeTable(sessionExecutor, collection, subDocType);
        }
    }

    private SubDocType getSubDocType(ObjectValue value) {
        SubDocType.Builder builder = new SubDocType.Builder();

        for (Map.Entry<String, DocValue> entry : value.getAttributes()) {
            if (!(entry.getValue() instanceof ObjectValue)) {
                BasicType type = entry.getValue().accept(typeTranslator, null);

                SubDocAttribute att = new SubDocAttribute(entry.getKey(), type);
                builder.add(att);
            }
        }

        return builder.build();
    }

    private SplitDocument translate(
            ToroDocument doc,
            int docId,
            Map<ObjectValue, SubDocType> collectedTypes) {

        SplitDocument.Builder splitDocBuilder = new SplitDocument.Builder();

        ValueTranslator translator = new ValueTranslator(docId, splitDocBuilder,
                                                         collectedTypes);

        RootTranslatorConsumer consumer = new RootTranslatorConsumer();

        doc.getRoot().accept(translator, consumer);

        splitDocBuilder.setId(docId);

        splitDocBuilder.setRoot(consumer.getRoot());

        return splitDocBuilder.build();
    }

    private static class TypeTranslator implements
            com.torodb.kvdocument.values.DocValueVisitor<BasicType, Void> {

        @Override
        public BasicType visit(
                BooleanValue value,
                Void arg) {
            return BasicType.BOOLEAN;
        }

        @Override
        public BasicType visit(
                NullValue value,
                Void arg) {
            return BasicType.NULL;
        }

        @Override
        public BasicType visit(
                ArrayValue value,
                Void arg) {
            return BasicType.ARRAY;
        }

        @Override
        public BasicType visit(
                IntegerValue value,
                Void arg) {
            return BasicType.INTEGER;
        }

        @Override
        public BasicType visit(
                LongValue value,
                Void arg) {
            return BasicType.LONG;
        }

        @Override
        public BasicType visit(
                DoubleValue value,
                Void arg) {
            return BasicType.DOUBLE;
        }

        @Override
        public BasicType visit(
                StringValue value,
                Void arg) {
            return BasicType.STRING;
        }

        @Override
        public BasicType visit(
                ObjectValue value,
                Void arg) {
            return BasicType.NULL;
        }

        @Override
        public BasicType visit(
                TwelveBytesValue value,
                Void arg) {
            return BasicType.TWELVE_BYTES;
        }

        @Override
        public BasicType visit(
                DateTimeValue value,
                Void arg) {
            return BasicType.DATETIME;
        }

        @Override
        public BasicType visit(
                DateValue value,
                Void arg) {
            return BasicType.DATE;
        }

        @Override
        public BasicType visit(
                TimeValue value,
                Void arg) {
            return BasicType.TIME;
        }

        @Override
        public BasicType visit(PatternValue value, Void arg) {
            return BasicType.PATTERN;
        }

    }

    private class TypesCollector extends ValueDFW<Map<ObjectValue, SubDocType>> {

        public Map<ObjectValue, SubDocType> collectTypes(ObjectValue value) {
            Map<ObjectValue, SubDocType> calculatedTypes = Maps.newHashMap();

            value.accept(this, calculatedTypes);

            return calculatedTypes;
        }

        @Override
        protected void preObjectValue(ObjectValue value,
                                      Map<ObjectValue, SubDocType> types) {
            SubDocType subDocType = getSubDocType(value);

            types.put(value, subDocType);
        }
    }

    @NotThreadSafe
    private static class ValueTranslator implements
            DocValueVisitor<Void, TranslatorConsumer> {

        private final int docId;
        private final SplitDocument.Builder splitDocBuilder;
        private final Map<ObjectValue, SubDocType> collectedTypes;
        private final Map<SubDocType, Integer> indixes;

        public ValueTranslator(
                int docId,
                SplitDocument.Builder splitDocBuilder,
                Map<ObjectValue, SubDocType> collectedTypes) {

            this.docId = docId;
            this.splitDocBuilder = splitDocBuilder;
            this.collectedTypes = collectedTypes;
            this.indixes = Maps.newHashMapWithExpectedSize(new HashSet(
                    collectedTypes.values()).size());
        }

        public int consumeIndex(SubDocType type) {
            Integer index = indixes.get(type);
            if (index == null) {
                index = 0;
            }
            indixes.put(type, index + 1);
            return index;
        }

        @Override
        public Void visit(
                ObjectValue value,
                TranslatorConsumer arg) {
            DocStructure.Builder structureBuilder = new DocStructure.Builder();
            SubDocument.Builder subDocBuilder = new SubDocument.Builder();

            SubDocType type = collectedTypes.get(value);

            int index = consumeIndex(type);

            structureBuilder.setIndex(index);
            structureBuilder.setType(type);

            subDocBuilder.setIndex(index);
            subDocBuilder.setDocumentId(docId);

            ObjectTranslatorConsumer consumer = new ObjectTranslatorConsumer(
                    subDocBuilder, structureBuilder);

            for (Map.Entry<String, DocValue> entry : value.getAttributes()) {
                consumer.setAttributeName(entry.getKey());
                entry.getValue().accept(this, consumer);
            }

            splitDocBuilder.add(subDocBuilder.build());
            arg.consume(structureBuilder.built());

            return null;
        }

        @Override
        public Void visit(
                ArrayValue value,
                TranslatorConsumer arg) {
            com.torodb.torod.core.subdocument.values.ArrayValue.Builder valueBuilder
                    = new com.torodb.torod.core.subdocument.values.ArrayValue.Builder();
            com.torodb.torod.core.subdocument.structure.ArrayStructure.Builder structureBuilder
                    = new com.torodb.torod.core.subdocument.structure.ArrayStructure.Builder();

            ArrayTranslatorConsumer consumer = new ArrayTranslatorConsumer(
                    valueBuilder, structureBuilder);

            int i = 0;
            for (DocValue child : value) {
                consumer.setIndex(i);
                child.accept(this, consumer);

                i++; //i is 0-based!
            }

            arg.consume(valueBuilder.build());
            arg.consume(structureBuilder.built());

            return null;
        }

        @Override
        public Void visit(
                BooleanValue value,
                TranslatorConsumer arg) {
            arg.consume(com.torodb.torod.core.subdocument.values.BooleanValue
                    .from(value.getValue()));
            return null;
        }

        @Override
        public Void visit(
                NullValue value,
                TranslatorConsumer arg) {
            arg.consume(
                    com.torodb.torod.core.subdocument.values.NullValue.INSTANCE
            );
            return null;
        }

        @Override
        public Void visit(
                IntegerValue value,
                TranslatorConsumer arg) {
            arg.consume(
                    new com.torodb.torod.core.subdocument.values.IntegerValue(
                            value.getValue()
                    )
            );
            return null;
        }

        @Override
        public Void visit(
                LongValue value,
                TranslatorConsumer arg) {
            arg.consume(new com.torodb.torod.core.subdocument.values.LongValue(
                    value.getValue()
            )
            );
            return null;
        }

        @Override
        public Void visit(
                DoubleValue value,
                TranslatorConsumer arg) {
            arg.consume(
                    new com.torodb.torod.core.subdocument.values.DoubleValue(
                            value.getValue()
                    )
            );
            return null;
        }

        @Override
        public Void visit(
                StringValue value,
                TranslatorConsumer arg) {
            arg.consume(
                    new com.torodb.torod.core.subdocument.values.StringValue(
                            value.getValue()
                    )
            );
            return null;
        }

        @Override
        public Void visit(
                TwelveBytesValue value,
                TranslatorConsumer arg) {
            arg.consume(
                    new com.torodb.torod.core.subdocument.values.TwelveBytesValue(
                            value.getValue()
                    )
            );
            return null;
        }

        @Override
        public Void visit(
                DateTimeValue value,
                TranslatorConsumer arg) {
            arg.consume(
                    new com.torodb.torod.core.subdocument.values.DateTimeValue(
                            value.getValue()
                    )
            );
            return null;
        }

        @Override
        public Void visit(
                DateValue value,
                TranslatorConsumer arg) {
            arg.consume(
                    new com.torodb.torod.core.subdocument.values.DateValue(
                            value.getValue()
                    )
            );
            return null;
        }

        @Override
        public Void visit(
                TimeValue value,
                TranslatorConsumer arg) {

            arg.consume(
                    new com.torodb.torod.core.subdocument.values.TimeValue(
                            value.getValue()
                    )
            );
            return null;
        }

        @Override
        public Void visit(PatternValue value, TranslatorConsumer arg) {
            arg.consume(
                    new com.torodb.torod.core.subdocument.values.PatternValue(
                            value.getValue()
                    )
            );
            return null;
        }

    }

    public static interface TranslatorConsumer {

        void consume(Value value);

        void consume(DocStructure structure);

        void consume(ArrayStructure arrayStructure);
    }

    private static class ArrayTranslatorConsumer implements TranslatorConsumer {

        private final com.torodb.torod.core.subdocument.values.ArrayValue.Builder valueBuilder;
        private final com.torodb.torod.core.subdocument.structure.ArrayStructure.Builder structureBuilder;

        private int index;

        public ArrayTranslatorConsumer(
                com.torodb.torod.core.subdocument.values.ArrayValue.Builder valueBuilder,
                com.torodb.torod.core.subdocument.structure.ArrayStructure.Builder structureBuilder) {
            this.valueBuilder = valueBuilder;
            this.structureBuilder = structureBuilder;
            this.index = 0;
        }

        @Override
        public void consume(Value value) {
            valueBuilder.add(value);
        }

        @Override
        public void consume(DocStructure structure) {
            valueBuilder.add(
                    com.torodb.torod.core.subdocument.values.NullValue.INSTANCE);
            structureBuilder.add(index, structure);
        }

        @Override
        public void consume(ArrayStructure arrayStructure) {
            structureBuilder.add(index, arrayStructure);
        }

        public void setIndex(int index) {
            this.index = index;
        }

    }

    private static class ObjectTranslatorConsumer implements TranslatorConsumer {

        private final SubDocument.Builder subDocBuilder;
        private final DocStructure.Builder structureBuilder;
        private String attributeName;

        private ObjectTranslatorConsumer(SubDocument.Builder subDocBuilder,
                                         DocStructure.Builder structureBuilder) {
            this.subDocBuilder = subDocBuilder;
            this.structureBuilder = structureBuilder;
        }

        public void setAttributeName(String attributeName) {
            this.attributeName = attributeName;
        }

        @Override
        public void consume(Value value) {
            subDocBuilder.add(attributeName, value);
        }

        @Override
        public void consume(DocStructure structure) {
            structureBuilder.add(attributeName, structure);
        }

        @Override
        public void consume(ArrayStructure arrayStructure) {
            structureBuilder.add(attributeName, arrayStructure);
        }
    }

    private static class RootTranslatorConsumer implements TranslatorConsumer {

        private DocStructure root = null;

        @Override
        public void consume(Value value) {
            throw new UnsupportedOperationException(
                    "A doc structure was expected.");
        }

        @Override
        public void consume(ArrayStructure arrayStructure) {
            throw new UnsupportedOperationException(
                    "A doc structure was expected.");
        }

        @Override
        public void consume(DocStructure structure) {
            if (root == null) {
                root = structure;
            }
            else {
                throw new AssertionError(
                        "More than one root has been detected, but only one was expected");
            }
        }

        public DocStructure getRoot() {
            return root;
        }
    }
}
