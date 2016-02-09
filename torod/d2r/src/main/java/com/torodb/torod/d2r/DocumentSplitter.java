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

import com.google.common.collect.Maps;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.*;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.subdocument.SubDocType.Builder;
import com.torodb.torod.core.subdocument.*;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.values.ScalarNull;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.core.subdocument.values.heap.ListScalarArray;
import com.torodb.torod.core.utils.KVValueToScalarValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Provider;


/**
 *
 */
public class DocumentSplitter {

    private final DbMetaInformationCache cache;
    private final TypesCollector typesCollector = new TypesCollector();
    private final Provider<SubDocType.Builder> subDocTypeBuilderProvider;

    @Inject
    public DocumentSplitter(DbMetaInformationCache cache, Provider<Builder> subDocTypeBuilderProvider) {
        this.cache = cache;
        this.subDocTypeBuilderProvider = subDocTypeBuilderProvider;
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

        Map<KVDocument, SubDocType> collectedTypes = typesCollector
                .collectTypes(doc.getRoot());
        prepareValueTypesTables(sessionExecutor, collection, collectedTypes);

        return translate(doc, docId, collectedTypes);
    }

    private void prepareValueTypesTables(
            SessionExecutor sessionExecutor,
            String collection,
            Map<KVDocument, SubDocType> collectedTypes) {

        for (SubDocType subDocType : collectedTypes.values()) {
            cache.createSubDocTypeTable(sessionExecutor, collection, subDocType);
        }
    }

    private SubDocType getSubDocType(KVDocument value) {
        SubDocType.Builder builder = subDocTypeBuilderProvider.get();

        for (DocEntry<?> entry : value) {
            if (!(entry.getValue() instanceof KVDocument)) {
                ScalarType type = ScalarType.fromDocType(entry.getValue().getType());

                SubDocAttribute att = new SubDocAttribute(entry.getKey(), type);
                builder.add(att);
            }
        }

        return builder.build();
    }

    private SplitDocument translate(
            ToroDocument doc,
            int docId,
            Map<KVDocument, SubDocType> collectedTypes) {

        SplitDocument.Builder splitDocBuilder = new SplitDocument.Builder();

        ValueTranslator translator = new ValueTranslator(docId, splitDocBuilder, collectedTypes);

        RootTranslatorConsumer consumer = new RootTranslatorConsumer();

        doc.getRoot().accept(translator, consumer);

        splitDocBuilder.setId(docId);

        splitDocBuilder.setRoot(consumer.getRoot());

        return splitDocBuilder.build();
    }

    private class TypesCollector extends KVValueDFW<Map<KVDocument, SubDocType>> {

        public Map<KVDocument, SubDocType> collectTypes(KVDocument value) {
            Map<KVDocument, SubDocType> calculatedTypes = Maps.newHashMap();

            value.accept(this, calculatedTypes);

            return calculatedTypes;
        }

        @Override
        protected void preDoc(KVDocument value, Map<KVDocument, SubDocType> types) {
            SubDocType subDocType = getSubDocType(value);

            types.put(value, subDocType);
        }
    }

    @NotThreadSafe
    private static class ValueTranslator implements KVValueVisitor<Void, TranslatorConsumer> {

        private final int docId;
        private final SplitDocument.Builder splitDocBuilder;
        private final Map<KVDocument, SubDocType> collectedTypes;
        private final Map<SubDocType, Integer> indixes;

        public ValueTranslator(
                int docId,
                SplitDocument.Builder splitDocBuilder,
                Map<KVDocument, SubDocType> collectedTypes) {

            this.docId = docId;
            this.splitDocBuilder = splitDocBuilder;
            this.collectedTypes = collectedTypes;
            this.indixes = Maps.newHashMapWithExpectedSize(collectedTypes.size());
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
        public Void visit(KVDocument value, TranslatorConsumer arg) {
            SubDocType type = collectedTypes.get(value);

            DocStructure.Builder structureBuilder = new DocStructure.Builder();
            SubDocument.Builder subDocBuilder = SubDocument.Builder.withKnownType(type);

            int index = consumeIndex(type);

            structureBuilder.setIndex(index);
            structureBuilder.setType(type);

            subDocBuilder.setIndex(index);
            subDocBuilder.setDocumentId(docId);

            ObjectTranslatorConsumer consumer = new ObjectTranslatorConsumer(
                    subDocBuilder, structureBuilder);

            for (DocEntry<?> entry : value) {
                consumer.setAttributeName(entry.getKey());
                entry.getValue().accept(this, consumer);
            }

            splitDocBuilder.add(subDocBuilder.build());
            arg.consume(structureBuilder.built());

            return null;
        }

        @Override
        public Void visit(KVArray value, TranslatorConsumer arg) {
            List<ScalarValue<?>> valueBuilder = new ArrayList<>(value.size());
            ArrayStructure.Builder structureBuilder = new ArrayStructure.Builder();

            ArrayTranslatorConsumer consumer = new ArrayTranslatorConsumer(
                    valueBuilder, structureBuilder);

            int i = 0;
            for (KVValue<?> child : value) {
                consumer.setIndex(i);
                child.accept(this, consumer);

                i++; //i is 0-based!
            }

            arg.consume(new ListScalarArray(valueBuilder));
            arg.consume(structureBuilder.built());

            return null;
        }

        @Override
        public Void visit(KVBoolean value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVNull value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVInteger value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVLong value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVDouble value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVString value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVMongoObjectId value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVMongoTimestamp value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVInstant value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVDate value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVTime value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

        @Override
        public Void visit(KVBinary value, TranslatorConsumer arg) {
            arg.consume(KVValueToScalarValue.AS_VISITOR.visit(value, null));
            return null;
        }

    }

    public static interface TranslatorConsumer {

        void consume(ScalarValue value);

        void consume(DocStructure structure);

        void consume(ArrayStructure arrayStructure);
    }

    private static class ArrayTranslatorConsumer implements TranslatorConsumer {

        private final List<ScalarValue<?>> valueListBuilder;
        private final ArrayStructure.Builder structureBuilder;

        private int index;

        public ArrayTranslatorConsumer(
                List<ScalarValue<?>> valueListBuilder,
                ArrayStructure.Builder structureBuilder) {
            this.valueListBuilder = valueListBuilder;
            this.structureBuilder = structureBuilder;
            this.index = 0;
        }

        @Override
        public void consume(ScalarValue value) {
            valueListBuilder.add(value);
        }

        @Override
        public void consume(DocStructure structure) {
            valueListBuilder.add(ScalarNull.getInstance());
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
        public void consume(ScalarValue value) {
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
        public void consume(ScalarValue value) {
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
