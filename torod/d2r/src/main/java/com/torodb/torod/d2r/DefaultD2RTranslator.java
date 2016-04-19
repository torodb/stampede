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

import com.google.common.base.Function;
import com.torodb.kvdocument.values.*;
import com.torodb.kvdocument.values.heap.*;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.core.subdocument.values.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 *
 */
public class DefaultD2RTranslator implements D2RTranslator {

    private final DbMetaInformationCache cache;
    private final DocumentSplitter splitter;
    
    private final Function<SplitDocument, ToroDocument> toDocFunction;

    @Inject
    public DefaultD2RTranslator(
            DbMetaInformationCache cache,
            DocumentBuilderFactory documentBuilderFactory,
            Provider<SubDocType.Builder> subDocTypeBuilderProvider) {
        this.cache = cache;
        this.splitter = new DocumentSplitter(cache, subDocTypeBuilderProvider);
        this.toDocFunction = new ToDocFunction(documentBuilderFactory);
    }

    @Override
    public void initialize() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void shutdownNow() {
    }

    @Override
    public Function<SplitDocument, ToroDocument> getToDocumentFunction() {
        return toDocFunction;
    }

    @Override
    public Function<ToroDocument, SplitDocument> getToRelationalFunction(
            final SessionExecutor sessionExecutor,
            final String collection) {
        return new Function<ToroDocument, SplitDocument>() {
            @Override
            public SplitDocument apply(@Nonnull ToroDocument input) {
                cache.createCollection(sessionExecutor, collection, null);
                final SplitDocument splitDocument = splitter.split(
                        sessionExecutor,
                        collection,
                        input
                );

                return splitDocument;
            }
        };
    }

    private static class ToDocFunction implements Function<SplitDocument, ToroDocument> {

        private static final SubDocValueToDocValueTranslator VALUE_TRANSLATOR = new SubDocValueToDocValueTranslator();
        private final DocumentBuilderFactory documentBuilderFactory;

        private ToDocFunction(DocumentBuilderFactory documentBuilderFactory) {
            this.documentBuilderFactory = documentBuilderFactory;
        }

        @Override
        public ToroDocument apply(@Nonnull SplitDocument input) {
            final ToroDocument.DocumentBuilder docBuilder = documentBuilderFactory.newDocBuilder();

            SubDocValueToDocValueTranslator.Argument arg
                    = new SubDocValueToDocValueTranslator.Argument(input, input.getRoot());
            docBuilder.setRoot(translateDocStructure(arg));

            return docBuilder.build();
        }

        private static KVDocument translateDocStructure(SubDocValueToDocValueTranslator.Argument arg) {
            DocStructure structure = (DocStructure) arg.structure;
            SplitDocument splitDocument = arg.splitDoc;

            LinkedHashMap<String, KVValue<?>> map = new LinkedHashMap<>();
            SubDocument subDoc = splitDocument.getSubDocuments().get(
                    structure.getType(), structure.getIndex());

            for (String keyName : subDoc.getType().getAttributeKeys()) {
                //childStructure will be null if the child is a scalar
                StructureElement childStructure
                        = structure.getElements().get(keyName);
                arg.structure = childStructure;

                map.put(
                        keyName,
                        subDoc.getValue(keyName).accept(VALUE_TRANSLATOR, arg)
                );
            }
            for (Map.Entry<String, StructureElement> entry : structure.getElements().entrySet()) {
                if (entry.getValue() instanceof DocStructure) { //arrays has already been mapped as values
                    StructureElement childStructure
                            = structure.getElements().get(entry.getKey());
                    arg.structure = childStructure;

                    map.put(
                            entry.getKey(),
                            translateDocStructure(arg)
                    );
                }
            }
            return new MapKVDocument(map);
        }

        private static class SubDocValueToDocValueTranslator implements
                ScalarValueVisitor<KVValue<?>, SubDocValueToDocValueTranslator.Argument> {

            @Override
            public KVValue<?> visit(ScalarBoolean value, Argument arg) {
                if (value.getValue()) {
                    return KVBoolean.TRUE;
                }
                return KVBoolean.FALSE;
            }

            @Override
            public KVValue<?> visit(ScalarNull value, Argument arg) {
                return KVNull.getInstance();
            }

            @Override
            public KVValue<?> visit(ScalarArray value, Argument arg) {
                List<KVValue<?>> list = new ArrayList<>(value.size());

                ArrayStructure structure = (ArrayStructure) arg.structure;

                for (ScalarValue<?> e : value.getValue()) {
                    if (e instanceof ScalarArray) { //arrays will be mapped later
                        list.add(KVNull.getInstance());
                    } else {
                        list.add((KVValue<?>) e.accept(this, arg));
                    }
                }

                for (Map.Entry<Integer, StructureElement> entry : structure.getElements().entrySet()) {
                    StructureElement element = entry.getValue();
                    arg.structure = element;

                    int index = entry.getKey(); //array structure indexes are 0 based

                    if (element instanceof DocStructure) {
                        assert value.get(index) instanceof ScalarNull;

                        KVDocument translated = translateDocStructure(arg);
                        list.set(index, translated);
                    } else if (element instanceof ArrayStructure) {
                        assert value.get(index) instanceof ScalarArray;

                        ScalarArray childValue = (ScalarArray) value.get(index);

                        list.set(index, visit(childValue, arg));
                    } else {
                        throw new AssertionError(this.getClass()
                                + " doesn't recognizes " + element
                                + " as a structure");
                    }
                }

                KVArray result = new ListKVArray(list);

                assert result.size() == value.size();

                return result;
            }

            @Override
            public KVValue<?> visit(ScalarInteger value, Argument arg) {
                return KVInteger.of(value.getValue());
            }

            @Override
            public KVValue<?> visit(ScalarLong value, Argument arg) {
                return KVLong.of(value.getValue());
            }

            @Override
            public KVValue<?> visit(ScalarDouble value, Argument arg) {
                return KVDouble.of(value.getValue());
            }

            @Override
            public KVValue<?> visit(ScalarString value, Argument arg) {
                return new StringKVString(value.getValue());
            }

            @Override
            public KVValue<?> visit(ScalarMongoObjectId value, Argument arg) {
                return new ByteArrayKVMongoObjectId(value.getArrayValue());
            }

            @Override
            public KVValue<?> visit(ScalarMongoTimestamp value, Argument arg) {
                return new DefaultKVMongoTimestamp(value.getSecondsSinceEpoch(), value.getOrdinal());
            }

            @Override
            public KVValue<?> visit(ScalarInstant value, Argument arg) {
                return new LongKVInstant(value.getMillisFromUnix());
            }

            @Override
            public KVValue<?> visit(ScalarDate value, Argument arg) {
                return new LocalDateKVDate(value.getValue());
            }

            @Override
            public KVValue<?> visit(ScalarTime value, Argument arg) {
                return new LocalTimeKVTime(value.getValue());
            }

            @Override
            public KVValue<?> visit(ScalarBinary value, Argument arg) {
                return new ByteSourceKVBinary(value.getSubtype(), value.getCategory(), value.getByteSource());
            }

            public static class Argument {

                private final SplitDocument splitDoc;
                private StructureElement structure;

                public Argument(SplitDocument splitDoc, StructureElement structure) {
                    this.splitDoc = splitDoc;
                    this.structure = structure;
                }
            }
        }

    }

    
}
