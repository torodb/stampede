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

import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;
import com.torodb.torod.core.subdocument.values.StringValue;
import com.torodb.torod.core.subdocument.values.*;
import java.util.Map;
import javax.inject.Inject;

/**
 *
 */
public class DefaultD2RTranslator implements D2RTranslator {

    private final DbMetaInformationCache cache;
    private final DocumentSplitter splitter;
    private final DocumentBuilderFactory documentBuilderFactory;

    private static final SubDocValueToDocValueTranslator valueTranslator = new SubDocValueToDocValueTranslator();

    @Inject
    public DefaultD2RTranslator(
            DbMetaInformationCache cache,
            DocumentBuilderFactory documentBuilderFactory) {
        this.cache = cache;
        this.splitter = new DocumentSplitter(cache);
        this.documentBuilderFactory = documentBuilderFactory;
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
    public SplitDocument translate(SessionExecutor sessionExecutor, String collection, ToroDocument document) {
        cache.createCollection(sessionExecutor, collection, null);

        final SplitDocument splitDocument = splitter.split(sessionExecutor, collection, document);

        return splitDocument;
    }

    @Override
    public ToroDocument translate(SplitDocument splitDocument) {
        final ToroDocument.DocumentBuilder docBuilder = documentBuilderFactory.newDocBuilder();

        SubDocValueToDocValueTranslator.Argument arg = new SubDocValueToDocValueTranslator.Argument(splitDocument, splitDocument.getRoot());
        docBuilder.setRoot(translateDocStructure(arg));

        return docBuilder.build();
    }

    private static ObjectValue translateDocStructure(SubDocValueToDocValueTranslator.Argument arg) {
        DocStructure structure = (DocStructure) arg.structure;
        SplitDocument splitDocument = arg.splitDoc;

        ObjectValue.Builder objBuilder = new ObjectValue.Builder();
        SubDocument subDoc = splitDocument.getSubDocuments().get(structure.getType(), structure.getIndex());

        for (String keyName : subDoc.getAttributes().keySet()) {
            //childStructure will be null if the child is a scalar
            StructureElement childStructure = structure.getElements().get(keyName);
            arg.structure = childStructure;

            objBuilder.putValue(keyName, subDoc.getValue(keyName).accept(valueTranslator, arg));
        }
        for (Map.Entry<String, StructureElement> entry : structure.getElements().entrySet()) {
            if (entry.getValue() instanceof DocStructure) { //arrays has already been mapped as values
                StructureElement childStructure = structure.getElements().get(entry.getKey());
                arg.structure = childStructure;

                objBuilder.putValue(
                        entry.getKey(),
                        translateDocStructure(arg)
                );
            }
        }
        return objBuilder.build();
    }

    private static class SubDocValueToDocValueTranslator implements ValueVisitor<DocValue, SubDocValueToDocValueTranslator.Argument> {

        @Override
        public DocValue visit(BooleanValue value, Argument arg) {
            if (value.getValue()) {
                return com.torodb.kvdocument.values.BooleanValue.TRUE;
            }
            return com.torodb.kvdocument.values.BooleanValue.FALSE;
        }

        @Override
        public DocValue visit(NullValue value, Argument arg) {
            return com.torodb.kvdocument.values.NullValue.INSTANCE;
        }

        @Override
        public DocValue visit(ArrayValue value, Argument arg) {
            com.torodb.kvdocument.values.ArrayValue.Builder builder = new com.torodb.kvdocument.values.ArrayValue.Builder();

            ArrayStructure structure = (ArrayStructure) arg.structure;

            for (Value e : value.getValue()) {
                if (e instanceof ArrayValue) { //arrays will be mapped later
                    builder.add(com.torodb.kvdocument.values.NullValue.INSTANCE);
                } else {
                    builder.add((DocValue) e.accept(this, arg));
                }
            }

            for (Map.Entry<Integer, StructureElement> entry : structure.getElements().entrySet()) {
                StructureElement element = entry.getValue();
                arg.structure = element;

                int index = entry.getKey(); //array structure indexes are 0 based

                if (element instanceof DocStructure) {
                    assert value.get(index) instanceof NullValue;

                    ObjectValue translated = translateDocStructure(arg);
                    builder.setValue(index, translated);
                } else if (element instanceof ArrayStructure) {
                    assert value.get(index) instanceof ArrayValue;

                    ArrayValue childValue = (ArrayValue) value.get(index);

                    builder.setValue(index, visit(childValue, arg));
                } else {
                    throw new AssertionError(this.getClass() + " doesn't recognizes " + element + " as a structure");
                }
            }

            builder.setElementType(GenericType.INSTANCE);

            com.torodb.kvdocument.values.ArrayValue result = builder.build();

            assert result.size() == value.size();

            return result;
        }

        @Override
        public DocValue visit(IntegerValue value, Argument arg) {
            return new com.torodb.kvdocument.values.IntegerValue(value.getValue());
        }

        @Override
        public DocValue visit(LongValue value, Argument arg) {
            return new com.torodb.kvdocument.values.LongValue(value.getValue());
        }

        @Override
        public DocValue visit(DoubleValue value, Argument arg) {
            return new com.torodb.kvdocument.values.DoubleValue(value.getValue());
        }

        @Override
        public DocValue visit(StringValue value, Argument arg) {
            return new com.torodb.kvdocument.values.StringValue(value.getValue());
        }

        @Override
        public DocValue visit(TwelveBytesValue value, Argument arg) {
            return new com.torodb.kvdocument.values.TwelveBytesValue(value.getValue());
        }

        @Override
        public DocValue visit(DateTimeValue value, Argument arg) {
            return new com.torodb.kvdocument.values.DateTimeValue(value.getValue());
        }

        @Override
        public DocValue visit(DateValue value, Argument arg) {
            return new com.torodb.kvdocument.values.DateValue(value.getValue());
        }

        @Override
        public DocValue visit(TimeValue value, Argument arg) {
            return new com.torodb.kvdocument.values.TimeValue(value.getValue());
        }

        @Override
        public DocValue visit(PatternValue value, Argument arg) {
            return new com.torodb.kvdocument.values.PatternValue(value.getValue());
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
