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

package com.torodb.torod.db.backends.query;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.org.bson.utils.MongoBsonTranslator;
import com.google.common.collect.*;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.language.querycriteria.utils.ConjunctionBuilder;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.SimpleSubDocTypeBuilderProvider;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.values.ScalarInteger;
import com.torodb.torod.db.backends.meta.StructuresCache;
import com.torodb.torod.db.backends.query.processors.ProcessorTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.inject.Provider;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonReader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsExceptionClass;

/**
 *
 */
public class QueryStructureFilterTest {

    private static final BsonDocumentCodec BSON_CODEC = new BsonDocumentCodec();

    private static Map<Integer, DocStructure> structures;
    private static StructuresCache cache;

    private static final AttributeReference f1Ref
            = new AttributeReference.Builder()
            .addObjectKey("f1")
            .build();
    private static final AttributeReference f1f2Ref
            = new AttributeReference.Builder()
            .addObjectKey("f1")
            .addObjectKey("f2")
            .build();
    private static final AttributeReference f2Ref
            = new AttributeReference.Builder()
            .addObjectKey("f2")
            .build();

    @BeforeClass
    public static void setUpClass() {
        Map<Integer, String> textMap = Maps.newHashMap();

        textMap.put(0, "{\"f1\": null}");
        textMap.put(1, "{\"f1\": 1}");
        textMap.put(2, "{\"f1\": [{\"f2\": 1}]}");
        textMap.put(3, "{\"f1\": {\"f2\": 1}}");
        textMap.put(4, "{\"f1\": [{\"f2\": [{\"f3\": 1}]}]}");
        textMap.put(5, "{\"f1\": [{\"f2\": {\"f3\": 1}}]}");
        textMap.put(6, "{\"f1\": {\"f2\": [{\"f3\": 1}]}}");
        textMap.put(7, "{\"f1\": {\"f2\": {\"f3\": 1}}}");

        textMap.put(8, "{\"f1\": [1]}");
        textMap.put(9, "{\"f1\": [[1]]}");
        textMap.put(10, "{\"f1\": [[{\"f2\": 1}]]}");
        textMap.put(11, "{\"f1\": {\"f2\": [1]}}");
        textMap.put(12, "{\"f1\": [{\"f2\": [1]}]}");
        textMap.put(13, "{\"f1\": [1, 2, 3, [\"a\", \"b\", \"c\"]]}");

        structures = createStructuresFromText(textMap, new SimpleSubDocTypeBuilderProvider());

        cache = createCache(structures);
    }

    @Test
    public void testFalse() {
        assert QueryStructureFilter.filterStructures(cache, FalseQueryCriteria
                                                     .getInstance()).isEmpty();
    }

    @Test
    public void testTrue() {
        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, TrueQueryCriteria.getInstance());

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(TrueQueryCriteria.getInstance());

        for (Integer integer : structures.keySet()) {
            check(integer, expected, result);
        }
    }

    @Test
    public void test1() {
        //query = {f1: typeof int}
        QueryCriteria query;

        query = new TypeIsQueryCriteria(f1Ref, ScalarType.INTEGER);

        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, query);

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(TrueQueryCriteria.getInstance());

        check(0, Collections.<QueryCriteria>emptySet(), result);
        check(1, expected, result);
        check(2, Collections.<QueryCriteria>emptySet(), result);
        check(3, Collections.<QueryCriteria>emptySet(), result);
        check(4, Collections.<QueryCriteria>emptySet(), result);
        check(5, Collections.<QueryCriteria>emptySet(), result);
        check(6, Collections.<QueryCriteria>emptySet(), result);
        check(7, Collections.<QueryCriteria>emptySet(), result);
        check(8, Collections.<QueryCriteria>emptySet(), result);
        check(9, Collections.<QueryCriteria>emptySet(), result);
        check(10, Collections.<QueryCriteria>emptySet(), result);
        check(11, Collections.<QueryCriteria>emptySet(), result);
        check(12, Collections.<QueryCriteria>emptySet(), result);
        check(13, Collections.<QueryCriteria>emptySet(), result);
    }

    /*
     * Tests that test the mongo query {'f1': 1}, that is translated to: + 'f1':
     * 1 -> tested in test2_1 + 'f1': exists ("" : 1) -> tested in test2_2
     */
    @Test
    public void test2_1() {
        //query = {f1: 1}
        QueryCriteria query;

        query = new IsEqualQueryCriteria(f1Ref, ScalarInteger.of(1));

        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, query);

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(new IsEqualQueryCriteria(f1Ref, ScalarInteger.of(1)));

        check(0, Collections.<QueryCriteria>emptySet(), result);
        check(1, expected, result);
        check(2, Collections.<QueryCriteria>emptySet(), result);
        check(3, Collections.<QueryCriteria>emptySet(), result);
        check(4, Collections.<QueryCriteria>emptySet(), result);
        check(5, Collections.<QueryCriteria>emptySet(), result);
        check(6, Collections.<QueryCriteria>emptySet(), result);
        check(7, Collections.<QueryCriteria>emptySet(), result);
        check(8, Collections.<QueryCriteria>emptySet(), result);
        check(9, Collections.<QueryCriteria>emptySet(), result);
        check(10, Collections.<QueryCriteria>emptySet(), result);
        check(11, Collections.<QueryCriteria>emptySet(), result);
        check(12, Collections.<QueryCriteria>emptySet(), result);
        check(13, Collections.<QueryCriteria>emptySet(), result);
    }

    @Test
    public void test2_2() {
        //query = {f1: exists("" = 1)}
        QueryCriteria query;

        query = new ExistsQueryCriteria(
                f1Ref,
                new IsEqualQueryCriteria(
                        AttributeReference.EMPTY_REFERENCE,
                        ScalarInteger.of(1)
                )
        );

        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, query);

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(
                new ExistsQueryCriteria(
                        f1Ref,
                        new IsEqualQueryCriteria(
                                AttributeReference.EMPTY_REFERENCE,
                                ScalarInteger.of(1)
                        )
                )
        );

        check(0, Collections.<QueryCriteria>emptySet(), result);
        check(1, Collections.<QueryCriteria>emptySet(), result);
        check(2, expected, result);
        check(3, Collections.<QueryCriteria>emptySet(), result);
        check(4, expected, result);
        check(5, expected, result);
        check(6, Collections.<QueryCriteria>emptySet(), result);
        check(7, Collections.<QueryCriteria>emptySet(), result);
        check(8, expected, result);
        check(9, expected, result);
        check(10, expected, result);
        check(11, Collections.<QueryCriteria>emptySet(), result);
        check(12, expected, result);
        check(13, expected, result);
    }

    /*
     * Tests that test the mongo query {'f1.f2': 1}, that is translated to: +
     * 'f1.f2': 1 -> tested in test3_1 + 'f1.f2': exists ('' : 1) -> tested in
     * test3_2 + 'f1': exists ('.f2' : 1) -> tested in test3_3 + 'f1': exists
     * ('.f2' : exists ('': 1)) -> tested in test3_4
     */
    @Test
    public void test3_1() {
        //query = {f1.f2: 1}
        QueryCriteria query;

        query = new IsEqualQueryCriteria(f1f2Ref, ScalarInteger.of(1));

        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, query);

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(new IsEqualQueryCriteria(f1f2Ref, ScalarInteger.of(1)));

        check(0, Collections.<QueryCriteria>emptySet(), result);
        check(1, Collections.<QueryCriteria>emptySet(), result);
        check(2, Collections.<QueryCriteria>emptySet(), result);
        check(3, expected, result);
        check(4, Collections.<QueryCriteria>emptySet(), result);
        check(5, Collections.<QueryCriteria>emptySet(), result);
        check(6, Collections.<QueryCriteria>emptySet(), result);
        check(7, Collections.<QueryCriteria>emptySet(), result);
        check(8, Collections.<QueryCriteria>emptySet(), result);
        check(9, Collections.<QueryCriteria>emptySet(), result);
        check(10, Collections.<QueryCriteria>emptySet(), result);
        check(11, Collections.<QueryCriteria>emptySet(), result);
        check(12, Collections.<QueryCriteria>emptySet(), result);
        check(13, Collections.<QueryCriteria>emptySet(), result);
    }

    @Test
    public void test3_2() {
        //query = {f1.f2: exists('': 1)}

        QueryCriteria query;

        query = new ExistsQueryCriteria(
                f1f2Ref,
                new IsEqualQueryCriteria(
                        AttributeReference.EMPTY_REFERENCE,
                        ScalarInteger.of(1)
                )
        );

        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, query);

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(
                new ExistsQueryCriteria(
                        f1f2Ref,
                        new IsEqualQueryCriteria(
                                AttributeReference.EMPTY_REFERENCE,
                                ScalarInteger.of(1)
                        )
                )
        );

        check(0, Collections.<QueryCriteria>emptySet(), result);
        check(1, Collections.<QueryCriteria>emptySet(), result);
        check(2, Collections.<QueryCriteria>emptySet(), result);
        check(3, Collections.<QueryCriteria>emptySet(), result);
        check(4, Collections.<QueryCriteria>emptySet(), result);
        check(5, Collections.<QueryCriteria>emptySet(), result);
        check(6, expected, result);
        check(7, Collections.<QueryCriteria>emptySet(), result);
        check(8, Collections.<QueryCriteria>emptySet(), result);
        check(9, Collections.<QueryCriteria>emptySet(), result);
        check(10, Collections.<QueryCriteria>emptySet(), result);
        check(11, expected, result);
        check(12, Collections.<QueryCriteria>emptySet(), result);
        check(13, Collections.<QueryCriteria>emptySet(), result);
    }

    @Test
    public void test3_3() {
        //query = {f1: exists('.f2': 1)}

        QueryCriteria query;

        query = new ExistsQueryCriteria(
                f1Ref,
                new IsEqualQueryCriteria(
                        f2Ref,
                        ScalarInteger.of(1)
                )
        );

        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, query);

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(
                new IsEqualQueryCriteria(
                        f1Ref.append(new AttributeReference.ArrayKey(0)).append(
                                f2Ref),
                        ScalarInteger.of(1))
        );

        check(0, Collections.<QueryCriteria>emptySet(), result);
        check(1, Collections.<QueryCriteria>emptySet(), result);
        check(2, expected, result);
        check(3, Collections.<QueryCriteria>emptySet(), result);
        check(4, Collections.<QueryCriteria>emptySet(), result);
        check(5, Collections.<QueryCriteria>emptySet(), result);
        check(6, Collections.<QueryCriteria>emptySet(), result);
        check(7, Collections.<QueryCriteria>emptySet(), result);
        check(8, Collections.<QueryCriteria>emptySet(), result);
        check(9, Collections.<QueryCriteria>emptySet(), result);
        check(10, Collections.<QueryCriteria>emptySet(), result);
        check(11, Collections.<QueryCriteria>emptySet(), result);
        check(12, Collections.<QueryCriteria>emptySet(), result);
        check(13, Collections.<QueryCriteria>emptySet(), result);
    }

    @Test
    public void test3_4() {
        //query = {f1: exists('f2' : exists('': 1))}

        QueryCriteria query;

        query = new ExistsQueryCriteria(
                f1Ref,
                new ExistsQueryCriteria(
                        f2Ref,
                        new IsEqualQueryCriteria(
                                AttributeReference.EMPTY_REFERENCE,
                                ScalarInteger.of(1)
                        )
                )
        );

        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, query);

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(
                new ExistsQueryCriteria(
                        f1Ref.append(new AttributeReference.ArrayKey(0)).append(
                                f2Ref),
                        new IsEqualQueryCriteria(
                                AttributeReference.EMPTY_REFERENCE,
                                ScalarInteger.of(1)
                        )
                )
        );

        check(0, Collections.<QueryCriteria>emptySet(), result);
        check(1, Collections.<QueryCriteria>emptySet(), result);
        check(2, Collections.<QueryCriteria>emptySet(), result);
        check(3, Collections.<QueryCriteria>emptySet(), result);
        check(4, expected, result);
        check(5, Collections.<QueryCriteria>emptySet(), result);
        check(6, Collections.<QueryCriteria>emptySet(), result);
        check(7, Collections.<QueryCriteria>emptySet(), result);
        check(8, Collections.<QueryCriteria>emptySet(), result);
        check(9, Collections.<QueryCriteria>emptySet(), result);
        check(10, Collections.<QueryCriteria>emptySet(), result);
        check(11, Collections.<QueryCriteria>emptySet(), result);
        check(12, expected, result);
        check(13, Collections.<QueryCriteria>emptySet(), result);
    }

    /*
     * Tests that test the mongo query {'f1: {f2': 1}}, that is translated to: +
     * 'f1.f2': 1 && 'f1: {atts: f2} -> tested in test4_1 + 'f1': exists ('f2' :
     * 1 and '': {atts: f2})-> tested in test4_2
     */
    @Test
    public void test4_1() {
        //query = {f1.f2': 1 && 'f1: {atts: f2}}

        QueryCriteria query;

        query = new ConjunctionBuilder()
                .add(
                        new IsEqualQueryCriteria(
                                f1f2Ref,
                                ScalarInteger.of(1)
                        )
                )
                .add(
                        new ContainsAttributesQueryCriteria(
                                f1Ref,
                                Collections.singletonList("f2"),
                                true
                        )
                )
                .add(
                        new ContainsAttributesQueryCriteria(
                                AttributeReference.EMPTY_REFERENCE,
                                Collections.singletonList("f1"),
                                true
                        )
                )
                .build();

        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, query);

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(
                new IsEqualQueryCriteria(
                        f1f2Ref,
                        ScalarInteger.of(1)
                )
        );

        check(0, Collections.<QueryCriteria>emptySet(), result);
        check(1, Collections.<QueryCriteria>emptySet(), result);
        check(2, Collections.<QueryCriteria>emptySet(), result);
        check(3, expected, result);
        check(4, Collections.<QueryCriteria>emptySet(), result);
        check(5, Collections.<QueryCriteria>emptySet(), result);
        check(6, Collections.<QueryCriteria>emptySet(), result);
        check(7, Collections.<QueryCriteria>emptySet(), result);
        check(8, Collections.<QueryCriteria>emptySet(), result);
        check(9, Collections.<QueryCriteria>emptySet(), result);
        check(10, Collections.<QueryCriteria>emptySet(), result);
        check(11, Collections.<QueryCriteria>emptySet(), result);
        check(12, Collections.<QueryCriteria>emptySet(), result);
        check(13, Collections.<QueryCriteria>emptySet(), result);
    }

    @Test
    public void test4_2() {
        //query = {'f1': exists ('f2': 1 && '': {atts: f2}}

        QueryCriteria query;

        query = new ExistsQueryCriteria(
                f1Ref,
                new ConjunctionBuilder()
                .add(
                        new IsEqualQueryCriteria(
                                f2Ref,
                                ScalarInteger.of(1)
                        )
                )
                .add(
                        new ContainsAttributesQueryCriteria(
                                AttributeReference.EMPTY_REFERENCE,
                                Collections.singletonList("f2"),
                                true
                        )
                )
                .build()
        );

        Multimap<Integer, QueryCriteria> result = QueryStructureFilter
                .filterStructures(cache, query);

        Set<QueryCriteria> expected = Sets.newHashSetWithExpectedSize(1);
        expected.add(
                new IsEqualQueryCriteria(
                        f1Ref.append(new AttributeReference.ArrayKey(0)).append(
                                f2Ref),
                        ScalarInteger.of(1)
                )
        );

        check(0, Collections.<QueryCriteria>emptySet(), result);
        check(1, Collections.<QueryCriteria>emptySet(), result);
        check(2, expected, result);
        check(3, Collections.<QueryCriteria>emptySet(), result);
        check(4, Collections.<QueryCriteria>emptySet(), result);
        check(5, Collections.<QueryCriteria>emptySet(), result);
        check(6, Collections.<QueryCriteria>emptySet(), result);
        check(7, Collections.<QueryCriteria>emptySet(), result);
        check(8, Collections.<QueryCriteria>emptySet(), result);
        check(9, Collections.<QueryCriteria>emptySet(), result);
        check(10, Collections.<QueryCriteria>emptySet(), result);
        check(11, Collections.<QueryCriteria>emptySet(), result);
        check(12, Collections.<QueryCriteria>emptySet(), result);
        check(13, Collections.<QueryCriteria>emptySet(), result);
    }

    private static void check(Integer structureId,
                              Set<QueryCriteria> expected,
                              Multimap<Integer, QueryCriteria> result) {
        Set<QueryCriteria> resultQueries = Sets.newHashSet(result.get(
                structureId));

        Set<QueryCriteria> temp = Sets.newHashSet(expected);
        ProcessorTestUtils.testQueryCriteriaDifference(resultQueries, expected);
    }

    private static Map<Integer, DocStructure> createStructuresFromText(
            Map<Integer, String> objectsAsText,
            Provider<SubDocType.Builder> subDocTypeBuilderProvider) {
        Map<Integer, BsonDocument> objects = Maps.newHashMapWithExpectedSize(
                objectsAsText.size());

        for (Map.Entry<Integer, String> entry : objectsAsText.entrySet()) {
            objects.put(entry.getKey(), parseDocument(entry.getValue()));
        }

        return createStructures(objects, subDocTypeBuilderProvider);
    }

    private static BsonDocument parseDocument(String json) {
        JsonReader reader = new JsonReader(json);

        DecoderContext context = DecoderContext.builder().build();

        org.bson.BsonDocument decoded = BSON_CODEC.decode(reader, context);

        return MongoBsonTranslator.translate(decoded);
    }

    private static Map<Integer, DocStructure> createStructures(
            Map<Integer, BsonDocument> objects,
            Provider<SubDocType.Builder> subDocTypeBuilderProvider) {
        Map<Integer, DocStructure> structures = Maps.newHashMapWithExpectedSize(
                objects.size());

        for (Map.Entry<Integer, BsonDocument> entry : objects.entrySet()) {
            structures.put(
                    entry.getKey(),
                    toStructure(
                            entry.getValue(),
                            subDocTypeBuilderProvider
                    )
            );
        }

        return structures;
    }

    private static StructuresCache createCache(
            Map<Integer, DocStructure> structures) {
        StructuresCache cache = Mockito.mock(StructuresCache.class,
                                             new ThrowsExceptionClass(
                                                     AssertionError.class));

        BiMap<Integer, DocStructure> structuresMap = HashBiMap
                .create(structures);

        Mockito.doReturn(structuresMap).when(cache).getAllStructures();

        return cache;
    }

    private static DocStructure toStructure(BsonDocument object, Provider<SubDocType.Builder> subDocTypeBuilderProvider) {
        KVDocument translated = (KVDocument) MongoWPConverter.translate(object);
        return toStructure(translated, subDocTypeBuilderProvider);
    }

    private static DocStructure toStructure(
            KVDocument object,
            Provider<SubDocType.Builder> subDocTypeBuilderProvider) {
        DocStructure.Builder structureBuilder = new DocStructure.Builder();
        SubDocType.Builder subDocTypeBuilder = subDocTypeBuilderProvider.get();

        for (DocEntry<?> entry : object) {
            if (entry.getValue() instanceof KVDocument) {
                structureBuilder.add(entry.getKey(), toStructure(
                        (KVDocument) entry.getValue(),
                        subDocTypeBuilderProvider
                ));
            }
            else {
                if (entry.getValue() instanceof KVArray) {
                    structureBuilder.add(entry.getKey(), toStructure(
                            (KVArray) entry.getValue(),
                            subDocTypeBuilderProvider)
                    );
                }

                ScalarType basicType = ScalarType.fromDocType(entry.getValue().getType());

                subDocTypeBuilder.add(new SubDocAttribute(entry.getKey(),
                                                          basicType));
            }
        }
        structureBuilder.setIndex(0);
        structureBuilder.setType(subDocTypeBuilder.build());

        return structureBuilder.built();
    }

    private static ArrayStructure toStructure(
            KVArray array,
            Provider<SubDocType.Builder> subDocTypeBuilderProvider) {
        ArrayStructure.Builder builder = new ArrayStructure.Builder();

        for (int i = 0; i < array.getValue().size(); i++) {
            KVValue<?> docValue = array.getValue().get(i);

            if (docValue instanceof KVDocument) {
                builder.add(i, toStructure((KVDocument) docValue, subDocTypeBuilderProvider));
            }
            else if (docValue instanceof KVArray) {
                builder.add(i, toStructure((KVArray) docValue, subDocTypeBuilderProvider));
            }
        }
        return builder.built();
    }

}
