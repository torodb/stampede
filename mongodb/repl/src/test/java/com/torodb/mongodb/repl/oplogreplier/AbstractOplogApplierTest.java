
package com.torodb.mongodb.repl.oplogreplier;

import com.google.common.collect.Lists;
import com.google.inject.Module;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;


/**
 *
 */
@RunWith(Parameterized.class)
public abstract class AbstractOplogApplierTest {

    @Parameter(0)
    public String testName;
    @Parameter(1)
    public OplogTest oplogTest;
    private static final OpTimeFactory opTimeFactory = new OpTimeFactory();
    @Rule
    public OplogTestContextResourceRule testContextResource =
            new OplogTestContextResourceRule(this::getMongodSpecificTestModule);
    public abstract Module getMongodSpecificTestModule();

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        ArrayList<String> filesList = Lists.newArrayList(
                "deleteIndex",
                "deleteIndexes",
                "doNothing",
                "dropDatabase",
                "dropIndex",
                "dropIndexes",
                "insertRepeated",
                "insert_update_add",
                "letschat_upsert",
                "update_no_upsert",
                "update_upsert"
        );
        return filesList.stream()
                .map(filename -> Tuple.tuple(filename, filename + ".json"))
                .map(Unchecked.function((Tuple2<String, String> tuple) -> {
                    try {
                        OplogTest test = OplogTestParser.fromExtendedJsonResource(tuple.v2);
                        return Tuple.tuple(tuple.v1, test);
                    } catch (Throwable ex) {
                        throw new AssertionError("Failed to parse '" + tuple.v2 + "'", ex);
                    }
                }))
                .map(tuple -> new Object[] {
                    tuple.v2.getTestName().orElse(tuple.v1),
                    tuple.v2
                })
                .collect(Collectors.toList());
    }

    @Test
    public void test() throws Exception {
        Assume.assumeTrue("Test " + this.oplogTest + " marked as ignorable", !oplogTest.shouldIgnore());

        oplogTest.execute(testContextResource.getTestContext());
    }

}
