
package com.torodb.mongodb.repl.oplogreplier;

import com.google.common.collect.Lists;
import com.google.inject.*;
import org.junit.Test;


import java.util.Collection;
import java.util.stream.Collectors;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;


/**
 *
 */
@RunWith(Parameterized.class)
public abstract class AbstractOplogApplierTest {

    @Parameter
    public String jsonFile;
    private static final OpTimeFactory opTimeFactory = new OpTimeFactory();
    @Rule
    public OplogTestContextResourceRule testContextResource =
            new OplogTestContextResourceRule(this::getMongodSpecificTestModule);
    public abstract Module getMongodSpecificTestModule();

    @Parameters
    public static Collection<Object[]> data() {
        return Lists.newArrayList(
                "doNothing",
                "dropDatabase",
                "insertRepeated",
                "insert_update_add",
                "letschat_upsert",
                "update_no_upsert",
                "update_upsert"
        ).stream().map(s -> new String[] {s + ".json"}).collect(Collectors.toList());
    }

    @Test
    public void test() throws Exception {
        BDDOplogTest test;
        try {
            test = OplogTestParser
                    .fromExtendedJsonResource(jsonFile);
        } catch (Throwable ex) {
            throw new AssertionError("Failed to parse '" + jsonFile + "'", ex);
        }

        Assume.assumeTrue("Test " + jsonFile + " marked as ignorable", !test.shouldIgnore());

        test.execute(testContextResource.getTestContext());
    }

}
