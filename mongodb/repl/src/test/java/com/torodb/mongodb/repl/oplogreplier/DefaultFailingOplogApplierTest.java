
package com.torodb.mongodb.repl.oplogreplier;

import java.util.Collection;

import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier.UnexpectedOplogApplierException;


/**
 *
 */
public class DefaultFailingOplogApplierTest extends DefaultOplogApplierTest {

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return loadData(Lists.newArrayList(
                "unknownCommand",
                "emptyCommand",
                "insert_without_id",
                "update_no_upsert_without_id",
                "delete_without_id"
        ));
    }

    @Test(expected=UnexpectedOplogApplierException.class)
    public void test() throws Exception {
        super.test();
    }

}
