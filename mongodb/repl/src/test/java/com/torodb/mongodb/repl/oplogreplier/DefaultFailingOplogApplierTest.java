
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
                "emptyCommand"
        ));
    }

    @Test(expected=UnexpectedOplogApplierException.class)
    public void test() throws Exception {
        super.test();
    }

}
