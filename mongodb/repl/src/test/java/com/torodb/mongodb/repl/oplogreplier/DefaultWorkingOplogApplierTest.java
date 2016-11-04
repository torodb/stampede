
package com.torodb.mongodb.repl.oplogreplier;

import java.util.Collection;

import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;


/**
 *
 */
public class DefaultWorkingOplogApplierTest extends DefaultOplogApplierTest {

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return loadData(Lists.newArrayList(
                "deleteIndex",
                "deleteIndexes",
                "doNothing",
                "dropDatabase",
                "dropIndex",
                "dropIndexes",
                "insertRepeated",
                "insert_update_add",
                "letschat_upsert",
                "update_array",
                "update_no_upsert",
                "update_upsert",
                "renameIndex_noDropTarget",
                "renameIndex_dropTarget"
        ));
    }

    @Test
    public void test() throws Exception {
        super.test();
    }

}
