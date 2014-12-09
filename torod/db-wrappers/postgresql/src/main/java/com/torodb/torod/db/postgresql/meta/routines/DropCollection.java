
package com.torodb.torod.db.postgresql.meta.routines;

import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.tables.SubDocTable;
import com.torodb.torod.db.sql.AutoCloser;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import org.jooq.*;
import org.jooq.impl.DSL;

/**
 *
 */
public class DropCollection {

    public static void execute(Configuration jooqConf, CollectionSchema colSchema) {
        DSLContext dsl = DSL.using(jooqConf);
        for (SubDocTable subDocTable : colSchema.getSubDocTables()) {
            deleteAll(dsl, subDocTable);
        }
        
        Table<Record> rootTable = DSL.tableByName(colSchema.getName(), "root");
        deleteAll(dsl, rootTable);
    }
    
    private static void deleteAll(DSLContext dsl, Table table) {
        dsl.delete(table)
                .execute();
    }

}
