
package com.torodb.torod.db.backends.meta.routines;

import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.sql.AutoCloser;
import com.torodb.torod.db.backends.tables.CollectionsTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 */
public class DropCollection {

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public static void execute(
            Configuration jooqConf, IndexStorage.CollectionSchema colSchema, @Nonnull DatabaseInterface databaseInterface
    ) {
        
        ConnectionProvider provider = jooqConf.connectionProvider();
        Connection connection = provider.acquire();
        Statement st = null;
        try {
            st = connection.createStatement();
            st.executeUpdate(databaseInterface.dropSchemaStatement(colSchema.getName()));
            
            DSLContext dsl = DSL.using(jooqConf);
            int deleted = dsl.deleteFrom(CollectionsTable.COLLECTIONS)
                    .where(
                            CollectionsTable.COLLECTIONS.NAME
                            .eq(colSchema.getCollection()))
                    .execute();
            assert deleted == 1;
        }
        catch (SQLException ex) {
            throw new ToroImplementationException(ex);
        } finally {
            AutoCloser.close(st);
        }
    }
}
