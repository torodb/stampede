
package com.torodb.torod.db.backends.meta.routines;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.Nonnull;

import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.meta.CollectionSchema;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public class DropCollection {

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public static void execute(
            Configuration jooqConf, CollectionSchema colSchema, @Nonnull DatabaseInterface databaseInterface
    ) {
        
        ConnectionProvider provider = jooqConf.connectionProvider();
        Connection connection = provider.acquire();

        try (Statement st = connection.createStatement()) {
            st.executeUpdate(databaseInterface.dropSchemaStatement(colSchema.getName()));
            
            DSLContext dsl = DSL.using(jooqConf);
            int deleted = dsl.deleteFrom(databaseInterface.getCollectionsTable())
                    .where(
                            databaseInterface.getCollectionsTable().NAME
                            .eq(colSchema.getCollection()))
                    .execute();
            assert deleted == 1;
        }
        catch (SQLException ex) {
            throw new ToroImplementationException(ex);
        }
    }
}
