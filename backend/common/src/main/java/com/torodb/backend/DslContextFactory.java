
package com.torodb.backend;

import java.sql.Connection;

import org.jooq.DSLContext;

/**
 * Given a connection, this factory generates a configured DSLContext.
 */
@FunctionalInterface
public interface DslContextFactory {

    public DSLContext createDSLContext(Connection connection);

}
