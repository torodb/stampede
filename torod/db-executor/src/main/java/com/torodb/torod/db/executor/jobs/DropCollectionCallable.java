
package com.torodb.torod.db.executor.jobs;

import com.google.common.base.Supplier;
import com.torodb.torod.core.dbWrapper.DbConnection;
import java.util.concurrent.Callable;

/**
 *
 */
public class DropCollectionCallable implements Callable<Void> {

    private final Supplier<DbConnection> connectionProvider;
    private final String collection;

    public DropCollectionCallable(Supplier<DbConnection> connectionProvider, String collection) {
        this.connectionProvider = connectionProvider;
        this.collection = collection;
    }

    @Override
    public Void call() throws Exception {
        connectionProvider.get().dropCollection(collection);
        return null;
    }

}
