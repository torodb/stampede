
package com.torodb.backend.jobs;

import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.dsl.backend.AddScalarDDLJob;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaScalar;

/**
 *
 */
public class AddScalarDDLJobImpl implements AddScalarDDLJob {

    private final MetaDatabase db;
    private final MetaCollection col;
    private final MetaDocPart docPart;
    private final MetaScalar newScalar;

    public AddScalarDDLJobImpl(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaScalar newScalar) {
        this.db = db;
        this.col = col;
        this.docPart = docPart;
        this.newScalar = newScalar;
    }

    @Override
    public MetaDatabase getDatabase() {
        return db;
    }

    @Override
    public MetaCollection getCollection() {
        return col;
    }

    @Override
    public MetaDocPart getDocPart() {
        return docPart;
    }

    @Override
    public MetaScalar getScalar() {
        return newScalar;
    }

    @Override
    public void execute(WriteBackendTransaction connection) throws UserException {
        connection.addScalar(db, col, docPart, newScalar);
    }

}
