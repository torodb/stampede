
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import java.util.List;

/**
 *
 */
public class GetCollectionsMetainfoCallable extends Job<List<CollectionMetainfo>> {

    private final DbWrapper wrapper;
    private final GetCollectionsMetainfoCallable.Report report;

    public GetCollectionsMetainfoCallable(DbWrapper wrapper, Report report) {
        this.wrapper = wrapper;
        this.report = report;
    }

    @Override
    protected List<CollectionMetainfo> onFail(Throwable t) throws ToroException,
            ToroRuntimeException {
        if (t instanceof ToroException) {
            throw (ToroException) t;
        }
        throw new ToroRuntimeException(t);
    }
    
    @Override
    protected List<CollectionMetainfo> failableCall() throws ToroException,
            ToroRuntimeException {
        List<CollectionMetainfo> result;
        try {
            result = wrapper.consumeSessionDbConnection().getCollectionsMetainfo();
        }
        catch (ImplementationDbException ex) {
            throw new ToroRuntimeException(ex);
        }
        
        report.getCollectionsMetainfoExecuted(result);
        
        return result;
    }

    public static interface Report {
        public void getCollectionsMetainfoExecuted(List<CollectionMetainfo> metainfo);
    }
    
}
