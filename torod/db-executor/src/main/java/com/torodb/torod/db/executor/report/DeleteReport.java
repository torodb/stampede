
package com.torodb.torod.db.executor.report;

import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.language.operations.DeleteOperation;
import java.util.List;

/**
 *
 */
public interface DeleteReport {

    public void taskExecuted(
            String collection, 
            List<? extends DeleteOperation> deletes, 
            WriteFailMode mode);
}
