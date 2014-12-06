
package com.torodb.torod.db.executor.report;

import com.torodb.torod.core.subdocument.SubDocType;

/**
 *
 */
public interface CreateSubDocTableReport {

    public void taskExecuted(String collection, SubDocType type);
    
}
