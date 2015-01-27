
package com.torodb.torod.db.sql.index;

import java.io.Serializable;

/**
 *
 */
public interface DbIndex extends Serializable {

    public String getSchema();
    
    public String getTable();
    
    public String getColumn();
    
    public UnnamedDbIndex asUnnamed();
    
}
