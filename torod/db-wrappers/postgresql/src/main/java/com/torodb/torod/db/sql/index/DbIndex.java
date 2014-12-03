
package com.torodb.torod.db.sql.index;

import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 *
 */
public interface DbIndex {

    public String getSchema();

    public boolean isUnique();

    public ImmutableList<IndexedColumnInfo> getColumns();
    
    public UnnamedDbIndex asUnnamed();
    
}
