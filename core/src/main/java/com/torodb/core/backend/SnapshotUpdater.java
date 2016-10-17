
package com.torodb.core.backend;

import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.transaction.metainf.MetainfoRepository;

/**
 *
 */
public interface SnapshotUpdater {

    /**
     * Updates the given metainf repository to add all meta structures stored on the database.
     *
     * @param metainfoRepository The repository where meta structures will be added. It should be empty.
     * @throws InvalidDatabaseException
     */
    public void updateSnapshot(MetainfoRepository metainfoRepository)
            throws InvalidDatabaseException;

}
