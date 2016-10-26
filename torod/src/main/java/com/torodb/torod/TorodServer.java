
package com.torodb.torod;

import com.torodb.core.services.TorodbService;

/**
 *
 */
public interface TorodServer extends TorodbService {

    public TorodConnection openConnection();

    public void disableDataImportMode();

    public void enableDataImportMode();
}
