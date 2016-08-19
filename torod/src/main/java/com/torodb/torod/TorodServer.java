
package com.torodb.torod;

import com.google.common.util.concurrent.Service;

/**
 *
 */
public interface TorodServer extends Service {

    public TorodConnection openConnection();

    public void disableDataImportMode();

    public void enableDataImportMode();
}
