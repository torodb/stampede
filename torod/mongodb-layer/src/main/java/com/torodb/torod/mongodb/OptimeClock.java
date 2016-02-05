
package com.torodb.torod.mongodb;

import com.eightkdata.mongowp.OpTime;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public interface OptimeClock {

    public OpTime tick();

}
