
package com.torodb.torod.mongodb.commands;

import com.eightkdata.mongowp.WriteConcern;
import com.google.common.base.Function;
import com.torodb.torod.core.WriteFailMode;
import javax.inject.Singleton;

/**
 *
 */
public interface WriteConcernToWriteFailModeFunction extends Function<WriteConcern, WriteFailMode> {

    @Override
    public WriteFailMode apply(WriteConcern input);

    @Singleton
    public static class AlwaysTransactionalWriteFailMode implements WriteConcernToWriteFailModeFunction {

        @Override
        public WriteFailMode apply(WriteConcern input) {
            return WriteFailMode.TRANSACTIONAL;
        }

    }
}
