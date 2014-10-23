/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.core.executor;

import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import java.io.Closeable;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public interface SessionExecutor extends Closeable {
    
    /**
     * Close the executor and rollback its changes since last commit.
     */
    @Override
    void close();

    /**
     * Stop the execution of new tasks of this executor until the {@linkplain SystemExecutor system executor}
     * associated with this {@link SessionExecutor} executor executes job marked with the given value.
     * <p>
     * Task that had been added before this method has been called are not affected, even if they have not been executed
     * yet.
     * @param tick 
     * @see SystemExecutor#getTick() 
     */
    void pauseUntil(long tick);

    SessionTransaction createTransaction() throws ImplementationDbException;
}
