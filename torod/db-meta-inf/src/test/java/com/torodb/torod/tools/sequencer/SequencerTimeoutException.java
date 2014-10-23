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

package com.torodb.torod.tools.sequencer;




import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class SequencerTimeoutException extends TimeoutException {
    private static final long serialVersionUID = 1L;
    private final ArrayList<String> blockedThreads;

    public SequencerTimeoutException(Collection<Thread> blockedThreads) {
        this.blockedThreads = Lists.newArrayList();
        
        for (Thread thread : blockedThreads) {
            this.blockedThreads.add(thread.toString());
        }
    }

    @Override
    public String toString() {
        return "Threads " + Joiner.on(",").join(blockedThreads) + " didn't finish in the expected time";
    }
}
