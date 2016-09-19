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

package com.torodb.integration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.torodb.packaging.ToroDbServer;

public class ToroRunnerClassRule extends AbstractBackendRunnerClassRule {

	private final Set<Throwable> UNCAUGHT_EXCEPTIONS = new HashSet<>();

	public ToroRunnerClassRule() {
        super();
    }

    public void addUncaughtException(Throwable throwable) {
		synchronized (UNCAUGHT_EXCEPTIONS) {
			UNCAUGHT_EXCEPTIONS.add(throwable);
		};
	}

	public List<Throwable> getUcaughtExceptions() {
		List<Throwable> ucaughtExceptions;
		synchronized (UNCAUGHT_EXCEPTIONS) {
			ucaughtExceptions = new ArrayList<>(UNCAUGHT_EXCEPTIONS);
			UNCAUGHT_EXCEPTIONS.clear();
		}
		return ucaughtExceptions;
	}

	private ToroDbServer torodbServer;

    @Override
    protected void startUp() throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(
                new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        addUncaughtException(e);
                    }
                });
        
        torodbServer = getInjector().getInstance(ToroDbServer.class);
        torodbServer.startAsync();
        torodbServer.awaitRunning();
    }

    @Override
    protected void shutDown() throws Exception {
        ToroDbServer torodbServer = this.torodbServer;
        if (torodbServer != null) {
            torodbServer.stopAsync();
            torodbServer.awaitTerminated();
        }
        
        List<Throwable> exceptions = getUcaughtExceptions();
        if (!exceptions.isEmpty()) {
            throw new RuntimeException(exceptions.get(0));
        }
    }

}
