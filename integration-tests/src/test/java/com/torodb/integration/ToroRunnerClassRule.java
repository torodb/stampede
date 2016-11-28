
/*
 * ToroDB - ToroDB-poc: Integration Tests
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.integration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    @Override
    protected void startUp() throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(
                new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        addUncaughtException(e);
                    }
                });

        getService().startBackendBundle();
        getService().startTorodBundle();
    }

    @Override
    protected void shutDown() throws Exception {
        if (getService() != null) {
            getService().shutDown();
        }
        
        List<Throwable> exceptions = getUcaughtExceptions();
        if (!exceptions.isEmpty()) {
            throw new RuntimeException(exceptions.get(0));
        }
    }

}
