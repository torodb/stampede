/*
 * MongoWP - ToroDB-poc: Backends benchmark
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
package com.torodb.backend.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.d2r.ReservedIdGenerator.DocPartRidGenerator;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.services.IdleTorodbService;
import java.util.concurrent.ThreadFactory;

public class InMemoryRidGenerator extends IdleTorodbService implements ReservedIdGenerator, DocPartRidGenerator {
	
	private AtomicInteger global=new AtomicInteger(0);

    public InMemoryRidGenerator(@TorodbIdleService ThreadFactory threadFactory) {
        super(threadFactory);
    }
	
	@Override
	public int nextRid(String dbName, String collectionName, TableRef tableRef) {
		return global.getAndIncrement();
	}

	@Override
	public DocPartRidGenerator getDocPartRidGenerator(String dbName, String collectionName) {
		return this;
	}

	@Override
	public int nextRid(TableRef tableRef) {
		return global.getAndIncrement();
	}

    @Override
    public void setNextRid(TableRef tableRef, int nextRid) {
        global.set(nextRid);
    }

    @Override
    public void setNextRid(String dbName, String collectionName, TableRef tableRef, int nextRid) {
        global.set(nextRid);
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
    }

}
