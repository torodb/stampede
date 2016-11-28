/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.rid;

import static org.junit.Assert.assertEquals;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.ReservedIdGenerator.DocPartRidGenerator;
import com.torodb.core.impl.TableRefFactoryImpl;
import org.junit.Test;
import org.mockito.Mockito;

public class ReservedIdGeneratorImplTest {

  @Test
  public void whenTableRefDoesntExistsCallsToFactory() {
    ReservedIdInfoFactory factory = new MockedReservedIdInfoFactory();

    factory.startAsync();
    factory.awaitRunning();

    TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    ReservedIdInfoFactory reservedIdInfoFactory = Mockito.spy(factory);
    ReservedIdGeneratorImpl container = new ReservedIdGeneratorImpl(
        reservedIdInfoFactory, new ThreadFactoryBuilder().build());
    DocPartRidGenerator docPartRidGenerator = container.getDocPartRidGenerator("myDB",
        "myCollection");
    int nextRid = docPartRidGenerator.nextRid(tableRefFactory.createRoot());
    Mockito.verify(reservedIdInfoFactory).create("myDB", "myCollection", tableRefFactory
        .createRoot());
    assertEquals(1, nextRid);

  }

  private static class MockedReservedIdInfoFactory extends AbstractIdleService implements
      ReservedIdInfoFactory {

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
    }

    @Override
    public ReservedIdInfo create(String dbName, String collectionName, TableRef tableRef) {
      return new ReservedIdInfo(0, 0);
    }

  }
}
