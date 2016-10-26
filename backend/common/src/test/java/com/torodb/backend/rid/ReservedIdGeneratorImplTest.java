package com.torodb.backend.rid;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.ReservedIdGenerator.DocPartRidGenerator;
import com.torodb.core.impl.TableRefFactoryImpl;

public class ReservedIdGeneratorImplTest {

	@Test
	public void whenTableRefDoesntExistsCallsToFactory(){
		ReservedIdInfoFactory factory = new MockedReservedIdInfoFactory();

        factory.startAsync();
        factory.awaitRunning();

		TableRefFactory tableRefFactory = new TableRefFactoryImpl();
		ReservedIdInfoFactory reservedIdInfoFactory = Mockito.spy(factory);
		ReservedIdGeneratorImpl container = new ReservedIdGeneratorImpl(
                reservedIdInfoFactory, new ThreadFactoryBuilder().build());
		DocPartRidGenerator docPartRidGenerator = container.getDocPartRidGenerator("myDB", "myCollection");
		int nextRid = docPartRidGenerator.nextRid(tableRefFactory.createRoot());
		Mockito.verify(reservedIdInfoFactory).create("myDB", "myCollection", tableRefFactory.createRoot());
		assertEquals(1,nextRid);
		
	}


    private static class MockedReservedIdInfoFactory extends AbstractIdleService implements ReservedIdInfoFactory {

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
