package com.torodb.backend.rid;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.RidGenerator.DocPartRidGenerator;
import com.torodb.core.impl.TableRefFactoryImpl;

public class ReservedIdContainerTest {

	@Test
	public void whenTableRefDoesntExistsCallsToFactory(){
		ReservedIdInfoFactory factory = new ReservedIdInfoFactory() {
			
			@Override
			public ReservedIdInfo create(String dbName, String collectionName, TableRef tableRef) {
				return new ReservedIdInfo(0, 0);
			}
		};
		TableRefFactory tableRefFactory = new TableRefFactoryImpl();
		ReservedIdInfoFactory reservedIdInfoFactory = Mockito.spy(factory);
		ReservedIdContainer container=new ReservedIdContainer(reservedIdInfoFactory);
		DocPartRidGenerator docPartRidGenerator = container.getDocPartRidGenerator("myDB", "myCollection");
		int nextRid = docPartRidGenerator.nextRid(tableRefFactory.createRoot());
		Mockito.verify(reservedIdInfoFactory).create("myDB", "myCollection", tableRefFactory.createRoot());
		assertEquals(1,nextRid);
		
	}
	
}
