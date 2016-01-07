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

package com.torodb.integration.mongo.v3m0.jstests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.torodb.integration.config.Backend;
import com.torodb.integration.config.Protocol;
import com.torodb.integration.mongo.v3m0.jstests.JstestMetaInfo.JstestType;


@RunWith(Parameterized.class)
public class CustomIT extends JstestsIT {
	public CustomIT(String testResource, String prefix, Jstest jstest) {
		super(testResource, prefix, jstest);
	}

	@Test
	public void testJstest() throws Exception {
		runJstest();
	}
	
	@Parameters(name="{0}")
	public static Collection<Object[]> parameters() {
		return parameters(Tests.class, "custom");
	}
	
	public enum Tests implements Jstest {
		@JstestMetaInfo(type=JstestType.Working,protocols={Protocol.Mongo, Protocol.MongoReplSet},backends={Backend.Postgres, Backend.Greenplum})
		dummy("dummy.js");
		
		private final String[] testResources;
		
		private Tests(String...testResourceArray) {
			testResources = testResourceArray;
		}
		
		private Tests(String[]...testResourcesArray) {
			List<String> testResourceList = new ArrayList<String>();
			for (String[] testResources : testResourcesArray) {
				testResourceList.addAll(Arrays.asList(testResources));
			}
			this.testResources = testResourceList.toArray(new String[testResourceList.size()]);
		}
		
		@Override
		public String[] getTestResources() {
			return testResources;
		}
		
		@Override
		public JstestMetaInfo getJstestMetaInfoFor(String testResource, Protocol protocol, Backend backend) {
			return JstestType.getJstestMetaInfoFor(getClass(), testResource, protocol, backend);
		}
	}
}