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

import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.torodb.integration.config.Backend;
import com.torodb.integration.mongo.v3m0.jstests.JstestMetaInfo.JstestType;

@RunWith(Parameterized.class)
public class ToolIT extends JstestsIT {
	public ToolIT(String testResource, String prefix, Jstest jstest) {
		super(testResource, prefix, jstest);
	}

	@Test
	public void testJstest() throws Exception {
		runJstest();
	}
	
	@Parameters(name="{0}")
	public static Collection<Object[]> parameters() {
		return parameters(Tests.class, "tool");
	}
	
	public enum Tests implements Jstest {
		@JstestMetaInfo(type=JstestType.Working,backends={Backend.Postgres,Backend.Greenplum})
		working(WORKING),
		@JstestMetaInfo(type=JstestType.Failing,backends={Backend.Postgres,Backend.Greenplum})
		failing(FAILING),
		@JstestMetaInfo(type=JstestType.FalsePositive,backends={Backend.Postgres,Backend.Greenplum})
		falsePositive(FALSE_POSITIVE),
		@JstestMetaInfo(type=JstestType.NotImplemented,backends={Backend.Postgres,Backend.Greenplum})
		notImplemented(NOT_IMPLEMENTED),
		@JstestMetaInfo(type=JstestType.Ignored,backends={Backend.Postgres,Backend.Greenplum})
		ignored(IGNORED);
		
		private final String[] testResources;
		
		private Tests(String testResource) {
			testResources = new String[] { testResource };
		}
		
		private Tests(String[] testResources) {
			this.testResources = testResources;
		}
		
		@Override
		public String[] getTestResources() {
			return testResources;
		}
		
		@Override
		public JstestMetaInfo getJstestMetaInfoFor(String testResource, Backend backend) {
			return JstestType.getJstestMetaInfoFor(getClass(), testResource, backend);
		}
	}

	
	private static final String[] WORKING = new String[] {
			"command_line_quotes.js",
	};
	
	private static final String[] FAILING = new String[] {
			"csv1.js",
			"csvexport1.js",
			"csvimport1.js",
			"dumpauth.js",
			"dumpfilename1.js",
			"dumprestore_auth.js",
			"dumprestore_auth2.js",
			"dumprestore_auth3.js",
			"dumprestore_excludecollections.js",
			"dumprestore1.js",
			"dumprestore10.js",
			"dumprestore3.js",
			"dumprestore4.js",
			"dumprestore7.js",
			"dumprestore8.js",
			"dumprestoreWithNoOptions.js",
			"dumpsecondary.js",
			"exportimport_date.js",
			"exportimport1.js",
			"exportimport3.js",
			"exportimport4.js",
			"exportimport5.js",
			"exportimport6.js",
			"files1.js",
			"gridfs.js",
			"oplog_all_ops.js",
			"oplog1.js",
			"restorewithauth.js",
			"stat1.js",
			"tool_replset.js",
			"tool1.js",
			"tsv1.js",
	};
	
	private static final String[] FALSE_POSITIVE = new String[] {
			"exportimport_bigarray.js",
	};
	
	private static final String[] NOT_IMPLEMENTED = new String[] {
			"dumprestore9.js",
			"exportimport_minkey_maxkey.js",
	};
	
	private static final String[] IGNORED = new String[] {
			"csvexport2.js",
	};
}