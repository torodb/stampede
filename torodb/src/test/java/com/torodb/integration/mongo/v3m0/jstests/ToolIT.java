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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class ToolIT implements JstestsSuiteIT<
		ToolIT.ToolWorkingIT, 
		ToolIT.ToolFailingIT, 
		ToolIT.ToolFalsePositiveIT, 
		ToolIT.ToolNotImplementedIT, 
		ToolIT.ToolIgnoredIT
		> {
	@Override
	public String getJstestsPrefixPath() {
		return "tool";
	}
	
	@RunWith(Parameterized.class)
	public static class ToolWorkingIT extends JstestsWorkingIT<ToolIT> {
		public ToolWorkingIT(String test) {
			super(test);
		}

		@Test
		public void testJstest() throws Exception {
			runJstest(test);
		}
		
		@Parameters(name="{0}")
		public static Collection<Object[]> parameters() {
			return parameters(WORKING);
		}
	}

	@RunWith(Parameterized.class)
	public static class ToolFailingIT extends JstestsFailingIT<ToolIT> {
		public ToolFailingIT(String test) {
			super(test);
		}

		@Test
		@Ignore
		public void testJstest() throws Exception {
			runJstest(test);
		}
		
		@Parameters(name="{0}")
		public static Collection<Object[]> parameters() {
			return parameters(FAILING);
		}
	}

	@RunWith(Parameterized.class)
	public static class ToolFalsePositiveIT extends JstestsFalsePositiveIT<ToolIT> {
		public ToolFalsePositiveIT(String test) {
			super(test);
		}

		@Test(expected=AssertionError.class)
		@Ignore
		public void testJstest() throws Exception {
			runJstest(test);
		}
		
		@Parameters(name="{0}")
		public static Collection<Object[]> parameters() {
			return parameters(FALSE_POSITIVE);
		}
	}

	@RunWith(Parameterized.class)
	public static class ToolNotImplementedIT extends JstestsNotImplementedIT<ToolIT> {
		public ToolNotImplementedIT(String test) {
			super(test);
		}

		@Test(expected=AssertionError.class)
		@Ignore
		public void testJstest() throws Exception {
			runJstest(test);
		}
		
		@Parameters(name="{0}")
		public static Collection<Object[]> parameters() {
			return parameters(NOT_IMPLEMENTED);
		}
	}

	@RunWith(Parameterized.class)
	public static class ToolIgnoredIT extends JstestsIgnoredIT<ToolIT> {
		public ToolIgnoredIT(String test) {
			super(test);
		}

		@Test(expected=AssertionError.class)
		@Ignore
		public void testJstest() throws Exception {
			runJstest(test);
		}
		
		@Parameters(name="{0}")
		public static Collection<Object[]> parameters() {
			return parameters(IGNORED);
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