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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;

import com.google.common.base.Charsets;
import com.torodb.config.model.Config;
import com.torodb.integration.ToroRunnerClassRule;

public abstract class JstestsIT {

	@ClassRule
	public final static ToroRunnerClassRule toroRunnerClassRule = new ToroRunnerClassRule();
	@Rule
	public final MongoJstestIgnoreRule jstestIgnoreRule = new MongoJstestIgnoreRule();

	private final Jstest jstest;
	private final String testResource;
	private final String prefix;
	private final URL testResourceUrl;
	
	public JstestsIT(String testResource, String prefix, Jstest jstest) {
		this.jstest = jstest;
		this.testResource = testResource;
		this.prefix = prefix;
		this.testResourceUrl = Jstest.class.getResource(prefix + testResource);
	}

	public Jstest getJstest() {
		return jstest;
	}

	public URL getTestResourceUrl() {
		return testResourceUrl;
	}

	public String getTestResource() {
		return testResource;
	}

	public String getPrefix() {
		return prefix;
	}

	protected void runJstest() throws Exception {
		Config config = toroRunnerClassRule.getConfig();
		String toroConnectionString = config.getProtocol().getMongo().getNet().getBindIp() + ":"
				+ config.getProtocol().getMongo().getNet().getPort() + "/"
				+ config.getBackend().asPostgres().getDatabase();
		URL mongoMocksUrl = Jstest.class.getResource("mongo_mocks.js");
		
		Process mongoProcess = Runtime.getRuntime()
				.exec(new String[] {
					"mongo",
					toroConnectionString, 
					mongoMocksUrl.getPath(),
					testResourceUrl.getPath(),
				});
		InputStream inputStream = mongoProcess.getInputStream();
		InputStream erroStream = mongoProcess.getErrorStream();
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		
		int result = mongoProcess.waitFor();
		
		List<Throwable> uncaughtExceptions = toroRunnerClassRule.getUcaughtExceptions();
		
		if (result != 0) {
			int read;
			
			while ((read = inputStream.read()) != -1) {
				byteArrayOutputStream.write(read);
			}

			while ((read = erroStream.read()) != -1) {
				byteArrayOutputStream.write(read);
			}
		}
		
		if (!uncaughtExceptions.isEmpty()) {
			PrintStream printStream = new PrintStream(byteArrayOutputStream);
			
			for (Throwable throwable : uncaughtExceptions) {
				throwable.printStackTrace(printStream);
			}
		}
		
		Assert.assertEquals("Test " + testResourceUrl.getFile() + " failed:\n" + new String(byteArrayOutputStream.toByteArray(), Charsets.UTF_8), 0, result);
		
		Assert.assertTrue("Test " + testResourceUrl.getFile() + " did not failed but following exception where received:\n" + new String(byteArrayOutputStream.toByteArray(), Charsets.UTF_8), uncaughtExceptions.isEmpty());
	}

	protected static Collection<Object[]> parameters(Class<? extends Jstest> jstestsClass, String prefix) {
		try {
			prefix = prefix + "/";
			List<Object[]> parameters = new ArrayList<Object[]>();
			
			for (Jstest jstest : (Jstest[]) jstestsClass.getMethod("values").invoke(null)) {
				String[] testResources = jstest.getTestResources();
				for (int index = 0; index < testResources.length; index++) {
					parameters.add(new Object[] { testResources[index], prefix, jstest });
				}
			}
			
			return parameters;
		} catch(Exception exception) {
			throw new RuntimeException(exception);
		}
	}
}