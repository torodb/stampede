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
import java.util.Collection;
import java.util.List;

public abstract class JstestsDummyIT<T extends JstestsSuiteIT<
	? extends JstestsWorkingIT<T>, 
	? extends JstestsFailingIT<T>, 
	? extends JstestsFalsePositiveIT<T>, 
	? extends JstestsNotImplementedIT<T>, 
	? extends JstestsIgnoredIT<T>
	>> {
	protected final String test;
	protected final String pathPrefix;
	
	@SuppressWarnings("unchecked")
	public JstestsDummyIT(String test) {
		this.test = test;
		
		if (!JstestsSuiteIT.class.isAssignableFrom(getClass().getEnclosingClass())) {
			throw new RuntimeException(getClass().getName() + " must be enclosed in a class that implement " + JstestsSuiteIT.class.getName());
		}
		
		try {
			pathPrefix = ((Class<T>) getClass().getEnclosingClass()).newInstance().getJstestsPrefixPath();
		} catch(Exception exception) {
			throw new RuntimeException(exception);
		}
	}
	
	protected void runJstest(String test) throws Exception {
	}

	protected static Collection<Object[]> parameters(String[] tests) {
		List<Object[]> parameters = new ArrayList<Object[]>();
		
		for (String test : tests) {
			parameters.add(new Object[] { test });
		}
		
		return parameters;
	}

}
