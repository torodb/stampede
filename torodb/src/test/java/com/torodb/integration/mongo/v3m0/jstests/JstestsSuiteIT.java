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

public interface JstestsSuiteIT<
		WORKING extends JstestsWorkingIT<? extends JstestsSuiteIT<WORKING, FAILING, FALSE_POSITIVE, NOT_IMPLEMENTED, IGNORED>>, 
		FAILING extends JstestsFailingIT<? extends JstestsSuiteIT<WORKING, FAILING, FALSE_POSITIVE, NOT_IMPLEMENTED, IGNORED>>, 
		FALSE_POSITIVE extends JstestsFalsePositiveIT<? extends JstestsSuiteIT<WORKING, FAILING, FALSE_POSITIVE, NOT_IMPLEMENTED, IGNORED>>, 
		NOT_IMPLEMENTED extends JstestsNotImplementedIT<? extends JstestsSuiteIT<WORKING, FAILING, FALSE_POSITIVE, NOT_IMPLEMENTED, IGNORED>>, 
		IGNORED extends JstestsIgnoredIT<? extends JstestsSuiteIT<WORKING, FAILING, FALSE_POSITIVE, NOT_IMPLEMENTED, IGNORED>>
		> {
	public String getJstestsPrefixPath();
}
