/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with torodb. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.integration.mongo.v3m0.jstests;

import com.google.common.base.Preconditions;
import java.net.URL;

/**
 *
 */
public class Script {

    private final String scriptName;
	private final URL testResourceUrl;
	private final Integer threads;

    public Script(String scriptName, URL testResourceUrl) {
        Preconditions.checkArgument(testResourceUrl != null, "Illegal construction of script %s. Its resource is null", scriptName);
        this.scriptName = scriptName;
        this.testResourceUrl = testResourceUrl;
        this.threads = null;
    }

    public Script(String scriptName, URL testResourceUrl, Integer threads) {
        Preconditions.checkArgument(testResourceUrl != null, "Illegal construction of script %s. Its resource is null", scriptName);
        this.scriptName = scriptName;
        this.testResourceUrl = testResourceUrl;
        this.threads = threads;
    }

    URL getURL() {
        return testResourceUrl;
    }

    Integer getThreads() {
        return threads;
    }

    @Override
    public String toString() {
        return scriptName;
    }
}
