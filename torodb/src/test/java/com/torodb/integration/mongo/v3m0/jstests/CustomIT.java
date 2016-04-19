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

import static com.torodb.integration.Backend.*;
import static com.torodb.integration.Protocol.*;
import static com.torodb.integration.TestCategory.FAILING;
import static com.torodb.integration.TestCategory.WORKING;

import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.torodb.integration.mongo.v3m0.jstests.ScriptClassifier.Builder;


@RunWith(Parameterized.class)
public class CustomIT extends AbstractIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomIT.class);

	public CustomIT() {
		super(LOGGER);
	}
	
	@Parameters(name="{0}-{1}")
	public static Collection<Object[]> parameters() {
		return parameters(createScriptClassifier());
	}

    private static ScriptClassifier createScriptClassifier() {
        return new Builder()
                .addScripts(Mongo, Postgres, WORKING, asScriptSet("dummy.js"))
                .addScripts(MongoReplSet, Postgres, WORKING, asScriptSet("dummy.js"))
                .addScripts(Mongo, Greenplum, WORKING, asScriptSet("dummy.js"))
                .addScripts(MongoReplSet, Greenplum, WORKING, asScriptSet("dummy.js"))
                .addScripts(Mongo, MySQL, WORKING, asScriptSet("dummy.js"))
                .addScripts(MongoReplSet, MySQL, WORKING, asScriptSet("dummy.js"))

                .addScripts(Mongo, Postgres, WORKING, asScriptSet(new String[] {"binary.js", "undefined.js"}))
                .addScripts(Mongo, MySQL, WORKING, asScriptSet(new String[] {"binary.js", "undefined.js"}))

                .addScripts(Mongo, Postgres, FAILING, asScriptSet("alwaysfail.js"))
                .addScripts(Mongo, MySQL, FAILING, asScriptSet("alwaysfail.js"))
                .build();
    }

    private static Set<Script> asScriptSet(String scriptName) {
        return asScriptSet(new String[] {scriptName});
    }

    private static Set<Script> asScriptSet(String[] scriptNames) {
        HashSet<Script> result = new HashSet<>(scriptNames.length);
        for (String scriptName : scriptNames) {
            String relativePath = "custom/" + scriptName;
            result.add(new Script(relativePath, createURL(relativePath)));
        }

        return result;
    }

    private static URL createURL(String relativePath) {
        return ToolIT.class.getResource(relativePath);
    }
}