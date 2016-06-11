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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.torodb.integration.mongo.v3m0.jstests.ScriptClassifier.Builder;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.torodb.integration.Backend.*;
import static com.torodb.integration.Protocol.*;
import static com.torodb.integration.TestCategory.*;

@RunWith(Parameterized.class)
public class ToolIT extends AbstractIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ToolIT.class);

	public ToolIT() {
		super(LOGGER);
	}
	
	@Parameters(name="{0}-{1}")
	public static Collection<Object[]> parameters() {
		return parameters(createScriptClassifier());
	}

    private static ScriptClassifier createScriptClassifier() {
        Set<Script> workingSet = getWorkingSet(),
                catastrophicSet = getCatastrophicSet(),
                falsePositiveSet = getFalsePositiveSet(),
                notImplementedSet = getNotImplementedSet(),
                ignoredSet = getIgnoredSet(),
                allSet = Sets.newHashSet(Iterables.concat(workingSet, catastrophicSet, falsePositiveSet, notImplementedSet, ignoredSet));
        return new Builder()
                .addScripts(Mongo, Postgres, WORKING, workingSet)
                .addScripts(Mongo, MySQL, WORKING, workingSet)

                .addScripts(Mongo, Greenplum, CATASTROPHIC, workingSet)

                .addScripts(Mongo, Postgres, CATASTROPHIC, catastrophicSet)
                .addScripts(Mongo, Greenplum, CATASTROPHIC, catastrophicSet)
                .addScripts(Mongo, MySQL, CATASTROPHIC, catastrophicSet)

                .addScripts(Mongo, Postgres, FALSE_POSITIVE, falsePositiveSet)
                .addScripts(Mongo, Greenplum, FALSE_POSITIVE, falsePositiveSet)
                .addScripts(Mongo, MySQL, FALSE_POSITIVE, falsePositiveSet)

                .addScripts(Mongo, Postgres, NOT_IMPLEMENTED, notImplementedSet)
                .addScripts(Mongo, Greenplum, NOT_IMPLEMENTED, notImplementedSet)
                .addScripts(Mongo, MySQL, NOT_IMPLEMENTED, notImplementedSet)

                .addScripts(Mongo, Postgres, IGNORED, ignoredSet)
                .addScripts(Mongo, Greenplum, IGNORED, ignoredSet)
                .addScripts(Mongo, MySQL, IGNORED, ignoredSet)

                .addScripts(MongoReplSet, Postgres, CATASTROPHIC, allSet)
                .addScripts(MongoReplSet, Greenplum, CATASTROPHIC, allSet)
                .addScripts(MongoReplSet, MySQL, CATASTROPHIC, allSet)

                .build();
    }

    private static Set<Script> asScriptSet(String[] scriptNames) {
        HashSet<Script> result = new HashSet<>(scriptNames.length);
        for (String scriptName : scriptNames) {
            String relativePath = "tool/" + scriptName;
            result.add(new Script(relativePath, createURL(relativePath)));
        }

        return result;
    }

    private static URL createURL(String relativePath) {
        return ToolIT.class.getResource(relativePath);
    }
	
	private static Set<Script> getWorkingSet() {
        return asScriptSet(new String[]{
            "command_line_quotes.js",}
        );
    }
	
	private static final Set<Script> getCatastrophicSet() {
        return asScriptSet(new String[]{
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
            "tsv1.js",}
        );
    }
	
	private static Set<Script> getFalsePositiveSet(){
        return asScriptSet(new String[] {
			"exportimport_bigarray.js",}
        );
    }
	
	private static Set<Script> getNotImplementedSet(){
        return asScriptSet(new String[] {
			"dumprestore9.js",
			"exportimport_minkey_maxkey.js",}
        );
    }
	
	private static Set<Script> getIgnoredSet(){
        return asScriptSet(new String[] {
			"csvexport2.js",}
        );
    }
}