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

import com.torodb.integration.IntegrationTestEnvironment;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.torodb.integration.Backend;
import com.torodb.integration.Protocol;
import com.torodb.integration.TestCategory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 *
 */
public class ScriptClassifier {

    private final Map<IntegrationTestEnvironment, Multimap<TestCategory, Script>> scriptMap;

    public ScriptClassifier(Map<IntegrationTestEnvironment, Multimap<TestCategory, Script>> scriptMap) {
        this.scriptMap = scriptMap;
    }

    @Nonnull
    public Multimap<TestCategory, Script> getScriptFor(IntegrationTestEnvironment env) {
        Multimap<TestCategory, Script> result = scriptMap.get(env);
        if (result == null) {
            return ImmutableSetMultimap.of();
        }
        return result;
    }

    public static class Builder {
        private final Map<IntegrationTestEnvironment, Multimap<TestCategory, Script>> scriptMap;

        public Builder() {
            scriptMap = new HashMap<>(20);
        }

        public Builder addScripts(
                Protocol protocol,
                Backend backend,
                TestCategory testCategory,
                Collection<? extends Script> scripts) {
            IntegrationTestEnvironment ite = new IntegrationTestEnvironment(protocol, backend);
            
            return addScripts(ite, testCategory, scripts);
        }

        public Builder addScripts(IntegrationTestEnvironment ite, TestCategory testCategory, Collection<? extends Script> scripts) {
            Multimap<TestCategory, Script> multimap = scriptMap.get(ite);
            if (multimap == null) {
                multimap = HashMultimap.create(TestCategory.values().length, 200);
                scriptMap.put(ite, multimap);
            }

            for (Script script : scripts) {
                Preconditions.checkArgument(script != null, "There is a null script on %s", scripts);
                multimap.put(testCategory, script);
            }
            return this;
        }

        public ScriptClassifier build() {
            return new ScriptClassifier(scriptMap);
        }
    }

}
