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

package com.torodb.torod.db.backends.query;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.backends.meta.StructuresCache;

import java.util.Collection;

/**
 *
 */
public class DidsByStructureQueryEvaluatorCollector implements QueryEvaluatorCollector{

    private final StructuresCache structuresCache;
    private final Multimap<DocStructure, Integer> didsByStructure = MultimapBuilder.hashKeys().hashSetValues().build();

    public DidsByStructureQueryEvaluatorCollector(StructuresCache structuresCache) {
        this.structuresCache = structuresCache;
    }
    
    @Override
    public void addAll(Integer sid, Collection<Integer> dids) {
        didsByStructure.putAll(structuresCache.getAllStructures().get(sid), dids);
    }

    public Multimap<DocStructure, Integer> getDidsByStructure() {
        return didsByStructure;
    }

    @Override
    public String toString() {
        return didsByStructure.toString();
    }
    
}
