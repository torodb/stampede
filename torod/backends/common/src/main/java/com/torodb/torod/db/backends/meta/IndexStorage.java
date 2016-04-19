/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General PublicSchema License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General PublicSchema License for more details.
 *
 *     You should have received a copy of the GNU Affero General PublicSchema License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */
package com.torodb.torod.db.backends.meta;

import java.io.Serializable;
import java.util.Set;

import org.jooq.DSLContext;

import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.db.backends.sql.index.NamedDbIndex;
import com.torodb.torod.db.backends.sql.index.UnnamedDbIndex;

/**
 *
 */
public interface IndexStorage extends Serializable {
    
    public void initialize(DSLContext dsl, String databaseName, CollectionSchema colSchema);
    
    public Set<NamedDbIndex> getAllDbIndexes(DSLContext dsl);

    public Set<NamedToroIndex> getAllToroIndexes(DSLContext dsl);

    public void dropIndex(DSLContext dsl, NamedDbIndex index);

    public NamedDbIndex createIndex(DSLContext dsl, UnnamedDbIndex unnamedDbIndex);
    
    public void eventToroIndexRemoved(DSLContext dsl, String indexName);

    public void eventToroIndexCreated(DSLContext dsl, NamedToroIndex index);

}
