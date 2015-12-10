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

package com.torodb.torod.db.wrappers.meta;

import com.torodb.torod.db.wrappers.exceptions.InvalidCollectionSchemaException;
import org.jooq.DSLContext;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 *
 */
public interface TorodbMeta {

    @Nonnull String getDatabaseName();

    boolean exists(String collection);

    IndexStorage.CollectionSchema getCollectionSchema(String collection);

    void dropCollectionSchema(String collection);

    IndexStorage.CollectionSchema createCollectionSchema(
            String colName, String schemaName, DSLContext dsl
    ) throws InvalidCollectionSchemaException;

    Collection<IndexStorage.CollectionSchema> getCollectionSchemes();

}