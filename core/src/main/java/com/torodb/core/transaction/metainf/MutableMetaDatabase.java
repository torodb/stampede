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
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.transaction.metainf;

import com.torodb.core.annotations.DoNotChange;
import java.util.stream.Stream;

/**
 *
 */
public interface MutableMetaDatabase extends MetaDatabase {

    @Override
    public MutableMetaCollection getMetaCollectionByIdentifier(String collectionIdentifier);

    @Override
    public MutableMetaCollection getMetaCollectionByName(String collectionName);

    @Override
    public Stream<? extends MutableMetaCollection> streamMetaCollections();

    public abstract MutableMetaCollection addMetaCollection(String colName, String colId) throws IllegalArgumentException;

    @DoNotChange
    public abstract Iterable<? extends MutableMetaCollection> getModifiedCollections();

    public abstract ImmutableMetaDatabase immutableCopy();

}
