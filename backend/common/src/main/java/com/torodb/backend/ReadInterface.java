/*
 * MongoWP - ToroDB-poc: Backend common
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend;

import com.google.common.collect.Multimap;
import java.sql.SQLException;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple2;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;
import java.util.List;

public interface ReadInterface {

    @Nonnull
    Cursor<Integer> getCollectionDidsWithFieldEqualsTo(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase,
            @Nonnull MetaCollection metaCol, @Nonnull MetaDocPart metaDocPart,
            @Nonnull MetaField metaField, @Nonnull KVValue<?> value)
            throws SQLException;

    @Nonnull
    public Cursor<Integer> getCollectionDidsWithFieldsIn(DSLContext dsl, MetaDatabase metaDatabase,
            MetaCollection metaCol, MetaDocPart metaDocPart, Multimap<MetaField, KVValue<?>> valuesMap)
            throws SQLException;
    
    @Nonnull
    public Cursor<Tuple2<Integer, KVValue<?>>> getCollectionDidsAndProjectionWithFieldsIn(DSLContext dsl, MetaDatabase metaDatabase,
            MetaCollection metaCol, MetaDocPart metaDocPart, Multimap<MetaField, KVValue<?>> valuesMultimap)
            throws SQLException;

    long countAll(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection);

    @Nonnull
    Cursor<Integer> getAllCollectionDids(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection)
            throws SQLException;

    @Nonnull
    List<DocPartResult> getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
            @Nonnull Cursor<Integer> didCursor, int maxSize) throws SQLException;

    @Nonnull
    List<DocPartResult> getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
            @Nonnull Collection<Integer> dids) throws SQLException;

    int getLastRowIdUsed(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull MetaDocPart metaDocPart);
}
