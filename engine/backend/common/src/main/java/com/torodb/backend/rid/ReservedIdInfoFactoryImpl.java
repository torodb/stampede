/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.rid;

import com.google.common.base.Preconditions;
import com.torodb.backend.SqlInterface;
import com.torodb.core.TableRef;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ReservedIdInfoFactoryImpl extends IdleTorodbService implements ReservedIdInfoFactory {

  private final MetainfoRepository metainfoRepository;
  private final SqlInterface sqlInterface;
  @SuppressWarnings("checkstyle:LineLength")
  private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>>> megaMap;

  @Inject
  public ReservedIdInfoFactoryImpl(@TorodbIdleService ThreadFactory threadFactory,
      MetainfoRepository metainfoRepository, SqlInterface sqlInterface) {
    super(threadFactory);
    this.metainfoRepository = metainfoRepository;
    this.sqlInterface = sqlInterface;
  }

  @Override
  protected void startUp() throws Exception {
    ImmutableMetaSnapshot snapshot;
    try (SnapshotStage snapshotStage = metainfoRepository.startSnapshotStage()) {
      snapshot = snapshotStage.createImmutableSnapshot();
    }

    try (Connection connection = sqlInterface.getDbBackend().createSystemConnection()) {
      DSLContext dsl = sqlInterface.getDslContextFactory().createDslContext(connection);

      megaMap = loadRowIds(dsl, snapshot);
    }

  }

  @Override
  protected void shutDown() throws Exception {
    megaMap.clear();
  }

  @SuppressWarnings("checkstyle:LineLength")
  private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>>> loadRowIds(
      DSLContext dsl, MetaSnapshot snapshot) {
    ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>>> rowsIdMap =
        new ConcurrentHashMap<>();

    snapshot.streamMetaDatabases().forEach(db -> {
      ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>> collMap =
          new ConcurrentHashMap<>();
      rowsIdMap.put(db.getName(), collMap);
      db.streamMetaCollections().forEach(collection -> {
        ConcurrentHashMap<TableRef, ReservedIdInfo> tableRefMap = new ConcurrentHashMap<>();
        collMap.put(collection.getName(), tableRefMap);
        collection.streamContainedMetaDocParts().forEach(metaDocPart -> {
          TableRef tableRef = metaDocPart.getTableRef();
          Integer lastRowIUsed = sqlInterface.getReadInterface().getLastRowIdUsed(dsl, db,
              collection, metaDocPart);
          tableRefMap.put(tableRef, new ReservedIdInfo(lastRowIUsed, lastRowIUsed));
        });
      });
    });
    return rowsIdMap;
  }

  @Override
  public ReservedIdInfo create(String dbName, String collectionName, TableRef tableRef) {
    Preconditions.checkState(isRunning(), "This " + ReservedIdInfoFactory.class
        + " is also a service and it is not running");

    assert megaMap != null;

    ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>> collectionsMap =
        this.megaMap.computeIfAbsent(dbName,
            name -> new ConcurrentHashMap<>());
    ConcurrentHashMap<TableRef, ReservedIdInfo> docPartsMap = collectionsMap.computeIfAbsent(
        collectionName,
        name -> new ConcurrentHashMap<>());
    return docPartsMap.computeIfAbsent(tableRef, tr -> new ReservedIdInfo(-1, -1));
  }

}
