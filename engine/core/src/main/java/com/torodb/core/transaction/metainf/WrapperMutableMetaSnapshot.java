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

package com.torodb.core.transaction.metainf;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaSnapshot implements MutableMetaSnapshot {

  private static final Logger LOGGER = LogManager.getLogger(WrapperMutableMetaSnapshot.class);
  private final ImmutableMetaSnapshot wrapped;
  private final HashMap<String, Tuple2<MutableMetaDatabase, MetaElementState>> dbsByName;
  private final Map<String, Tuple2<MutableMetaDatabase, MetaElementState>> aliveMap;

  public WrapperMutableMetaSnapshot(ImmutableMetaSnapshot wrapped) {
    this.wrapped = wrapped;
    this.dbsByName = new HashMap<>();

    wrapped.streamMetaDatabases().forEach((db) -> {
      WrapperMutableMetaDatabase mutable = createMetaDatabase(db);
      dbsByName.put(db.getName(), new Tuple2<>(mutable, MetaElementState.NOT_CHANGED));
    });
    aliveMap = Maps.filterValues(dbsByName, tuple -> tuple.v2().isAlive());
  }

  protected WrapperMutableMetaDatabase createMetaDatabase(ImmutableMetaDatabase immutable) {
    return new WrapperMutableMetaDatabase(immutable, this::onMetaDatabaseChange);
  }

  @Override
  public MutableMetaDatabase addMetaDatabase(String dbName, String dbId) throws
      IllegalArgumentException {
    if (getMetaDatabaseByName(dbName) != null) {
      throw new IllegalArgumentException("There is another database whose name is " + dbName);
    }

    assert getMetaDatabaseByIdentifier(dbId) == null : "There is another database whose id is "
        + dbId;

    WrapperMutableMetaDatabase result = createMetaDatabase(
        new ImmutableMetaDatabase(dbName, dbId, Collections.emptyList())
    );

    dbsByName.put(dbName, new Tuple2<>(result, MetaElementState.ADDED));

    return result;
  }

  @Override
  public Iterable<Tuple2<MutableMetaDatabase, MetaElementState>> getModifiedDatabases() {
    return Maps.filterValues(dbsByName, tuple -> tuple.v2().hasChanged())
        .values();
  }

  @Override
  public boolean hasChanged() {
    return dbsByName.values().stream().anyMatch(tuple -> tuple.v2.hasChanged());
  }

  @Override
  public ImmutableMetaSnapshot immutableCopy() {
    if (dbsByName.values().stream().noneMatch(tuple -> tuple.v2().hasChanged())) {
      return wrapped;
    } else {
      ImmutableMetaSnapshot.Builder builder = new ImmutableMetaSnapshot.Builder(wrapped);

      dbsByName.values()
          .forEach(tuple -> {
            switch (tuple.v2()) {
              case ADDED:
              case MODIFIED:
              case NOT_CHANGED:
                builder.put(tuple.v1().immutableCopy());
                break;
              case REMOVED:
                builder.remove(tuple.v1());
                break;
              case NOT_EXISTENT:
              default:
                throw new AssertionError("Unexpected case" + tuple.v2());
            }
          });
      return builder.build();
    }
  }

  @Override
  public Stream<? extends WrapperMutableMetaDatabase> streamMetaDatabases() {
    return aliveMap.values().stream().map(tuple -> (WrapperMutableMetaDatabase) tuple.v1());
  }

  @Override
  public WrapperMutableMetaDatabase getMetaDatabaseByName(String dbName) {
    Tuple2<MutableMetaDatabase, MetaElementState> tuple = aliveMap.get(dbName);
    if (tuple == null) {
      return null;
    }
    return (WrapperMutableMetaDatabase) tuple.v1();
  }

  @Override
  public WrapperMutableMetaDatabase getMetaDatabaseByIdentifier(String dbIdentifier) {
    LOGGER.debug("Looking for a meta collection by the unindexed attribute 'id'. "
        + "Performance lost is expected");
    return aliveMap.values().stream()
        .map(tuple -> (WrapperMutableMetaDatabase) tuple.v1())
        .filter((collection) -> collection.getIdentifier().equals(dbIdentifier))
        .findAny()
        .orElse(null);
  }

  @Override
  public boolean removeMetaDatabaseByName(String dbName) {
    WrapperMutableMetaDatabase metaDb = getMetaDatabaseByName(dbName);
    if (metaDb == null) {
      return false;
    }
    removeMetaDatabase(metaDb);
    return true;
  }

  @Override
  public boolean removeMetaDatabaseByIdentifier(String dbId) {
    WrapperMutableMetaDatabase metaDb = getMetaDatabaseByIdentifier(dbId);
    if (metaDb == null) {
      return false;
    }
    removeMetaDatabase(metaDb);
    return true;
  }

  private void removeMetaDatabase(WrapperMutableMetaDatabase metaCol) {
    dbsByName.put(metaCol.getName(), new Tuple2<>(metaCol, MetaElementState.REMOVED));
  }

  private boolean isTransitionAllowed(MetaDatabase metaDb, MetaElementState newState) {
    MetaElementState oldState;
    Tuple2<MutableMetaDatabase, MetaElementState> tuple = dbsByName.get(metaDb.getName());

    if (tuple == null) {
      oldState = MetaElementState.NOT_EXISTENT;
    } else {
      oldState = tuple.v2();
    }

    oldState.assertLegalTransition(newState);
    return true;
  }

  private void onMetaDatabaseChange(WrapperMutableMetaDatabase changed) {
    assert isTransitionAllowed(changed, MetaElementState.MODIFIED);

    dbsByName.put(changed.getName(), new Tuple2<>(changed, MetaElementState.MODIFIED));
  }
}
