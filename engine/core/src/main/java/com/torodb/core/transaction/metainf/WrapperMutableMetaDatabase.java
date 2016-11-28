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
import com.torodb.core.annotations.DoNotChange;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaDatabase implements MutableMetaDatabase {

  private static final Logger LOGGER = LogManager.getLogger(WrapperMutableMetaDatabase.class);
  private final ImmutableMetaDatabase wrapped;
  private final HashMap<String, Tuple2<MutableMetaCollection, MetaElementState>> collectionsByName;
  private final Consumer<WrapperMutableMetaDatabase> changeConsumer;
  private final Map<String, Tuple2<MutableMetaCollection, MetaElementState>> aliveCollectionsMap;

  public WrapperMutableMetaDatabase(ImmutableMetaDatabase wrapped,
      Consumer<WrapperMutableMetaDatabase> changeConsumer) {
    this.wrapped = wrapped;
    this.changeConsumer = changeConsumer;

    this.collectionsByName = new HashMap<>();

    wrapped.streamMetaCollections().forEach((collection) -> {
      WrapperMutableMetaCollection mutable = createMetaColletion(collection);

      collectionsByName.put(collection.getName(),
          new Tuple2<>(mutable, MetaElementState.NOT_CHANGED));
    });
    aliveCollectionsMap = Maps.filterValues(collectionsByName, tuple -> tuple.v2().isAlive());
  }

  protected WrapperMutableMetaCollection createMetaColletion(ImmutableMetaCollection immutable) {
    return new WrapperMutableMetaCollection(immutable, this::onMetaCollectionChange);
  }

  @Override
  public WrapperMutableMetaCollection addMetaCollection(String colName, String colId) throws
      IllegalArgumentException {
    if (getMetaCollectionByName(colName) != null) {
      throw new IllegalArgumentException("There is another collection whose name is " + colName);
    }

    assert getMetaCollectionByIdentifier(colId) == null : "There is another collection whose id is "
        + colId;

    WrapperMutableMetaCollection result = createMetaColletion(new ImmutableMetaCollection(
        colName,
        colId,
        Collections.emptyMap(),
        Collections.emptyMap())
    );

    collectionsByName.put(colName, new Tuple2<>(result, MetaElementState.ADDED));
    changeConsumer.accept(this);

    return result;
  }

  @DoNotChange
  @Override
  public Iterable<Tuple2<MutableMetaCollection, MetaElementState>> getModifiedCollections() {
    return Maps.filterValues(collectionsByName, tuple -> tuple.v2().hasChanged())
        .values();
  }

  @Override
  public ImmutableMetaDatabase immutableCopy() {
    if (collectionsByName.values().stream().noneMatch(tuple -> tuple.v2().hasChanged())) {
      return wrapped;
    } else {
      ImmutableMetaDatabase.Builder builder = new ImmutableMetaDatabase.Builder(wrapped);

      collectionsByName.values()
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
  public String getName() {
    return wrapped.getName();
  }

  @Override
  public String getIdentifier() {
    return wrapped.getIdentifier();
  }

  @Override
  public Stream<? extends WrapperMutableMetaCollection> streamMetaCollections() {
    return aliveCollectionsMap.values().stream().map(tuple -> (WrapperMutableMetaCollection) tuple
        .v1());
  }

  @Override
  public WrapperMutableMetaCollection getMetaCollectionByName(String collectionName) {
    Tuple2<MutableMetaCollection, MetaElementState> tuple = aliveCollectionsMap.get(collectionName);
    if (tuple == null) {
      return null;
    }
    return (WrapperMutableMetaCollection) tuple.v1();
  }

  @Override
  public WrapperMutableMetaCollection getMetaCollectionByIdentifier(String collectionIdentifier) {
    LOGGER.debug("Looking for a meta collection by the unindexed attribute 'id'. "
        + "Performance lost is expected");
    return aliveCollectionsMap.values().stream()
        .map(tuple -> (WrapperMutableMetaCollection) tuple.v1())
        .filter((collection) -> collection.getIdentifier().equals(collectionIdentifier))
        .findAny()
        .orElse(null);
  }

  @Override
  public boolean removeMetaCollectionByName(String collectionName) {
    WrapperMutableMetaCollection metaCol = getMetaCollectionByName(collectionName);
    if (metaCol == null) {
      return false;
    }
    removeMetaCollection(metaCol);
    return true;
  }

  @Override
  public boolean removeMetaCollectionByIdentifier(String collectionId) {
    WrapperMutableMetaCollection metaCol = getMetaCollectionByIdentifier(collectionId);
    if (metaCol == null) {
      return false;
    }
    removeMetaCollection(metaCol);
    return true;
  }

  private void removeMetaCollection(WrapperMutableMetaCollection metaCol) {
    collectionsByName.put(metaCol.getName(), new Tuple2<>(metaCol, MetaElementState.REMOVED));
    changeConsumer.accept(this);
  }

  @Override
  public String toString() {
    return defautToString();
  }

  private boolean isTransitionAllowed(MetaCollection metaCol, MetaElementState newState) {
    MetaElementState oldState;
    Tuple2<MutableMetaCollection, MetaElementState> tuple = collectionsByName.get(
        metaCol.getName());

    if (tuple == null) {
      oldState = MetaElementState.NOT_EXISTENT;
    } else {
      oldState = tuple.v2();
    }

    oldState.assertLegalTransition(newState);
    return true;
  }

  private void onMetaCollectionChange(WrapperMutableMetaCollection changed) {
    assert isTransitionAllowed(changed, MetaElementState.MODIFIED);

    collectionsByName.put(changed.getName(), new Tuple2<>(changed, MetaElementState.MODIFIED));
    changeConsumer.accept(this);
  }

}
