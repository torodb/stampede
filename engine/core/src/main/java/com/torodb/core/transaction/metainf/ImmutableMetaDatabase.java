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

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class ImmutableMetaDatabase implements MetaDatabase {

  private final String name;
  private final String identifier;
  private final Map<String, ImmutableMetaCollection> collectionsById;
  private final Map<String, ImmutableMetaCollection> collectionsByName;

  public ImmutableMetaDatabase(String name, String identifier,
      Iterable<ImmutableMetaCollection> collections) {
    this.name = name;
    this.identifier = identifier;

    collectionsById = new HashMap<>();
    collectionsByName = new HashMap<>();

    for (ImmutableMetaCollection collection : collections) {
      collectionsById.put(collection.getIdentifier(), collection);
      collectionsByName.put(collection.getName(), collection);
    }
  }

  public ImmutableMetaDatabase(String name, String identifier,
      Map<String, ImmutableMetaCollection> collectionsById) {
    this.name = name;
    this.identifier = identifier;
    this.collectionsById = collectionsById;
    this.collectionsByName = new HashMap<>(collectionsById.size());
    for (ImmutableMetaCollection collection : collectionsById.values()) {
      collectionsByName.put(collection.getName(), collection);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public Stream<ImmutableMetaCollection> streamMetaCollections() {
    return collectionsById.values().stream();
  }

  @Override
  public ImmutableMetaCollection getMetaCollectionByName(String collectionName) {
    return collectionsByName.get(collectionName);
  }

  @Override
  public ImmutableMetaCollection getMetaCollectionByIdentifier(String collectionIdentifier) {
    return collectionsById.get(collectionIdentifier);
  }

  @Override
  public String toString() {
    return defautToString();
  }

  public static class Builder {

    private boolean built = false;
    private final String name;
    private final String identifier;
    private final Map<String, ImmutableMetaCollection> collectionsById;

    public Builder(String name, String identifier) {
      this.name = name;
      this.identifier = identifier;
      collectionsById = new HashMap<>();
    }

    public Builder(String name, String identifier, int expectedCollections) {
      this.name = name;
      this.identifier = identifier;
      collectionsById = new HashMap<>(expectedCollections);
    }

    public Builder(ImmutableMetaDatabase other) {
      this.name = other.name;
      this.identifier = other.identifier;
      this.collectionsById = new HashMap<>(other.collectionsById);
    }

    public Builder put(ImmutableMetaCollection collection) {
      Preconditions.checkState(!built, "This builder has already been built");
      collectionsById.put(collection.getIdentifier(), collection);
      return this;
    }

    public Builder put(ImmutableMetaCollection.Builder collectionBuilder) {
      return Builder.this.put(collectionBuilder.build());
    }

    public Builder remove(MetaCollection metaCol) {
      Preconditions.checkState(!built, "This builder has already been built");
      collectionsById.remove(metaCol.getIdentifier());
      return this;
    }

    public ImmutableMetaDatabase build() {
      Preconditions.checkState(!built, "This builder has already been built");
      built = true;
      return new ImmutableMetaDatabase(name, identifier, collectionsById);
    }
  }

}
