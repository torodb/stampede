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
import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class ImmutableMetaCollection implements MetaCollection {

  private final String name;
  private final String identifier;
  private final Map<TableRef, ImmutableMetaDocPart> docPartsByTableRef;
  private final Map<String, ImmutableMetaDocPart> docPartsByIdentifier;
  private final Map<String, ImmutableMetaIndex> indexesByName;

  public ImmutableMetaCollection(String colName, String colId,
      Iterable<ImmutableMetaDocPart> docParts, Iterable<ImmutableMetaIndex> indexes) {
    this.name = colName;
    this.identifier = colId;

    HashMap<TableRef, ImmutableMetaDocPart> byTableRef = new HashMap<>();
    HashMap<String, ImmutableMetaDocPart> byDbName = new HashMap<>();
    for (ImmutableMetaDocPart table : docParts) {
      byTableRef.put(table.getTableRef(), table);
      byDbName.put(table.getIdentifier(), table);
    }
    this.docPartsByTableRef = Collections.unmodifiableMap(byTableRef);
    this.docPartsByIdentifier = Collections.unmodifiableMap(byDbName);

    HashMap<String, ImmutableMetaIndex> indexesByName = new HashMap<>();
    for (ImmutableMetaIndex index : indexes) {
      indexesByName.put(index.getName(), index);
    }
    this.indexesByName = Collections.unmodifiableMap(indexesByName);
  }

  public ImmutableMetaCollection(String colName, String colId,
      @DoNotChange Map<String, ImmutableMetaDocPart> docPartsById,
      Map<String, ImmutableMetaIndex> indexesByName) {
    this.name = colName;
    this.identifier = colId;

    this.docPartsByIdentifier = docPartsById;
    this.docPartsByTableRef = new HashMap<>();
    for (ImmutableMetaDocPart table : docPartsById.values()) {
      docPartsByTableRef.put(table.getTableRef(), table);
    }

    this.indexesByName = indexesByName;
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
  public Stream<ImmutableMetaDocPart> streamContainedMetaDocParts() {
    return docPartsByIdentifier.values().stream();
  }

  @Override
  public ImmutableMetaDocPart getMetaDocPartByIdentifier(String tableDbName) {
    return docPartsByIdentifier.get(tableDbName);
  }

  @Override
  public ImmutableMetaDocPart getMetaDocPartByTableRef(TableRef tableRef) {
    return docPartsByTableRef.get(tableRef);
  }

  @Override
  public Stream<ImmutableMetaIndex> streamContainedMetaIndexes() {
    return indexesByName.values().stream();
  }

  @Override
  public ImmutableMetaIndex getMetaIndexByName(String indexName) {
    return indexesByName.get(indexName);
  }

  @Override
  public List<Tuple2<MetaIndex, List<String>>> getMissingIndexesForNewField(
      MutableMetaDocPart docPart, MetaField newField) {
    return getMissingIndexesForNewField(streamContainedMetaIndexes(), docPart, newField);
  }

  protected List<Tuple2<MetaIndex, List<String>>> getMissingIndexesForNewField(
      Stream<? extends MetaIndex> containedMetaIndexes,
      MutableMetaDocPart docPart, MetaField newField) {
    return containedMetaIndexes
        .filter(index -> index.getMetaIndexFieldByTableRefAndName(docPart.getTableRef(), newField
            .getName()) != null)
        .flatMap(index -> Seq.seq(index.iteratorMetaDocPartIndexesIdentifiers(docPart))
            .filter(identifiers -> identifiers.contains(newField.getIdentifier()))
            .map(identifiers -> new Tuple2<MetaIndex, List<String>>(index, identifiers)))
        .collect(Collectors.groupingBy(missingIndexEntry -> missingIndexEntry.v2()))
        .entrySet()
        .stream()
        .map(groupedMissingIndexEntries -> groupedMissingIndexEntries.getValue().get(0))
        .collect(Collectors.toList());
  }

  public static class Builder {

    private boolean built = false;
    private final String name;
    private final String identifier;
    private final Map<String, ImmutableMetaDocPart> docPartsByIdentifier;
    private final Map<String, ImmutableMetaIndex> indexesByName;

    public Builder(String name, String identifier) {
      this.name = name;
      this.identifier = identifier;
      this.docPartsByIdentifier = new HashMap<>();
      this.indexesByName = new HashMap<>();
    }

    public Builder(String name, String identifier, int expectedDocParts, int expectedIndexes) {
      this.name = name;
      this.identifier = identifier;
      this.docPartsByIdentifier = new HashMap<>(expectedDocParts);
      this.indexesByName = new HashMap<>(expectedIndexes);
    }

    public Builder(ImmutableMetaCollection other) {
      this.name = other.getName();
      this.identifier = other.getIdentifier();

      docPartsByIdentifier = new HashMap<>(other.docPartsByIdentifier);
      this.indexesByName = new HashMap<>(other.indexesByName);
    }

    public Builder put(ImmutableMetaDocPart.Builder tableBuilder) {
      return put(tableBuilder.build());
    }

    public Builder put(ImmutableMetaDocPart table) {
      Preconditions.checkState(!built, "This builder has already been built");
      docPartsByIdentifier.put(table.getIdentifier(), table);
      return this;
    }

    public Builder put(ImmutableMetaIndex.Builder indexBuilder) {
      return put(indexBuilder.build());
    }

    public Builder put(ImmutableMetaIndex index) {
      Preconditions.checkState(!built, "This builder has already been built");
      indexesByName.put(index.getName(), index);
      return this;
    }

    public Builder remove(MetaIndex index) {
      Preconditions.checkState(!built, "This builder has already been built");
      indexesByName.remove(index.getName());
      return this;
    }

    public ImmutableMetaCollection build() {
      Preconditions.checkState(!built, "This builder has already been built");
      Preconditions.checkState(
          docPartsByIdentifier.values().isEmpty() || docPartsByIdentifier.values().stream()
          .anyMatch((dp) -> dp.getTableRef().isRoot()),
          "Tryng to create a MetaCollection without a root doc part"
      );
      built = true;
      return new ImmutableMetaCollection(name, identifier, docPartsByIdentifier, indexesByName);
    }
  }

  @Override
  public String toString() {
    return defautToString();
  }
}
