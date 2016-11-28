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

import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex.Builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaIndex implements MutableMetaIndex {

  private final ImmutableMetaIndex wrapped;
  /**
   * This map contains all fields contained by wrapper and all new fields
   */
  private final Map<TableRef, List<ImmutableMetaIndexField>> newFields;
  /**
   * This list just contains the fields that have been added on this wrapper but not on the wrapped
   * object.
   */
  private final List<ImmutableMetaIndexField> addedFields;
  private final Consumer<WrapperMutableMetaIndex> changeConsumer;

  public WrapperMutableMetaIndex(ImmutableMetaIndex wrapped,
      Consumer<WrapperMutableMetaIndex> changeConsumer) {
    this.wrapped = wrapped;

    newFields = new HashMap<>();

    wrapped.iteratorFields().forEachRemaining((field) ->
        newFields.computeIfAbsent(field.getTableRef(), t -> new ArrayList<>()).add(field)
    );
    addedFields = new ArrayList<>();
    this.changeConsumer = changeConsumer;
  }

  @Override
  public ImmutableMetaIndexField addMetaIndexField(TableRef tableRef, String name,
      FieldIndexOrdering ordering) throws
      IllegalArgumentException {
    if (getMetaIndexFieldByTableRefAndName(tableRef, name) != null) {
      throw new IllegalArgumentException("There is another field with tableRef " + tableRef
          + " whose name is " + name);
    }

    ImmutableMetaIndexField newField = new ImmutableMetaIndexField(
        size(),
        tableRef,
        name,
        ordering);
    newFields.computeIfAbsent(tableRef, t -> new ArrayList<>()).add(newField);
    addedFields.add(newField);
    changeConsumer.accept(this);
    return newField;
  }

  @Override
  @DoNotChange
  public Iterable<ImmutableMetaIndexField> getAddedMetaIndexFields() {
    return addedFields;
  }

  @Override
  public ImmutableMetaIndex immutableCopy() {
    if (addedFields.isEmpty()) {
      return wrapped;
    } else {
      ImmutableMetaIndex.Builder builder = new Builder(wrapped);
      for (ImmutableMetaIndexField addedField : addedFields) {
        builder.add(addedField);
      }
      return builder.build();
    }
  }

  @Override
  public String getName() {
    return wrapped.getName();
  }

  @Override
  public boolean isUnique() {
    return wrapped.isUnique();
  }

  @Override
  public int size() {
    return newFields.values().stream()
        .collect(Collectors.summingInt(l -> l.size()));
  }

  @Override
  public Iterator<? extends ImmutableMetaIndexField> iteratorFields() {
    return newFields.values().stream().flatMap(list -> list.stream()).iterator();
  }

  @Override
  public Iterator<? extends ImmutableMetaIndexField> iteratorMetaIndexFieldByTableRef(
      TableRef tableRef) {
    return newFields.computeIfAbsent(tableRef, t -> new ArrayList<>()).iterator();
  }

  @Override
  public Stream<TableRef> streamTableRefs() {
    return newFields.keySet().stream();
  }

  @Override
  public ImmutableMetaIndexField getMetaIndexFieldByPosition(int position) {
    if (position < wrapped.size()) {
      return wrapped.getMetaIndexFieldByPosition(position);
    }
    return addedFields.get(position - wrapped.size());
  }

  @Override
  public ImmutableMetaIndexField getMetaIndexFieldByTableRefAndName(TableRef tableRef,
      String fieldName) {
    return newFields.computeIfAbsent(tableRef, t -> new ArrayList<>()).stream()
        .filter(f -> f.getName().equals(fieldName))
        .findAny()
        .orElse(null);
  }

  @Override
  public MetaIndexField getMetaIndexFieldByTableRefAndPosition(TableRef tableRef, int position) {
    return newFields.computeIfAbsent(tableRef, t -> new ArrayList<>()).get(position);
  }

  @Override
  public boolean isCompatible(MetaDocPart docPart, MetaDocPartIndex docPartIndex) {
    return wrapped.isCompatible(docPart, docPartIndex,
        iteratorMetaIndexFieldByTableRef(docPart.getTableRef()));
  }

  @Override
  public boolean isCompatible(MetaDocPart docPart) {
    return wrapped.isCompatible(docPart,
        iteratorMetaIndexFieldByTableRef(docPart.getTableRef()));
  }

  @Override
  public boolean isMatch(MetaDocPart docPart, List<String> identifiers,
      MetaDocPartIndex docPartIndex) {
    return wrapped.isMatch(docPart, identifiers, docPartIndex,
        iteratorMetaIndexFieldByTableRef(docPart.getTableRef()), false);
  }

  @Override
  public boolean isMatch(MetaIndex index) {
    return wrapped.isMatch(index,
        iteratorFields());
  }

  @Override
  public boolean isSubMatch(MetaDocPart docPart, List<String> identifiersSublist,
      MetaDocPartIndex docPartIndex) {
    return wrapped.isMatch(docPart, identifiersSublist, docPartIndex,
        iteratorMetaIndexFieldByTableRef(docPart.getTableRef()), true);
  }

  @Override
  public Iterator<List<String>> iteratorMetaDocPartIndexesIdentifiers(MetaDocPart docPart) {
    return wrapped.iteratorMetaDocPartIndexesIdentifiers(docPart,
        iteratorMetaIndexFieldByTableRef(docPart.getTableRef()));
  }

  @Override
  public String toString() {
    return defautToString();
  }

}
