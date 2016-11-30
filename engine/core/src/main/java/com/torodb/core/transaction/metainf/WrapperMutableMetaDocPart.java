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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart.Builder;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaDocPart implements MutableMetaDocPart {

  private final ImmutableMetaDocPart wrapped;
  /**
   * This table contains all fields contained by wrapper and all new fields
   */
  private final Table<String, FieldType, ImmutableMetaField> newFields;
  /**
   * This list just contains the fields that have been added on this wrapper but not on the wrapped
   * object.
   */
  private final List<ImmutableMetaField> addedFields;
  private final Map<String, ImmutableMetaField> addedFieldsByIndetifiers;
  private final Consumer<WrapperMutableMetaDocPart> changeConsumer;
  private final EnumMap<FieldType, ImmutableMetaScalar> newScalars;
  @SuppressWarnings("checkstyle:LineLength")
  private final HashMap<String, Tuple2<ImmutableMetaIdentifiedDocPartIndex, MetaElementState>> indexesByIdentifier;
  @SuppressWarnings("checkstyle:LineLength")
  private final Map<String, Tuple2<ImmutableMetaIdentifiedDocPartIndex, MetaElementState>> aliveIndexesMap;
  private final List<MutableMetaDocPartIndex> addedMutableIndexes;

  public WrapperMutableMetaDocPart(ImmutableMetaDocPart wrapped,
      Consumer<WrapperMutableMetaDocPart> changeConsumer) {
    this.wrapped = wrapped;

    newFields = HashBasedTable.create();

    wrapped.streamFields().forEach((field) ->
        newFields.put(field.getName(), field.getType(), field)
    );
    addedFields = new ArrayList<>();
    addedFieldsByIndetifiers = new HashMap<>();
    this.changeConsumer = changeConsumer;
    this.newScalars = new EnumMap<>(FieldType.class);
    indexesByIdentifier = new HashMap<>();
    wrapped.streamIndexes().forEach((docPartIndexindex) -> {
      indexesByIdentifier.put(docPartIndexindex.getIdentifier(), new Tuple2<>(docPartIndexindex,
          MetaElementState.NOT_CHANGED));
    });
    aliveIndexesMap = Maps.filterValues(indexesByIdentifier, tuple -> tuple.v2().isAlive());
    addedMutableIndexes = new ArrayList<>();
  }

  @Override
  public ImmutableMetaField addMetaField(String name, String identifier, FieldType type) throws
      IllegalArgumentException {
    if (getMetaFieldByNameAndType(name, type) != null) {
      throw new IllegalArgumentException("There is another field with the name " + name
          + " whose type is " + type);
    }

    assert getMetaFieldByIdentifier(identifier) == null :
        "There is another field with the identifier " + identifier;

    ImmutableMetaField newField = new ImmutableMetaField(name, identifier, type);
    newFields.put(name, type, newField);
    addedFields.add(newField);
    addedFieldsByIndetifiers.put(newField.getIdentifier(), newField);
    changeConsumer.accept(this);
    return newField;
  }

  @Override
  public ImmutableMetaScalar addMetaScalar(String identifier, FieldType type) throws
      IllegalArgumentException {
    if (getScalar(type) != null) {
      throw new IllegalArgumentException("There is another scalar with type " + type + ", "
          + "whose identifier is " + identifier);
    }
    ImmutableMetaScalar scalar = new ImmutableMetaScalar(identifier, type);
    newScalars.put(type, scalar);
    changeConsumer.accept(this);
    return scalar;
  }

  @Override
  @DoNotChange
  public Iterable<ImmutableMetaField> getAddedMetaFields() {
    return addedFields;
  }

  @Override
  public ImmutableMetaField getAddedFieldByIdentifier(String identifier) {
    return addedFieldsByIndetifiers.get(identifier);
  }

  @Override
  public Iterable<? extends ImmutableMetaScalar> getAddedMetaScalars() {
    return newScalars.values();
  }

  @Override
  public MutableMetaDocPartIndex addMetaDocPartIndex(boolean unique) {
    MutableMetaDocPartIndex newIndex = new WrapperMutableMetaDocPartIndex(unique,
        this::onDocPartIndexChange);
    addedMutableIndexes.add(newIndex);
    return newIndex;
  }

  @Override
  public boolean removeMetaDocPartIndexByIdentifier(String indexId) {
    ImmutableMetaIdentifiedDocPartIndex metaDocPartIndex = getMetaDocPartIndexByIdentifier(indexId);
    if (metaDocPartIndex == null) {
      return false;
    }

    indexesByIdentifier.put(metaDocPartIndex.getIdentifier(), new Tuple2<>(metaDocPartIndex,
        MetaElementState.REMOVED));
    changeConsumer.accept(this);
    return true;
  }

  @Override
  @DoNotChange
  @SuppressWarnings("checkstyle:LineLength")
  public Iterable<Tuple2<ImmutableMetaIdentifiedDocPartIndex, MetaElementState>> getModifiedMetaDocPartIndexes() {
    return Maps.filterValues(indexesByIdentifier, tuple -> tuple.v2().hasChanged())
        .values();
  }

  @Override
  @DoNotChange
  public Iterable<MutableMetaDocPartIndex> getAddedMutableMetaDocPartIndexes() {
    return addedMutableIndexes;
  }

  @Override
  public ImmutableMetaDocPart immutableCopy() {
    if (addedFields.isEmpty() && newScalars.isEmpty() && indexesByIdentifier.values().stream()
        .noneMatch(tuple -> tuple.v2().hasChanged())) {
      return wrapped;
    } else {
      ImmutableMetaDocPart.Builder builder = new Builder(wrapped);
      for (ImmutableMetaField addedField : addedFields) {
        builder.put(addedField);
      }
      for (ImmutableMetaScalar value : newScalars.values()) {
        builder.put(value);
      }

      indexesByIdentifier.values()
          .forEach(tuple -> {
            switch (tuple.v2()) {
              case ADDED:
              case MODIFIED:
              case NOT_CHANGED:
                builder.put(tuple.v1());
                break;
              case REMOVED:
                builder.remove(tuple.v1());
                break;
              case NOT_EXISTENT:
              default:
                throw new AssertionError("Unexpected case " + tuple.v2());
            }
          });
      return builder.build();
    }
  }

  @Override
  public TableRef getTableRef() {
    return wrapped.getTableRef();
  }

  @Override
  public String getIdentifier() {
    return wrapped.getIdentifier();
  }

  @Override
  public Stream<? extends ImmutableMetaField> streamFields() {
    return newFields.values().stream();
  }

  @Override
  public Stream<? extends ImmutableMetaField> streamMetaFieldByName(String columnName) {
    return newFields.row(columnName).values().stream();
  }

  @Override
  public ImmutableMetaField getMetaFieldByNameAndType(String fieldName, FieldType type) {
    return newFields.get(fieldName, type);
  }

  @Override
  public ImmutableMetaField getMetaFieldByIdentifier(String fieldId) {
    return newFields.values().stream()
        .filter((field) -> field.getIdentifier().equals(fieldId))
        .findAny()
        .orElse(null);
  }

  @Override
  public Stream<? extends MetaScalar> streamScalars() {
    return Stream.concat(newScalars.values().stream(), wrapped.streamScalars());
  }

  @Override
  public MetaScalar getScalar(FieldType type) {
    ImmutableMetaScalar scalar = newScalars.get(type);
    if (scalar != null) {
      return scalar;
    }
    return wrapped.getScalar(type);
  }

  @Override
  public Stream<? extends ImmutableMetaIdentifiedDocPartIndex> streamIndexes() {
    return aliveIndexesMap.values().stream().map(tuple -> tuple.v1());
  }

  @Override
  public ImmutableMetaIdentifiedDocPartIndex getMetaDocPartIndexByIdentifier(String indexId) {
    Tuple2<ImmutableMetaIdentifiedDocPartIndex, MetaElementState> tuple = aliveIndexesMap.get(
        indexId);
    if (tuple == null) {
      return null;
    }
    return tuple.v1();
  }

  @Override
  public MutableMetaDocPartIndex getOrCreatePartialMutableDocPartIndexForMissingIndexAndNewField(
      MetaIndex missingIndex,
      List<String> identifiers, MetaField newField) {
    int position = identifiers.indexOf(newField.getIdentifier());
    Optional<MutableMetaDocPartIndex> matchingMutableDocPartIndex = Seq.seq(
        getAddedMutableMetaDocPartIndexes())
        .filter(docPartIndex -> docPartIndex.getMetaDocPartIndexColumnByPosition(position) == null
            && identifiers.size() >= docPartIndex.size()
            && missingIndex.isSubMatch(this, identifiers, docPartIndex)
            // We ensure we do not pick a doc part index that fit a isSubMatch for our index but
            // was the only chance for another combination. For example:
            // 1. a_i, b_i, c_i are old fields
            // 2. a_s, b_s, c_s are new fields
            // 3. we have index a asc, b asc, c asc
            // 4. we added doc part index a_s asc, null, null and a_s asc, b_i asc, null
            // 5. we search for a sub match for a_s, b_i, c_s and found a_s asc, null, null
            && noneNonCurrentAndNullIndexColumnIsNew(position, docPartIndex, identifiers))
        .findAny();
    MutableMetaDocPartIndex docPartIndex;
    if (matchingMutableDocPartIndex.isPresent()) {
      docPartIndex = matchingMutableDocPartIndex.get();
    } else {
      docPartIndex = addMetaDocPartIndex(missingIndex.isUnique());
      int index = 0;
      for (String identifier : identifiers) {
        if (getAddedFieldByIdentifier(identifier) == null) {
          MetaIndexField indexField = missingIndex.getMetaIndexFieldByTableRefAndPosition(
              getTableRef(), index);
          docPartIndex.putMetaDocPartIndexColumn(index, identifier, indexField.getOrdering());
        }
        index++;
      }
    }
    MetaIndexField indexField = missingIndex.getMetaIndexFieldByTableRefAndPosition(getTableRef(),
        position);
    docPartIndex.putMetaDocPartIndexColumn(position, newField.getIdentifier(),
        indexField.getOrdering());
    return docPartIndex;
  }

  private boolean noneNonCurrentAndNullIndexColumnIsNew(int position,
      MutableMetaDocPartIndex docPartIndex,
      List<String> identifiers) {
    return IntStream.range(0, identifiers.size())
        .noneMatch(index -> index != position && docPartIndex.getMetaDocPartIndexColumnByPosition(
            index) == null && getAddedFieldByIdentifier(identifiers.get(index)) == null);
  }

  private boolean isTransitionAllowed(MetaIdentifiedDocPartIndex metaDocPartIndexIndex,
      MetaElementState newState) {
    MetaElementState oldState;
    Tuple2<ImmutableMetaIdentifiedDocPartIndex, MetaElementState> tuple = indexesByIdentifier.get(
        metaDocPartIndexIndex.getIdentifier());

    if (tuple == null) {
      oldState = MetaElementState.NOT_EXISTENT;
    } else {
      oldState = tuple.v2();
    }

    oldState.assertLegalTransition(newState);
    return true;
  }

  protected void onDocPartIndexChange(WrapperMutableMetaDocPartIndex changedIndex,
      ImmutableMetaIdentifiedDocPartIndex immutableIndex) {
    assert isTransitionAllowed(immutableIndex, MetaElementState.ADDED);

    if (getMetaDocPartIndexByIdentifier(immutableIndex.getIdentifier()) != null) {
      throw new IllegalArgumentException("There is another index with the identifier "
          + immutableIndex.getIdentifier());
    }

    addedMutableIndexes.remove(changedIndex);
    indexesByIdentifier.put(immutableIndex.getIdentifier(), new Tuple2<>(immutableIndex,
        MetaElementState.ADDED));
    changeConsumer.accept(this);
  }

  @Override
  public String toString() {
    return defautToString();
  }

}
