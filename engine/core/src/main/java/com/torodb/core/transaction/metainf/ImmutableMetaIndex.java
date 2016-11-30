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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import org.jooq.lambda.Seq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class ImmutableMetaIndex implements MetaIndex {

  private final String name;
  private final boolean unique;
  private final List<ImmutableMetaIndexField> fieldsByPosition;
  private final Map<TableRef, List<ImmutableMetaIndexField>> fieldsByTableRefAndPosition;
  private final Table<TableRef, String, ImmutableMetaIndexField> fieldsByTableRefAndName;

  public ImmutableMetaIndex(String name, boolean unique) {
    this(name, unique, Collections.emptyList());
  }

  public ImmutableMetaIndex(String name, boolean unique, Iterable<ImmutableMetaIndexField> fields) {
    this.name = name;
    this.unique = unique;

    fieldsByTableRefAndName = HashBasedTable.create();
    fieldsByPosition = new ArrayList<>(fieldsByTableRefAndName.size());
    fieldsByTableRefAndPosition = new HashMap<>(fieldsByTableRefAndName.size());

    for (ImmutableMetaIndexField field : fields) {
      fieldsByPosition.add(field);
      fieldsByTableRefAndPosition.computeIfAbsent(field.getTableRef(), tableRef ->
          new ArrayList<>()).add(field);
      fieldsByTableRefAndName.put(field.getTableRef(), field.getName(), field);
    }
  }

  public ImmutableMetaIndex(String name, boolean unique,
      List<ImmutableMetaIndexField> fieldsByPosition) {
    this.name = name;
    this.unique = unique;
    this.fieldsByPosition = fieldsByPosition;
    this.fieldsByTableRefAndPosition = new HashMap<>();
    this.fieldsByTableRefAndName = HashBasedTable.create();
    for (ImmutableMetaIndexField field : fieldsByPosition) {
      fieldsByTableRefAndPosition.computeIfAbsent(field.getTableRef(), tableRef ->
          new ArrayList<>()).add(field);
      fieldsByTableRefAndName.put(field.getTableRef(), field.getName(), field);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isUnique() {
    return unique;
  }

  @Override
  public int size() {
    return fieldsByPosition.size();
  }

  @Override
  public Iterator<ImmutableMetaIndexField> iteratorFields() {
    return fieldsByPosition.iterator();
  }

  @Override
  public Iterator<? extends ImmutableMetaIndexField> iteratorMetaIndexFieldByTableRef(
      TableRef tableRef) {
    return fieldsByTableRefAndPosition.computeIfAbsent(tableRef, t -> new ArrayList<>())
        .iterator();
  }

  @Override
  public Stream<TableRef> streamTableRefs() {
    return fieldsByTableRefAndName.rowKeySet().stream();
  }

  @Override
  public ImmutableMetaIndexField getMetaIndexFieldByTableRefAndName(
      TableRef tableRef, String name) {
    return fieldsByTableRefAndName.get(tableRef, name);
  }

  @Override
  public ImmutableMetaIndexField getMetaIndexFieldByTableRefAndPosition(TableRef tableRef,
      int position) {
    return fieldsByTableRefAndPosition.computeIfAbsent(tableRef, t -> new ArrayList<>()).get(
        position);
  }

  @Override
  public ImmutableMetaIndexField getMetaIndexFieldByPosition(int position) {
    return fieldsByPosition.get(position);
  }

  @Override
  public boolean isCompatible(MetaDocPart docPart) {
    return isCompatible(docPart,
        iteratorMetaIndexFieldByTableRef(docPart.getTableRef()));
  }

  protected boolean isCompatible(MetaDocPart docPart,
      Iterator<? extends MetaIndexField> indexFieldIterator) {
    if (!indexFieldIterator.hasNext()) {
      return false;
    }

    while (indexFieldIterator.hasNext()) {
      MetaIndexField indexField = indexFieldIterator.next();
      if (!indexField.isCompatible(docPart)) {
        return false;
      }
    }

    return !indexFieldIterator.hasNext();
  }

  @Override
  public boolean isCompatible(MetaDocPart docPart, MetaDocPartIndex docPartIndex) {
    return isCompatible(docPart, docPartIndex,
        iteratorMetaIndexFieldByTableRef(docPart.getTableRef()));
  }

  protected boolean isCompatible(MetaDocPart docPart, MetaDocPartIndex docPartIndex,
      Iterator<? extends MetaIndexField> indexFieldIterator) {
    if (unique != docPartIndex.isUnique()) {
      return false;
    }

    if (!indexFieldIterator.hasNext()) {
      return false;
    }

    Iterator<? extends MetaDocPartIndexColumn> fieldIndexIterator =
        docPartIndex.iteratorColumns();
    while (indexFieldIterator.hasNext() && fieldIndexIterator.hasNext()) {
      MetaIndexField indexField = indexFieldIterator.next();
      MetaDocPartIndexColumn indexColumn = fieldIndexIterator.next();
      if (!indexField.isCompatible(docPart, indexColumn)) {
        return false;
      }
    }

    return !indexFieldIterator.hasNext() && !fieldIndexIterator.hasNext();
  }

  @Override
  public boolean isMatch(MetaDocPart docPart, List<String> identifiers,
      MetaDocPartIndex docPartIndex) {
    return isMatch(docPart, identifiers, docPartIndex,
        iteratorMetaIndexFieldByTableRef(docPart.getTableRef()), false);
  }

  protected boolean isMatch(MetaDocPart docPart, List<String> identifiers,
      MetaDocPartIndex docPartIndex, Iterator<? extends MetaIndexField> indexFieldIterator,
      boolean isSubMatch) {
    if (isUnique() != docPartIndex.isUnique()) {
      return false;
    }

    if (!indexFieldIterator.hasNext()) {
      return false;
    }

    if (isSubMatch && identifiers.isEmpty()) {
      return true;
    }

    Iterator<? extends MetaDocPartIndexColumn> fieldIndexIterator =
        docPartIndex.iteratorColumns();
    Iterator<String> identifiersIterator = identifiers.iterator();
    while (indexFieldIterator.hasNext() && (isSubMatch || fieldIndexIterator.hasNext())
        && identifiersIterator.hasNext()) {
      MetaIndexField indexField = indexFieldIterator.next();
      MetaDocPartIndexColumn indexColumn;
      if (isSubMatch) {
        if (fieldIndexIterator.hasNext()) {
          indexColumn = fieldIndexIterator.next();
        } else {
          indexColumn = null;
        }
      } else {
        indexColumn = fieldIndexIterator.next();
      }
      String identifier = identifiersIterator.next();
      if (indexColumn == null) {
        if (!isSubMatch) {
          return false;
        }
      } else {
        if (!indexField.isMatch(docPart, identifier, indexColumn)) {
          return false;
        }
      }
    }

    return (isSubMatch || !indexFieldIterator.hasNext() && !fieldIndexIterator.hasNext())
        && !identifiersIterator.hasNext();
  }
  
  @Override
  public boolean isMatch(MetaIndex index) {
    return isMatch(index, iteratorFields());
  }

  protected boolean isMatch(MetaIndex index, Iterator<? extends MetaIndexField> iteratorFields) {
    if (getName().equals(index.getName())) {
      return true;
    }

    return index.isUnique() == isUnique() && index.size() == size() && Seq.seq(iteratorFields)
        .allMatch(indexField -> {
          MetaIndexField otherIndexField = index.getMetaIndexFieldByPosition(indexField
              .getPosition());
          return otherIndexField != null && indexField.isMatch(otherIndexField);
        });
  }

  @Override
  public boolean isSubMatch(MetaDocPart docPart, List<String> identifiersSublist,
      MetaDocPartIndex docPartIndex) {
    return isMatch(docPart, identifiersSublist, docPartIndex,
        iteratorMetaIndexFieldByTableRef(docPart.getTableRef()), true);
  }

  @Override
  public Iterator<List<String>> iteratorMetaDocPartIndexesIdentifiers(MetaDocPart docPart) {
    return iteratorMetaDocPartIndexesIdentifiers(docPart, iteratorMetaIndexFieldByTableRef(docPart
        .getTableRef()));
  }

  protected Iterator<List<String>> iteratorMetaDocPartIndexesIdentifiers(MetaDocPart docPart,
      Iterator<? extends MetaIndexField> indexFieldIterator) {
    List<List<String>> docPartIndexesIdentifiers = new ArrayList<>((int) Math.pow(2, size()));
    while (indexFieldIterator.hasNext()) {
      MetaIndexField indexField = indexFieldIterator.next();
      cartesianAppend(docPartIndexesIdentifiers,
          docPart.streamMetaFieldByName(indexField.getName())
              .collect(Collectors.toList()));
    }
    return docPartIndexesIdentifiers.iterator();
  }

  private void cartesianAppend(List<List<String>> docPartIndexesIdentifiers,
      List<MetaField> fields) {
    if (fields.isEmpty()) {
      return;
    }

    if (docPartIndexesIdentifiers.isEmpty()) {
      for (MetaField field : fields) {
        List<String> docPartIndexIdentifiers = new ArrayList<>();
        docPartIndexIdentifiers.add(field.getIdentifier());
        docPartIndexesIdentifiers.add(docPartIndexIdentifiers);
      }
    } else {
      List<List<String>> newDocPartIndexesIdentifiers =
          new ArrayList<>(docPartIndexesIdentifiers.size() * fields.size());
      for (List<String> docPartIndexIdentifiers : docPartIndexesIdentifiers) {
        Iterator<MetaField> fieldsIterator = fields.iterator();
        MetaField field = fieldsIterator.next();

        while (fieldsIterator.hasNext()) {
          MetaField nextField = fieldsIterator.next();
          List<String> docPartIndexIdentifiersCopy = new ArrayList<>(docPartIndexIdentifiers);
          docPartIndexIdentifiersCopy.add(nextField.getIdentifier());
          newDocPartIndexesIdentifiers.add(docPartIndexIdentifiersCopy);
        }

        docPartIndexIdentifiers.add(field.getIdentifier());
        newDocPartIndexesIdentifiers.add(docPartIndexIdentifiers);
      }

      docPartIndexesIdentifiers.clear();
      docPartIndexesIdentifiers.addAll(newDocPartIndexesIdentifiers);
    }
  }

  @Override
  public String toString() {
    return defautToString();
  }

  public static class Builder {

    private boolean built = false;
    private final String name;
    private final boolean unique;
    private final List<ImmutableMetaIndexField> fieldsByPosition;

    public Builder(String name, boolean unique) {
      this.name = name;
      this.unique = unique;
      fieldsByPosition = new ArrayList<>();
    }

    public Builder(String name, boolean unique, int expectedFields) {
      this.name = name;
      this.unique = unique;
      fieldsByPosition = new ArrayList<>(expectedFields);
    }

    public Builder(ImmutableMetaIndex other) {
      this.name = other.name;
      this.unique = other.isUnique();
      fieldsByPosition = new ArrayList<>(other.fieldsByPosition);
    }

    public Builder add(ImmutableMetaIndexField field) {
      Preconditions.checkState(!built, "This builder has already been built");
      fieldsByPosition.add(field);
      return this;
    }

    public Builder remove(MetaIndexField field) {
      Preconditions.checkState(!built, "This builder has already been built");
      fieldsByPosition.remove(field.getPosition());
      return this;
    }

    public ImmutableMetaIndex build() {
      Preconditions.checkState(!built, "This builder has already been built");
      built = true;
      return new ImmutableMetaIndex(name, unique, fieldsByPosition);
    }
  }

}
