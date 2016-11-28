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

package com.torodb.torod.pipeline;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaDocPartIndex;
import org.jooq.lambda.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 */
public class BatchMetaDocPart implements MutableMetaDocPart {

  private final MutableMetaDocPart delegate;
  private final ArrayList<ImmutableMetaField> fieldsChangesOnBatch = new ArrayList<>();
  private final ArrayList<ImmutableMetaScalar> scalarChangesOnBatch = new ArrayList<>();
  private final Consumer<BatchMetaDocPart> changeConsumer;
  private boolean createdOnCurrentBatch;

  public BatchMetaDocPart(MutableMetaDocPart delegate, Consumer<BatchMetaDocPart> changeConsumer,
      boolean createdOnCurrentBatch) {
    this.delegate = delegate;
    this.createdOnCurrentBatch = createdOnCurrentBatch;
    this.changeConsumer = changeConsumer;
  }

  public void newBatch() {
    fieldsChangesOnBatch.clear();
    scalarChangesOnBatch.clear();
    createdOnCurrentBatch = false;
  }

  public boolean isCreatedOnCurrentBatch() {
    return createdOnCurrentBatch;
  }

  public void setCreatedOnCurrentBatch(boolean createdOnCurrentBatch) {
    this.createdOnCurrentBatch = createdOnCurrentBatch;
  }

  @DoNotChange
  public Iterable<ImmutableMetaField> getOnBatchModifiedMetaFields() {
    return fieldsChangesOnBatch;
  }

  @DoNotChange
  public Iterable<ImmutableMetaScalar> getOnBatchModifiedMetaScalars() {
    return scalarChangesOnBatch;
  }

  @Override
  public ImmutableMetaField addMetaField(String name, String identifier, FieldType type) throws
      IllegalArgumentException {
    ImmutableMetaField newMetaField = delegate.addMetaField(name, identifier, type);

    fieldsChangesOnBatch.add(newMetaField);
    changeConsumer.accept(this);

    return newMetaField;
  }

  @Override
  public ImmutableMetaScalar addMetaScalar(String identifier, FieldType type) throws
      IllegalArgumentException {
    ImmutableMetaScalar newMetaScalar = delegate.addMetaScalar(identifier, type);

    scalarChangesOnBatch.add(newMetaScalar);
    changeConsumer.accept(this);

    return newMetaScalar;
  }

  @Override
  public ImmutableMetaField getMetaFieldByNameAndType(String fieldName, FieldType type) {
    return delegate.getMetaFieldByNameAndType(fieldName, type);
  }

  @Override
  public Stream<? extends ImmutableMetaField> streamMetaFieldByName(String fieldName) {
    return delegate.streamMetaFieldByName(fieldName);
  }

  @Override
  public ImmutableMetaField getMetaFieldByIdentifier(String fieldId) {
    return delegate.getMetaFieldByIdentifier(fieldId);
  }

  @Override
  public Stream<? extends ImmutableMetaField> streamFields() {
    return delegate.streamFields();
  }

  @Override
  public Iterable<? extends ImmutableMetaField> getAddedMetaFields() {
    return delegate.getAddedMetaFields();
  }

  @Override
  public ImmutableMetaField getAddedFieldByIdentifier(String identifier) {
    return delegate.getAddedFieldByIdentifier(identifier);
  }

  @Override
  public ImmutableMetaDocPart immutableCopy() {
    return delegate.immutableCopy();
  }

  @Override
  public TableRef getTableRef() {
    return delegate.getTableRef();
  }

  @Override
  public String getIdentifier() {
    return delegate.getIdentifier();
  }

  @Override
  public Iterable<? extends ImmutableMetaScalar> getAddedMetaScalars() {
    return delegate.getAddedMetaScalars();
  }

  @Override
  public Stream<? extends MetaScalar> streamScalars() {
    return delegate.streamScalars();
  }

  @Override
  public Stream<? extends MetaIdentifiedDocPartIndex> streamIndexes() {
    return delegate.streamIndexes();
  }

  @Override
  public MetaIdentifiedDocPartIndex getMetaDocPartIndexByIdentifier(String indexId) {
    return delegate.getMetaDocPartIndexByIdentifier(indexId);
  }

  @Override
  public MutableMetaDocPartIndex addMetaDocPartIndex(boolean unique)
      throws IllegalArgumentException {
    return delegate.addMetaDocPartIndex(unique);
  }

  @Override
  @SuppressWarnings("checkstyle:LineLength")
  public Iterable<Tuple2<ImmutableMetaIdentifiedDocPartIndex, MetaElementState>> getModifiedMetaDocPartIndexes() {
    return delegate.getModifiedMetaDocPartIndexes();
  }

  @Override
  public Iterable<MutableMetaDocPartIndex> getAddedMutableMetaDocPartIndexes() {
    return delegate.getAddedMutableMetaDocPartIndexes();
  }

  @Override
  public boolean removeMetaDocPartIndexByIdentifier(String indexId) {
    return delegate.removeMetaDocPartIndexByIdentifier(indexId);
  }

  @Override
  public MutableMetaDocPartIndex getOrCreatePartialMutableDocPartIndexForMissingIndexAndNewField(
      MetaIndex missingIndex, List<String> identifiers, MetaField newField) {
    return delegate.getOrCreatePartialMutableDocPartIndexForMissingIndexAndNewField(missingIndex,
        identifiers, newField);
  }

  @Override
  public String toString() {
    return defautToString();
  }
}
