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

package com.torodb.backend;

import com.google.common.base.Preconditions;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaDocPartIndex;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import org.jooq.lambda.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExclusiveWriteBackendTransactionImpl extends SharedWriteBackendTransactionImpl
    implements ExclusiveWriteBackendTransaction {

  private final ReservedIdGenerator ridGenerator;

  public ExclusiveWriteBackendTransactionImpl(SqlInterface sqlInterface,
      BackendConnectionImpl backendConnection,
      IdentifierFactory identifierFactory,
      ReservedIdGenerator ridGenerator) {
    super(sqlInterface, backendConnection, identifierFactory);

    this.ridGenerator = ridGenerator;
  }

  @Override
  public void renameCollection(MetaDatabase fromDb, MetaCollection fromColl,
      MutableMetaDatabase toDb, MutableMetaCollection toColl) {
    Preconditions.checkState(!isClosed(), "This transaction is closed");

    copyMetaCollection(fromDb, fromColl, toDb, toColl);
    getSqlInterface().getStructureInterface().renameCollection(getDsl(), fromDb.getIdentifier(),
        fromColl,
        toDb.getIdentifier(), toColl);
    dropMetaCollection(fromDb, fromColl);
  }

  @Override
  public void dropAll() throws RollbackException {
    getSqlInterface().getStructureInterface().dropAll(getDsl());
  }

  @Override
  public void dropUserData() throws RollbackException {
    getSqlInterface().getStructureInterface().dropUserData(getDsl());
  }

  @Override
  public void checkOrCreateMetaDataTables() throws InvalidDatabaseException {
    getBackendConnection().getSchemaUpdater().checkOrCreate(getDsl());
  }

  private void copyMetaCollection(MetaDatabase fromDb, MetaCollection fromColl,
      MutableMetaDatabase toDb, MutableMetaCollection toColl) {
    IdentifierFactory identifierFactory = getIdentifierFactory();

    Iterator<? extends MetaIndex> fromMetaIndexIterator = fromColl.streamContainedMetaIndexes()
        .iterator();
    while (fromMetaIndexIterator.hasNext()) {
      MetaIndex fromMetaIndex = fromMetaIndexIterator.next();
      MutableMetaIndex toMetaIndex = toColl.addMetaIndex(fromMetaIndex.getName(), fromMetaIndex
          .isUnique());
      getSqlInterface().getMetaDataWriteInterface()
          .addMetaIndex(getDsl(), toDb, toColl, toMetaIndex);
      copyIndexFields(fromMetaIndex, toDb, toColl, toMetaIndex);
    }

    Iterator<? extends MetaDocPart> fromMetaDocPartIterator = fromColl.streamContainedMetaDocParts()
        .iterator();
    while (fromMetaDocPartIterator.hasNext()) {
      MetaDocPart fromMetaDocPart = fromMetaDocPartIterator.next();
      MutableMetaDocPart toMetaDocPart = toColl.addMetaDocPart(fromMetaDocPart.getTableRef(),
          identifierFactory.toDocPartIdentifier(
              toDb, toColl.getName(), fromMetaDocPart.getTableRef()));
      getSqlInterface().getMetaDataWriteInterface().addMetaDocPart(getDsl(), toDb, toColl,
          toMetaDocPart);
      copyScalar(identifierFactory, fromMetaDocPart, toDb, toColl, toMetaDocPart);
      copyFields(identifierFactory, fromMetaDocPart, toDb, toColl, toMetaDocPart);
      copyIndexes(identifierFactory, fromMetaDocPart, toDb, toColl, toMetaDocPart);
      int nextRid = ridGenerator.getDocPartRidGenerator(fromDb.getName(), fromColl.getName())
          .nextRid(fromMetaDocPart.getTableRef());
      ridGenerator.getDocPartRidGenerator(toDb.getName(), toColl.getName()).setNextRid(toMetaDocPart
          .getTableRef(), nextRid - 1);
    }
  }

  private void copyIndexFields(MetaIndex fromMetaIndex,
      MetaDatabase toMetaDb, MetaCollection toMetaColl, MutableMetaIndex toMetaIndex) {
    Iterator<? extends MetaIndexField> fromMetaIndexFieldIterator = fromMetaIndex.iteratorFields();
    while (fromMetaIndexFieldIterator.hasNext()) {
      MetaIndexField fromMetaIndexField = fromMetaIndexFieldIterator.next();
      MetaIndexField toMetaIndexField = toMetaIndex.addMetaIndexField(
          fromMetaIndexField.getTableRef(),
          fromMetaIndexField.getName(),
          fromMetaIndexField.getOrdering());
      getSqlInterface().getMetaDataWriteInterface().addMetaIndexField(
          getDsl(), toMetaDb, toMetaColl, toMetaIndex, toMetaIndexField);
    }
  }

  private void copyScalar(IdentifierFactory identifierFactory, MetaDocPart fromMetaDocPart,
      MetaDatabase toMetaDb, MetaCollection toMetaColl, MutableMetaDocPart toMetaDocPart) {
    Iterator<? extends MetaScalar> fromMetaScalarIterator = fromMetaDocPart.streamScalars()
        .iterator();
    while (fromMetaScalarIterator.hasNext()) {
      MetaScalar fromMetaScalar = fromMetaScalarIterator.next();
      MetaScalar toMetaScalar = toMetaDocPart.addMetaScalar(
          identifierFactory.toFieldIdentifierForScalar(fromMetaScalar.getType()),
          fromMetaScalar.getType());
      getSqlInterface().getMetaDataWriteInterface().addMetaScalar(
          getDsl(), toMetaDb, toMetaColl, toMetaDocPart, toMetaScalar);
    }
  }

  private void copyFields(IdentifierFactory identifierFactory, MetaDocPart fromMetaDocPart,
      MetaDatabase toMetaDb, MetaCollection toMetaColl, MutableMetaDocPart toMetaDocPart) {
    Iterator<? extends MetaField> fromMetaFieldIterator = fromMetaDocPart.streamFields().iterator();
    while (fromMetaFieldIterator.hasNext()) {
      MetaField fromMetaField = fromMetaFieldIterator.next();
      MetaField toMetaField = toMetaDocPart.addMetaField(
          fromMetaField.getName(),
          identifierFactory.toFieldIdentifier(toMetaDocPart, fromMetaField.getName(), fromMetaField
              .getType()),
          fromMetaField.getType());
      getSqlInterface().getMetaDataWriteInterface().addMetaField(
          getDsl(), toMetaDb, toMetaColl, toMetaDocPart, toMetaField);
    }
  }

  private void copyIndexes(IdentifierFactory identifierFactory, MetaDocPart fromMetaDocPart,
      MetaDatabase toMetaDb, MetaCollection toMetaColl, MutableMetaDocPart toMetaDocPart) {
    Iterator<? extends MetaIdentifiedDocPartIndex> fromMetaDocPartIndexIterator = fromMetaDocPart
        .streamIndexes().iterator();
    while (fromMetaDocPartIndexIterator.hasNext()) {
      MetaIdentifiedDocPartIndex fromMetaDocPartIndex = fromMetaDocPartIndexIterator.next();
      MutableMetaDocPartIndex toMutableMetaDocPartIndex = toMetaDocPart.addMetaDocPartIndex(
          fromMetaDocPartIndex.isUnique());
      List<Tuple2<String, Boolean>> identifiers =
          copyMetaIndexColumns(fromMetaDocPartIndex, toMutableMetaDocPartIndex);
      MetaIdentifiedDocPartIndex toMetaDocPartIndex = toMutableMetaDocPartIndex.immutableCopy(
          identifierFactory.toIndexIdentifier(
              toMetaDb,
              toMetaDocPart.getIdentifier(),
              identifiers)
      );
      getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndex(
          getDsl(), toMetaDb, toMetaColl, toMetaDocPart, toMetaDocPartIndex);
      writeIndexColumns(toMetaDb, toMetaColl, toMetaDocPart, toMetaDocPartIndex);
    }
  }

  private List<Tuple2<String, Boolean>> copyMetaIndexColumns(
      MetaIdentifiedDocPartIndex fromMetaDocPartIndex,
      MutableMetaDocPartIndex toMetaDocPartIndex) {
    List<Tuple2<String, Boolean>> identifiers = new ArrayList<>();
    Iterator<? extends MetaDocPartIndexColumn> fromMetaDocPartIndexColumnIterator =
        fromMetaDocPartIndex.iteratorColumns();
    while (fromMetaDocPartIndexColumnIterator.hasNext()) {
      MetaDocPartIndexColumn fromMetaDocPartIndexColumn = fromMetaDocPartIndexColumnIterator.next();
      toMetaDocPartIndex.addMetaDocPartIndexColumn(
          fromMetaDocPartIndexColumn.getIdentifier(), fromMetaDocPartIndexColumn.getOrdering());
      identifiers.add(new Tuple2<>(fromMetaDocPartIndexColumn.getIdentifier(),
          fromMetaDocPartIndexColumn.getOrdering().isAscending()));
    }
    return identifiers;
  }

  private void writeIndexColumns(MetaDatabase toMetaDb, MetaCollection toMetaColl,
      MetaDocPart toMetaDocPart,
      MetaIdentifiedDocPartIndex toMetaDocPartIndex) {
    Iterator<? extends MetaDocPartIndexColumn> toMetaDocPartIndexColumnIterator = toMetaDocPartIndex
        .iteratorColumns();
    while (toMetaDocPartIndexColumnIterator.hasNext()) {
      MetaDocPartIndexColumn toMetaDocPartIndexColumn = toMetaDocPartIndexColumnIterator.next();
      getSqlInterface().getMetaDataWriteInterface().addMetaDocPartIndexColumn(
          getDsl(), toMetaDb, toMetaColl, toMetaDocPart, toMetaDocPartIndex,
          toMetaDocPartIndexColumn);
    }
  }

}
