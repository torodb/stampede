/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.backend;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;

public class ExclusiveWriteBackendTransactionImpl extends SharedWriteBackendTransactionImpl implements ExclusiveWriteBackendTransaction {

    private final IdentifierFactory identifierFactory;
    private final RidGenerator ridGenerator;
    
    public ExclusiveWriteBackendTransactionImpl(SqlInterface sqlInterface, BackendConnectionImpl backendConnection,
            R2DTranslator r2dTranslator, IdentifierFactory identifierFactory, RidGenerator ridGenerator) {
        super(sqlInterface, backendConnection, r2dTranslator, identifierFactory);
        
        this.identifierFactory = identifierFactory;
        this.ridGenerator = ridGenerator;
    }
    
    @Override
    public void renameCollection(MetaDatabase fromDb, MetaCollection fromColl, MutableMetaDatabase toDb, MutableMetaCollection toColl) {
        Preconditions.checkState(!isClosed(), "This transaction is closed");

        copyMetaCollection(fromDb, fromColl, toDb, toColl);
        getSqlInterface().getStructureInterface().renameCollection(getDsl(), fromDb.getIdentifier(), fromColl,
                toDb.getIdentifier(), toColl);
        dropMetaCollection(fromDb, fromColl);
    }

    private void copyMetaCollection(MetaDatabase fromDb, MetaCollection fromColl,
            MutableMetaDatabase toDb, MutableMetaCollection toColl) {
        Iterator<? extends MetaDocPart> fromMetaDocPartIterator = fromColl.streamContainedMetaDocParts().iterator();
        while (fromMetaDocPartIterator.hasNext()) {
            MetaDocPart fromMetaDocPart = fromMetaDocPartIterator.next();
            MutableMetaDocPart toMetaDocPart = toColl.addMetaDocPart(fromMetaDocPart.getTableRef(), 
                    identifierFactory.toDocPartIdentifier(
                            toDb, toColl.getName(), fromMetaDocPart.getTableRef()));
            getSqlInterface().getMetaDataWriteInterface().addMetaDocPart(getDsl(), toDb, toColl, toMetaDocPart);
            copyScalar(identifierFactory, fromMetaDocPart, toDb, toColl, toMetaDocPart);
            copyFields(identifierFactory, fromMetaDocPart, toDb, toColl, toMetaDocPart);
            int nextRid = ridGenerator.getDocPartRidGenerator(fromDb.getName(), fromColl.getName()).nextRid(fromMetaDocPart.getTableRef());
            ridGenerator.getDocPartRidGenerator(toDb.getName(), toColl.getName()).setNextRid(toMetaDocPart.getTableRef(), nextRid - 1);
        }
    }

    private void copyScalar(IdentifierFactory identifierFactory, MetaDocPart fromMetaDocPart,
            MetaDatabase toMetaDb, MetaCollection toMetaColl, MutableMetaDocPart toMetaDocPart) {
        Iterator<? extends MetaScalar> fromMetaScalarIterator = fromMetaDocPart.streamScalars().iterator();
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
                    identifierFactory.toFieldIdentifier(toMetaDocPart, fromMetaField.getName(), fromMetaField.getType()), 
                    fromMetaField.getType());
            getSqlInterface().getMetaDataWriteInterface().addMetaField(
                    getDsl(), toMetaDb, toMetaColl, toMetaDocPart, toMetaField);
        }
    }

}
