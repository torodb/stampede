/*
 * ToroDB - ToroDB-poc: Backend common
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.jobs;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.dsl.backend.AddCollectionDDLJob;
import com.torodb.core.dsl.backend.AddDatabaseDDLJob;
import com.torodb.core.dsl.backend.AddDocPartDDLJob;
import com.torodb.core.dsl.backend.AddFieldDDLJob;
import com.torodb.core.dsl.backend.AddScalarDDLJob;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.dsl.backend.InsertBackendJob;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;

public class BackendConnectionJobFactoryImpl implements BackendTransactionJobFactory {

    @Override
    public AddDatabaseDDLJob createAddDatabaseDDLJob(MetaDatabase db) {
        return new AddDatabaseDDLJobImpl(db);
    }

    @Override
    public AddCollectionDDLJob createAddCollectionDDLJob(MetaDatabase db, MetaCollection col) {
        return new AddCollectionDDLJobImpl(db, col);
    }

    @Override
    public AddDocPartDDLJob createAddDocPartDDLJob(MetaDatabase db, MetaCollection col, MetaDocPart docPart) {
        return new AddDocPartDDLJobImpl(db, col, docPart);
    }

    @Override
    public AddFieldDDLJob createAddFieldDDLJob(MetaDatabase db, MetaCollection col, MutableMetaDocPart docPart,
            MetaField field) {
        return new AddFieldDDLJobImpl(db, col, docPart, field);
    }

    @Override
    public AddScalarDDLJob createAddScalarDDLJob(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaScalar scalar) {
        return new AddScalarDDLJobImpl(db, col, docPart, scalar);
    }

    @Override
    public InsertBackendJob insert(MetaDatabase db, MetaCollection col, DocPartData data) {
        return new InsertBackendJobImpl(db, col, data);
    }

}
