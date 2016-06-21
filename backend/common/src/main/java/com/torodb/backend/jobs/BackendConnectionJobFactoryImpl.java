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

package com.torodb.backend.jobs;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.dsl.backend.*;
import com.torodb.core.transaction.metainf.*;

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
    public AddFieldDDLJob createAddFieldDDLJob(MetaDatabase db, MetaCollection col, MetaDocPart docPart,
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
