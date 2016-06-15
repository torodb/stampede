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

import com.torodb.core.backend.BackendConnection;
import com.torodb.core.dsl.backend.AddFieldDDLJob;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;

public class AddFieldDDLJobImpl implements AddFieldDDLJob {

    private final MetaDatabase db;
    private final MetaCollection col;
    private final MetaDocPart docPart;
    private final MetaField newField;

    public AddFieldDDLJobImpl(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField) {
        super();
        this.db = db;
        this.col = col;
        this.docPart = docPart;
        this.newField = newField;
    }

    @Override
    public void execute(BackendConnection connection) {
        connection.addField(db, col, docPart, newField);
    }

    @Override
    public MetaDatabase getDatabase() {
        return db;
    }

    @Override
    public MetaCollection getCollection() {
        return col;
    }

    @Override
    public MetaDocPart getDocPart() {
        return docPart;
    }

    @Override
    public MetaField getField() {
        return newField;
    }

}