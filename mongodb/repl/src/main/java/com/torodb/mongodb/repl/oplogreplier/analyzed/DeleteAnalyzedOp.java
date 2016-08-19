/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 *
 */
package com.torodb.mongodb.repl.oplogreplier.analyzed;

import com.eightkdata.mongowp.server.api.oplog.DeleteOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
public class DeleteAnalyzedOp extends AbstractAnalyzedOp {

    DeleteAnalyzedOp(KVValue<?> mongoDocId) {
        super(mongoDocId, AnalyzedOpType.DELETE, null);
    }

    @Override
    public AnalyzedOp andThenInsert(KVDocument doc) {
        return new DeleteCreateAnalyzedOp(getMongoDocId(), doc);
    }

    @Override
    public AnalyzedOp andThenUpdateMod(UpdateOplogOperation op) {
        return new ErrorAnalyzedOp(getMongoDocId());
    }

    @Override
    public AnalyzedOp andThenUpdateSet(UpdateOplogOperation op) {
        return new ErrorAnalyzedOp(getMongoDocId());
    }

    @Override
    public AnalyzedOp andThenUpsertMod(UpdateOplogOperation op) {
        return new DeleteCreateAnalyzedOp(getMongoDocId(), createUpdateSetAsDocument(op));
    }

    @Override
    public AnalyzedOp andThenDelete(DeleteOplogOperation op) {
        return this;
    }

    @Override
    public String toString() {
        return "d(" + getMongoDocId() + ')';
    }
}