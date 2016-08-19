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

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 *
 */
public abstract class AbstractAnalyzedOp extends AnalyzedOp {

    private final KVValue<?> mongoDocId;
    private final AnalyzedOpType type;
    @Nullable
    private final Function<KVDocument, KVDocument> calculateFun;

    public AbstractAnalyzedOp(KVValue<?> mongoDocId, AnalyzedOpType type,
            @Nullable Function<KVDocument, KVDocument> calculateFun) {
        this.mongoDocId = mongoDocId;
        this.type = type;
        this.calculateFun = calculateFun;
    }

    abstract AnalyzedOp andThenInsert(KVDocument doc);

    @Override
    public abstract String toString();

    @Override
    AnalyzedOp andThenInsert(InsertOplogOperation op) {
        return andThenInsert(MongoWPConverter.toEagerDocument(op.getDocToInsert()));
    }

    @Override
    AnalyzedOp andThenUpsertSet(UpdateOplogOperation op) {
        return andThenInsert(UpdateActionsTool.applyAsUpsert(op));
    }

    @Override
    public AnalyzedOpType getType() {
        return type;
    }

    @Override
    public KVValue<?> getMongoDocId() {
        return mongoDocId;
    }

    @Override
    public Status<?> getMismatchErrorMessage() throws UnsupportedOperationException {
        if (!requiresMatch()) {
            throw new UnsupportedOperationException();
        }
        return Status.from(ErrorCode.OPERATION_FAILED);
    }

    @Override
    public KVDocument calculateDocToInsert(Function<AnalyzedOp, KVDocument> fetchedDocFun) {
        if (calculateFun == null) {
            return null;
        }
        KVDocument fetchedDoc = null;
        if (requiresFetch()) {
            fetchedDoc = fetchedDocFun.apply(this);
        }
        return calculateFun.apply(fetchedDoc);
    }

    protected final Function<KVDocument, KVDocument> createUpdateSetAsDocument(UpdateOplogOperation op) {
        assert UpdateActionsTool.isSetModification(op); //This is more a warning than an upsertion. Remove if want to do fancy things

        //A simple assertion to fail before the callback is called when an illegal update is recived
        assert UpdateActionsTool.applyAsUpsert(op) != null;

        return (ignored) -> UpdateActionsTool.applyAsUpsert(op);
    }

    protected final Function<KVDocument, KVDocument> createUpdateMergeChain(UpdateOplogOperation op) {
        //A simple assertion to fail before the callback is called when an illegal update is recived
        assert UpdateActionsTool.parseUpdateAction(op) != null;

        assert calculateFun != null;

        return (fetch) -> UpdateActionsTool.applyModification(
                calculateFun.apply(fetch),
                UpdateActionsTool.parseUpdateAction(op)
        );
    }
}
