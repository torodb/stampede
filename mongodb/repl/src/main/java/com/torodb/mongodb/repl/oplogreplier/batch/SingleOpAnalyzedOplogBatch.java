/*
 * MongoWP - ToroDB-poc: MongoDB Repl
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
package com.torodb.mongodb.repl.oplogreplier.batch;

import com.eightkdata.mongowp.server.api.oplog.OplogOperation;

/**
 *
 */
public class SingleOpAnalyzedOplogBatch extends AnalyzedOplogBatch {

    private final OplogOperation operation;

    public SingleOpAnalyzedOplogBatch(OplogOperation operation) {
        this.operation = operation;
    }
    
    public OplogOperation getOperation() {
        return operation;
    }

    @Override
    public <Result, Arg, T extends Throwable> Result accept(AnalyzedOplogBatchVisitor<Result, Arg, T> visitor, Arg arg) throws T {
        return visitor.visit(this, arg);
    }

}
