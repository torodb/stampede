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

import com.torodb.kvdocument.values.KVValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 *
 */
public class NoopAnalyzedOpTest extends AbstractAnalyzedOpTest<NoopAnalyzedOp> {
    private static final Logger LOGGER = LogManager.getLogger(NoopAnalyzedOpTest.class);

    @Override
    NoopAnalyzedOp getAnalyzedOp(KVValue<?> mongoDocId) {
        return new NoopAnalyzedOp(mongoDocId);
    }

    @Override
    Logger getLogger() {
        return LOGGER;
    }

    //TODO: Callback checks can be improved
    @Override
    void andThenInsertTest() {
        andThenInsert(this::emptyConsumer3,this::emptyBiConsumer);
    }

    //TODO: Callback checks can be improved
    @Override
    void andThenUpdateModTest() {
        andThenUpdateMod(this::emptyConsumer3,this::emptyBiConsumer);
    }

    //TODO: Callback checks can be improved
    @Override
    void andThenUpdateSetTest() {
        andThenUpdateSet(this::emptyConsumer3,this::emptyBiConsumer);
    }

    //TODO: Callback checks can be improved
    @Override
    void andThenUpsertModTest() {
        andThenUpsertMod(this::emptyConsumer3,this::emptyBiConsumer);
    }

    //TODO: Callback checks can be improved
    @Override
    void andThenUpsertSetTest() {
        andThenUpsertSet(this::emptyConsumer3,this::emptyBiConsumer);
    }

    //TODO: Callback checks can be improved
    @Override
    void andThenDeleteTest() {
        andThenDelete(this::emptyConsumer3,this::emptyBiConsumer);
    }

}
