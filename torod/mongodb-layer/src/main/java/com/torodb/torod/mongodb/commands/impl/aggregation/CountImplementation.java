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
 * along with mongodb-layer. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.torod.mongodb.commands.impl.aggregation;

import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.aggregation.CountCommand.CountArgument;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.google.common.util.concurrent.Futures;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import javax.inject.Inject;

/**
 *
 */
public class CountImplementation extends AbstractToroCommandImplementation<CountArgument, Long> {

    private final QueryCriteriaTranslator queryCriteriaTranslator;

    @Inject
    public CountImplementation(QueryCriteriaTranslator queryCriteriaTranslator) {
        this.queryCriteriaTranslator = queryCriteriaTranslator;
    }

    @Override
    public CommandResult<Long> apply(
            Command<? super CountArgument, ? super Long> command,
            CommandRequest<CountArgument> req) throws MongoException {

        CountArgument arg = req.getCommandArgument();
        ToroConnection connection = getToroConnection(req);

        QueryCriteria queryCriteria;
        if (arg.getQuery() == null) {
            queryCriteria = TrueQueryCriteria.getInstance();
        } else {
            queryCriteria = queryCriteriaTranslator.translate(arg.getQuery());
        }

        try (ToroTransaction transaction = connection.createTransaction(TransactionMetainfo.READ_ONLY)) {
            return new NonWriteCommandResult<>(
                    Futures.get(transaction.count(arg.getCollection(), queryCriteria), UnknownErrorException.class).longValue()
            );
        } catch (ImplementationDbException ex) {
            throw new UnknownErrorException(ex);
        }
    }

}
