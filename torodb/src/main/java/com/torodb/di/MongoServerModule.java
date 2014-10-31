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

package com.torodb.di;

import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.callback.RequestProcessor;
import com.google.inject.AbstractModule;
import com.torodb.mongowp.mongoserver.api.toro.ToroQueryCommandProcessor;
import com.torodb.mongowp.mongoserver.api.toro.ToroRequestProcessor;

/**
 *
 */
public class MongoServerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(RequestProcessor.class).to(ToroRequestProcessor.class);
        bind(QueryCommandProcessor.class).to(ToroQueryCommandProcessor.class);
    }
}
