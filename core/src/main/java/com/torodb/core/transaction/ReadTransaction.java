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
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.transaction;

import com.torodb.core.dsl.projection.ToroProjection;
import com.torodb.core.dsl.query.ToroQuery;
import com.torodb.kvdocument.values.KVDocument;
import java.util.stream.Stream;
import com.torodb.core.transaction.metainf.MetaSnapshot;

/**
 *
 */
public interface ReadTransaction extends AutoCloseable {

    public Stream<KVDocument> read(ToroQuery query, ToroProjection projection);

    public MetaSnapshot getMetainfoView();

    @Override
    public void close() throws ToroTransactionException;

}
