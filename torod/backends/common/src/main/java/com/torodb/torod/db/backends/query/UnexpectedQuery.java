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

package com.torodb.torod.db.backends.query;

import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;

/**
 *
 */
public class UnexpectedQuery extends ToroImplementationException {

    private static final long serialVersionUID = 1L;

    private final QueryCriteria query;

    public UnexpectedQuery(QueryCriteria query) {
        this.query = query;
    }

    public UnexpectedQuery(QueryCriteria query, String message) {
        super(message);
        this.query = query;
    }

    public UnexpectedQuery(QueryCriteria query, String message, Throwable cause) {
        super(message, cause);
        this.query = query;
    }

    public UnexpectedQuery(QueryCriteria query, Throwable cause) {
        super(cause);
        this.query = query;
    }

    public QueryCriteria getQuery() {
        return query;
    }

    @Override
    public String getMessage() {
        return "Unexpected query '" + query + "': " + super.getMessage();
    }

}
