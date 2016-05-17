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
 * along with query. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 gortiz.
 * 
 */

package com.torodb.poc.query.dsl.mongo;

/**
 *
 */
public class XorMongoQuery implements MongoQuery {

    private final MongoQuery q1;
    private final MongoQuery q2;

    public XorMongoQuery(MongoQuery q1, MongoQuery q2) {
        this.q1 = q1;
        this.q2 = q2;
    }

    public MongoQuery getQ1() {
        return q1;
    }

    public MongoQuery getQ2() {
        return q2;
    }

    @Override
    public <R, A> R visit(MongoQueryVisitor<R, A> visitor, A arg) {
        return visitor.visit(this, arg);
    }

}
