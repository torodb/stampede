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

import com.eightkdata.mongowp.bson.BsonValue;
import java.util.List;

/**
 *
 */
public class SimpleAttOpMongoQuery implements MongoQuery {

    private final List<String> path;
    private final BsonValue<?> constant;

    public SimpleAttOpMongoQuery(List<String> path, BsonValue<?> constant) {
        this.path = path;
        this.constant = constant;
    }

    public List<String> getPath() {
        return path;
    }

    public BsonValue<?> getConstant() {
        return constant;
    }

    @Override
    public <R, A> R visit(MongoQueryVisitor<R, A> visitor, A arg) {
        return visitor.visit(this, arg);
    }

    public static enum BinaryOp {
        EQ, GT, LW, REGEX, 
    }
}
