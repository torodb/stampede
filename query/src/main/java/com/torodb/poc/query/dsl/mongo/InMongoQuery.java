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

import com.eightkdata.mongowp.bson.BsonArray;
import java.util.List;

/**
 *
 */
public class InMongoQuery {

    private final List<String> path;
    private final BsonArray inArray;

    public InMongoQuery(List<String> path, BsonArray inArray) {
        this.path = path;
        this.inArray = inArray;
    }

    public List<String> getPath() {
        return path;
    }

    public BsonArray getInArray() {
        return inArray;
    }

}
